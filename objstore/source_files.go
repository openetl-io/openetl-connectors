package objstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"gocloud.dev/blob"
)

// globRegexp is the regular expression of a glob over keys: * and ? match
// within a path segment, ** any number of them, {a,b} either, and [...] a
// character class. prefix is its part before the first of them.
func globRegexp(glob string) (re *regexp.Regexp, prefix string, err error) {
	var b strings.Builder
	b.WriteString("^")
	literal := true
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			literal = false
			if i+1 < len(glob) && glob[i+1] == '*' {
				i++
				if i+1 < len(glob) && glob[i+1] == '/' {
					i++
					b.WriteString("(?:.*/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			literal = false
			b.WriteString("[^/]")
		case '{':
			literal = false
			end := strings.IndexByte(glob[i:], '}')
			if end < 0 {
				return nil, "", fmt.Errorf("glob %q: a { doesn't close", glob)
			}
			var alts []string
			for _, a := range strings.Split(glob[i+1:i+end], ",") {
				alts = append(alts, regexp.QuoteMeta(a))
			}
			b.WriteString("(?:" + strings.Join(alts, "|") + ")")
			i += end
		case '[':
			literal = false
			end := strings.IndexByte(glob[i:], ']')
			if end < 0 {
				return nil, "", fmt.Errorf("glob %q: a [ doesn't close", glob)
			}
			class := glob[i+1 : i+end]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			b.WriteString("[" + class + "]")
			i += end
		default:
			// A byte at a time, so a character of several bytes stays whole.
			if literal {
				prefix += glob[i : i+1]
			}
			b.WriteString(regexp.QuoteMeta(glob[i : i+1]))
		}
	}
	b.WriteString("$")
	re, err = regexp.Compile(b.String())
	if err != nil {
		return nil, "", fmt.Errorf("glob %q: %w", glob, err)
	}
	return re, prefix, nil
}

// object is a file a stream reads.
type object struct {
	Key      string
	Modified time.Time
	Size     int64
}

// list lists the stream's files in split, in the order a read reads them:
// by key for a full refresh, and by modification, then key, incrementally.
func (s *streamSpec) list(ctx context.Context, b *blob.Bucket, r hashRange, incremental bool) ([]object, error) {
	var out []object
	it := b.List(&blob.ListOptions{Prefix: s.prefix})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("objstore: list %s: %w", s.prefix, err)
		}
		if obj.IsDir || !s.glob.MatchString(obj.Key) || !r.has(obj.Key) {
			continue
		}
		out = append(out, object{Key: obj.Key, Modified: obj.ModTime, Size: obj.Size})
	}
	slices.SortFunc(out, func(a, b object) int {
		if incremental {
			if c := a.Modified.Compare(b.Modified); c != 0 {
				return c
			}
		}
		return cmp.Compare(a.Key, b.Key)
	})
	return out, nil
}

// readState is a stream's, or a split's, state.
type readState struct {
	// Incremental: files last modified before it are read, in Unix
	// nanoseconds.
	Floor int64 `json:"floor,omitempty"`
	// Incremental: the files read that were last modified at or after
	// Floor, with when, in Unix nanoseconds. A file at the floor's instant
	// that wasn't there before is read, which a cursor alone would miss.
	Read map[string]int64 `json:"read,omitempty"`
	// Full refresh: the last file read, in key order.
	Last string `json:"last,omitempty"`
	// The file being read, and how far.
	At *filePos `json:"at,omitempty"`
	// The split's files, where Resplit gave some of its ID's to another.
	Range *hashRange `json:"range,omitempty"`
	// Resplit's halvings since the read began, in order.
	Gave []cut `json:"gave,omitempty"`
}

// cut is where a Resplit halved a split: the files of Give after the file
// of Key and Modified, in the read's order, went to another split.
type cut struct {
	Keep     hashRange `json:"keep"`
	Give     hashRange `json:"give"`
	Key      string    `json:"key"`
	Modified int64     `json:"modified"`
}

// cmpPos compares a cut's file with o in the read's order: by key, or by
// modification time then key for an incremental read.
func cmpPos(c cut, o object, incremental bool) int {
	if incremental {
		if n := cmp.Compare(c.Modified, o.Modified.UnixNano()); n != 0 {
			return n
		}
	}
	return cmp.Compare(c.Key, o.Key)
}

// filePos is a position in a file, after a batch of its records.
type filePos struct {
	Key      string `json:"key"`
	Modified int64  `json:"modified"`
	Size     int64  `json:"size"`
	// Parquet: the next row group.
	RowGroup int `json:"rowGroup,omitempty"`
	// Text formats: the bytes read, decompressed.
	Offset int64 `json:"offset,omitempty"`
	// The records read.
	Records int64 `json:"records,omitempty"`
}

// The most files an incremental state keeps; past it, the floor rises.
const maxRead = 10000

// done reports whether the state has read the file whole.
func (st *readState) done(o object, incremental bool) bool {
	if incremental {
		m, ok := st.Read[o.Key]
		return o.Modified.UnixNano() < st.Floor || ok && m == o.Modified.UnixNano()
	}
	return st.Last != "" && o.Key <= st.Last
}

// finish records the file as read whole.
func (st *readState) finish(o object, incremental bool) {
	st.At = nil
	if !incremental {
		st.Last = o.Key
		return
	}
	if st.Read == nil {
		st.Read = map[string]int64{}
	}
	st.Read[o.Key] = o.Modified.UnixNano()
	// Pruned in steps, which a read of many files would otherwise sort at
	// each.
	if len(st.Read) > 2*maxRead {
		st.keep(maxRead)
	}
}

// keep keeps the n files last modified latest, raising the floor to them.
func (st *readState) keep(n int) {
	if len(st.Read) <= n {
		return
	}
	times := slices.Sorted(maps.Values(st.Read))
	st.Floor = max(st.Floor, times[len(times)-n])
	for k, m := range st.Read {
		if m < st.Floor {
			delete(st.Read, k)
		}
	}
}

// next is where the stream's next incremental read starts: lookback
// before the newest file read, unless its floor is later, keeping the
// files read since.
func (st readState) next(lookback time.Duration) readState {
	out := readState{Floor: st.Floor, Read: map[string]int64{}, Range: st.Range}
	var newest int64
	for _, m := range st.Read {
		newest = max(newest, m)
	}
	if newest > 0 {
		out.Floor = max(out.Floor, newest-lookback.Nanoseconds())
	}
	for k, m := range st.Read {
		if m >= out.Floor {
			out.Read[k] = m
		}
	}
	out.keep(maxRead)
	return out
}
