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

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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
	// The store's version of it, such as its ETag or generation; empty
	// where the store gives none.
	Version string
	// version is a reader's version of it, from the reader's As; nil where
	// the store gives none.
	version func(as func(any) bool) string
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
		o := object{Key: obj.Key, Modified: obj.ModTime, Size: obj.Size, version: s.version}
		if s.version != nil {
			o.Version = s.version(obj.As)
		}
		out = append(out, o)
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
	// Incremental: the versions of the files of Read whose store gives them.
	// A state of before versions has none, and its files match by time.
	Versions map[string]string `json:"versions,omitempty"`
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

// untilAt is where a read up to an until stops: by position, as a read
// again lists the files again, and a file written since leaves a state that
// never equals it.
type untilAt struct {
	// The until's file: the one it's in, at at, or with at nil the last it
	// read whole, in the read's order. Incremental: files last modified
	// before floor are before it too, and with none every other is after.
	key      string
	modified int64
	at       *filePos
	floor    int64
	none     bool
	// The files the read sends records of up to the until, as the until's
	// state saw them, which have to be as they were.
	need []needed
}

// needed is a file a read up to an until sends records of, as at has it,
// of any size where at's is below 0. Nil at: any file of the key, all a
// full refresh's state keeps of a file it read whole.
type needed struct {
	key string
	at  *filePos
}

func (n needed) matches(o object) bool {
	a := n.at
	return a == nil || o.Modified.UnixNano() == a.Modified && (a.Size < 0 || o.Size == a.Size) && sameTag(a.Version, o.Version)
}

// untilOf is where a read from from up to until stops. Incremental, the
// last file until read whole is the latest of those it has and from hasn't.
func untilOf(from, until readState, incremental bool) *untilAt {
	u := &untilAt{floor: until.Floor, none: true, need: neededOf(from, until, incremental)}
	switch {
	case until.At != nil:
		u.key, u.modified, u.at, u.none = until.At.Key, until.At.Modified, until.At, false
	case !incremental:
		u.key, u.none = until.Last, false
	default:
		for _, n := range readSince(from, until) {
			if u.none || cmp.Or(cmp.Compare(n.at.Modified, u.modified), cmp.Compare(n.key, u.key)) > 0 {
				u.key, u.modified, u.none = n.key, n.at.Modified, false
			}
		}
	}
	return u
}

// readSince are the files an incremental until read whole that from
// hadn't.
func readSince(from, until readState) []needed {
	var out []needed
	for k, m := range until.Read {
		v := until.Versions[k]
		if !from.done(object{Key: k, Modified: time.Unix(0, m), Version: v}, true) {
			out = append(out, needed{key: k, at: &filePos{Key: k, Modified: m, Size: -1, Version: v}})
		}
	}
	return out
}

// neededOf are the files a read from from up to until sends records of:
// those it reads whole on the way, and the until's own up to its place.
func neededOf(from, until readState, incremental bool) []needed {
	var out []needed
	switch {
	case incremental:
		out = readSince(from, until)
	case until.Last != "" && until.Last != from.Last:
		out = append(out, needed{key: until.Last})
	}
	a := until.At
	if f := from.At; !incremental && f != nil && (a == nil || a.Key != f.Key) {
		out = append(out, needed{key: f.Key, at: f})
	}
	if a != nil && (from.At == nil || *from.At != *a) {
		out = append(out, needed{key: a.Key, at: a})
	}
	return out
}

// gone fails a read with POSITION_LOST if a file it sends records of up to
// its until isn't among files, or isn't as it was: the read can't send
// those records again.
func (u *untilAt) gone(files []object, stream string) error {
	if u == nil {
		return nil
	}
	listed := make(map[string]object, len(files))
	for _, o := range files {
		listed[o.Key] = o
	}
	for _, n := range u.need {
		o, ok := listed[n.key]
		what := "was deleted"
		switch {
		case ok && n.matches(o):
			continue
		case ok:
			what = "was written again"
		}
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, fmt.Errorf("objstore: stream %q: %s, which the read up to where it stops sends records of, %s since, so the read can't send them again", stream, n.key, what))
	}
	return nil
}

// cmpFile compares o with the until's file in the read's order.
func (u *untilAt) cmpFile(o object, incremental bool) int {
	if !incremental {
		return cmp.Compare(o.Key, u.key)
	}
	m := o.Modified.UnixNano()
	switch {
	case m < u.floor:
		return -1
	case u.none:
		return 1
	}
	return cmp.Or(cmp.Compare(m, u.modified), cmp.Compare(o.Key, u.key))
}

// past says o is after the until, so the read stops before it. Nil: the
// read has no until.
func (u *untilAt) past(o object, incremental bool) bool {
	return u != nil && u.cmpFile(o, incremental) > 0
}

// after says a read that has read o whole has reached the until.
func (u *untilAt) after(o object, incremental bool) bool {
	return u != nil && u.cmpFile(o, incremental) >= 0
}

// in says a read at p in o has reached the until: o is the file the until
// is in, as it was then. Of the file written again since, the read reads
// all.
func (u *untilAt) in(o object, p filePos, incremental bool) bool {
	if u == nil || u.at == nil || u.cmpFile(o, incremental) != 0 {
		return false
	}
	a := u.at
	if o.Modified.UnixNano() != a.Modified || o.Size != a.Size || !sameTag(a.Version, o.Version) {
		return false
	}
	return cmp.Or(cmp.Compare(p.Records, a.Records), cmp.Compare(p.RowGroup, a.RowGroup), cmp.Compare(p.Offset, a.Offset)) >= 0
}

// filePos is a position in a file, after a batch of its records.
type filePos struct {
	Key      string `json:"key"`
	Modified int64  `json:"modified"`
	Size     int64  `json:"size"`
	Version  string `json:"version,omitempty"`
	// Parquet: the next row group.
	RowGroup int `json:"rowGroup,omitempty"`
	// Text formats: the bytes read, decompressed.
	Offset int64 `json:"offset,omitempty"`
	// The records read.
	Records int64 `json:"records,omitempty"`
}

// sameTag: versions a and b are one, or either is unknown.
func sameTag(a, b string) bool { return a == "" || b == "" || a == b }

// The most files an incremental state keeps; past it, the floor rises.
const maxRead = 10000

// done reports whether the state has read the file whole.
func (st *readState) done(o object, incremental bool) bool {
	if incremental {
		m, ok := st.Read[o.Key]
		return o.Modified.UnixNano() < st.Floor || ok && m == o.Modified.UnixNano() && sameTag(st.Versions[o.Key], o.Version)
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
	delete(st.Versions, o.Key)
	if o.Version != "" {
		if st.Versions == nil {
			st.Versions = map[string]string{}
		}
		st.Versions[o.Key] = o.Version
	}
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
			delete(st.Versions, k)
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
			if v, ok := st.Versions[k]; ok {
				if out.Versions == nil {
					out.Versions = map[string]string{}
				}
				out.Versions[k] = v
			}
		}
	}
	out.keep(maxRead)
	return out
}
