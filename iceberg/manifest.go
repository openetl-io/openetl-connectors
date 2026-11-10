package iceberg

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// manifestRef names a manifest in its read's directory: an object listing
// files of the read's earlier epochs, so that a lane's descriptors stay small
// however long the read runs.
type manifestRef struct {
	ID    string `json:"id"`
	Files int    `json:"files"`
}

// readsDir is where a stream's reads keep their manifests: reserved, beside
// the table's data and metadata.
func (w *writer) readsDir(tbl *table.Table, stream string) string {
	h := sha256.Sum256([]byte(w.cfg.Pipeline + "\x00" + stream))
	return strings.TrimSuffix(tbl.Location(), "/") + "/_openetl/reads/" + hex.EncodeToString(h[:12])
}

func (w *writer) readDir(tbl *table.Table, stream string, start int64) string {
	return w.readsDir(tbl, stream) + "/" + strconv.FormatInt(start, 10)
}

// saveEarlier moves the read's listed files into a manifest, and merges its
// smallest manifests, so that it has about log2 of its files' manifests.
func (w *writer) saveEarlier(ctx context.Context, stream string, r *read) error {
	if len(r.files) == 0 {
		return nil
	}
	w.mu.Lock()
	tbl := w.tables[stream]
	w.mu.Unlock()
	if tbl == nil {
		var err error
		if tbl, err = w.cat.LoadTable(ctx, w.ident(w.ns, r.table)); err != nil {
			return fmt.Errorf("iceberg: load %s: %w", r.table, err)
		}
	}
	dir := w.readDir(tbl, stream, r.start)
	ref, err := writeManifest(ctx, tbl, dir, r.files)
	if err != nil {
		return fmt.Errorf("iceberg: a manifest of %s's read: %w", stream, err)
	}
	r.manifests, r.files = w.compact(ctx, tbl, dir, append(r.manifests, ref)), nil
	return nil
}

// compact merges the two smallest manifests while the smaller is over half
// the other. Best effort: a read that can't merge them keeps them, and a lane
// can go on describing a read that has ended, whose manifests are gone. The
// ones it replaces may still be in the record before this one's, so they go
// when the read does.
func (w *writer) compact(ctx context.Context, tbl *table.Table, dir string, refs []manifestRef) []manifestRef {
	for {
		slices.SortStableFunc(refs, func(a, b manifestRef) int { return cmp.Compare(b.Files, a.Files) })
		n := len(refs)
		if n < 2 || refs[n-1].Files*2 <= refs[n-2].Files {
			return refs
		}
		files, err := manifestFiles(ctx, tbl, dir, refs[n-2:])
		if err != nil {
			return refs
		}
		ref, err := writeManifest(ctx, tbl, dir, files)
		if err != nil {
			return refs
		}
		refs = append(refs[:n-2], ref)
	}
}

// writeManifest writes files to an object named by what it holds.
func writeManifest(ctx context.Context, tbl *table.Table, dir string, files []string) (manifestRef, error) {
	b, err := json.Marshal(files)
	if err != nil {
		return manifestRef{}, err
	}
	store, err := tbl.FS(ctx)
	if err != nil {
		return manifestRef{}, err
	}
	wfs, ok := store.(iceio.WriteFileIO)
	if !ok {
		return manifestRef{}, fmt.Errorf("the table's store writes no files: %T", store)
	}
	h := sha256.Sum256(b)
	ref := manifestRef{ID: hex.EncodeToString(h[:12]), Files: len(files)}
	return ref, wfs.WriteFile(dir+"/"+ref.ID+".json", b)
}

// manifestFiles reads the files refs list.
func manifestFiles(ctx context.Context, tbl *table.Table, dir string, refs []manifestRef) ([]string, error) {
	var out []string
	if len(refs) == 0 {
		return out, nil
	}
	store, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		f, err := store.Open(dir + "/" + ref.ID + ".json")
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		var files []string
		if err := json.Unmarshal(b, &files); err != nil {
			return nil, fmt.Errorf("manifest %s: %w", ref.ID, err)
		}
		out = append(out, files...)
	}
	return out, nil
}

// dropManifests deletes what's under dir: a read's manifests, those it merged
// too, or those of a stream's reads once one has ended, a lane that never knew
// of an earlier one's end having written more. Best effort: whatever stays
// behind is an orphan, as dropFiles's is.
func (w *writer) dropManifests(ns []string, name, dir string) {
	if dir == "" || name == "" {
		return
	}
	tbl, err := w.cat.LoadTable(w.ctx, w.ident(ns, name))
	if err != nil {
		return
	}
	w.removeManifests(tbl, dir)
}

// removeManifests is dropManifests on a table already loaded.
func (w *writer) removeManifests(tbl *table.Table, dir string) {
	if dir == "" {
		return
	}
	store, err := tbl.FS(w.ctx)
	if err != nil {
		return
	}
	listed, ok := store.(iceio.ListableIO)
	if !ok {
		return
	}
	_ = listed.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			_ = store.Remove(path)
		}
		return nil
	})
}

// dropRead deletes the files of a read that was abandoned, and its manifests.
func (w *writer) dropRead(ctx context.Context, stream string, r *read) {
	files := r.files
	if tbl, err := w.cat.LoadTable(ctx, w.ident(w.ns, r.table)); err == nil {
		dir := w.readDir(tbl, stream, r.start)
		// A manifest that can't be read leaves its files as orphans.
		if earlier, err := manifestFiles(ctx, tbl, dir, r.manifests); err == nil {
			files = append(earlier, files...)
		}
		w.dropManifests(w.ns, r.table, dir)
	}
	w.dropFiles(w.ns, r.table, files)
}
