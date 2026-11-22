package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl-connectors/docstore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	Connection
	// Starts each table's name where the pipeline names no namespace; with
	// one, a table is NAMESPACE.TABLE.
	TablePrefix string `json:"tablePrefix"`
}

// Destination writes each stream to a table, staging an epoch's rows as
// items of a control table and applying them in Commit with writes that
// apply alike however often they run: merge puts and deletes keyed on the
// primary key and guarded by _openetl_seq, append puts keyed on
// _openetl_id. A stream's apply ends in its marker, which a single-item
// update of the pipeline's fence item writes, refused once a newer
// generation has raised the fence. A crash before the marker leaves the
// staging, which Recover applies again.
type Destination struct {
	cfg DestinationConfig
}

var (
	_ destination.Teardowner    = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	var cfg DestinationConfig
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, configInvalid(err)
	}
	if strings.Trim(cfg.TablePrefix, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") != "" {
		return nil, configInvalid(fmt.Errorf("dynamodb: tablePrefix %q has a character DynamoDB doesn't take in a table's name", cfg.TablePrefix))
	}
	return &Destination{cfg: cfg}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

// Teardown removes a deleted pipeline's fence item, and what it staged,
// from each control table the credentials can list, a pipeline's being
// <prefix>_openetl_state for its namespace or the config's tablePrefix.
func (d *Destination) Teardown(ctx context.Context, pipeline string) error {
	cl, err := d.cfg.connect(ctx, 10)
	if err != nil {
		return err
	}
	fence, stage, read := stringOf(fenceKey(pipeline)["p"]), stagePrefix(pipeline), readPrefix(pipeline)
	pages := dynamodb.NewListTablesPaginator(cl.db, &dynamodb.ListTablesInput{})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return classify(fmt.Errorf("dynamodb: list tables: %w", err))
		}
		for _, name := range page.TableNames {
			if !strings.HasSuffix(name, controlTable) {
				continue
			}
			w := &writer{db: cl.db, control: name}
			err := w.dropParts(ctx, "", func(p string) bool {
				return p == fence || strings.HasPrefix(p, stage) || strings.HasPrefix(p, read)
			})
			if err != nil && apiCode(err) != "ResourceNotFoundException" {
				return err
			}
		}
	}
	return nil
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	cl, err := d.cfg.connect(ctx, 10)
	if err != nil {
		return nil, err
	}
	w := &writer{
		db: cl.db, cfg: cfg, prefix: d.cfg.TablePrefix,
		streams: map[string]destination.Stream{}, targets: map[string]string{},
		epochs: map[destination.Epoch]map[string]*laneStream{},
	}
	if cfg.Namespace != "" {
		w.prefix = cfg.Namespace + "."
	}
	w.control = w.prefix + controlTable
	if err := validName(w.control); err != nil {
		return nil, configInvalid(err)
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	if err := w.ensureTable(ctx, w.control, []keyAttr{{"p", types.ScalarAttributeTypeS}, {"n", types.ScalarAttributeTypeS}}); err != nil {
		return nil, err
	}
	return w, nil
}

const (
	idColumn        = "_openetl_id"
	opColumn        = "_openetl_op"
	seqColumn       = "_openetl_seq"
	syncedColumn    = "_openetl_synced_at"
	deletedColumn   = "_openetl_deleted"
	unchangedColumn = "_openetl_unchanged"
	// A row's held columns, as a list; in the target, a map from each to
	// the _openetl_id of the version whose value is held.
	heldColumn = destination.HeldColumn
	// A merged table's key where the stream's has more than two columns:
	// their values as DynamoDB JSON.
	keyColumn = "_openetl_key"
	// The overwrite read that wrote an item.
	readColumn = "_openetl_read"
	// An unkeyed table's sort key: the staged row's order, so that two rows
	// with one _openetl_id, as two rows alike without a key have, both land;
	// "-" where the stream is deduplicated against the target, so that a
	// row's _openetl_id finds it.
	rowColumn = "_openetl_row"
	// The table of a namespace's, or a prefix's, staging and fences.
	controlTable = "_openetl_state"
	// Staged rows are spread over this many partitions per lane, by their
	// key, so that a key's versions are in one and apply in order.
	buckets = 16
)

type writer struct {
	db      dbAPI
	cfg     destination.OpenConfig
	prefix  string
	control string

	streams map[string]destination.Stream
	starts  map[string]time.Time
	// What the lane has staged of each epoch, by stream.
	epochs map[destination.Epoch]map[string]*laneStream
	row    int64

	mu sync.Mutex
	// Each stream's table, as its schemas name it.
	targets map[string]string
}

// laneStream is what a lane staged of a stream in an epoch.
type laneStream struct {
	target, read string
	rows         int64
	buckets      map[int]bool
	ended        bool
}

func hash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:6])
}

// The control table's partitions: an epoch's staged rows of a stream, by
// lane and bucket; an overwrite read's rows, by bucket; a pipeline's fence.
func stagePrefix(pipeline string) string { return "stage#" + hash(pipeline) + "#" }

func stagePart(pipeline, stream string, e destination.Epoch, lane, bucket int) string {
	return fmt.Sprintf("%s%d#%d#%d#%s#%d", stagePrefix(pipeline), e.Generation, e.N, lane, hash(stream), bucket)
}

func readPrefix(pipeline string) string { return "read#" + hash(pipeline) + "#" }

// readName names an overwrite read's partitions: readName#BUCKET. Its last
// part, the read's start, tags the items it writes.
func readName(pipeline, stream string, start time.Time) string {
	return readPrefix(pipeline) + hash(stream) + "#" + strconv.FormatInt(start.UnixMicro(), 10)
}

func fenceKey(pipeline string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"p": str("fence#" + pipeline), "n": str("-")}
}

// order is a staged row's sort key, which orders an epoch's rows, and an
// overwrite read's over its epochs.
func order(e destination.Epoch, lane int, row int64) string {
	return fmt.Sprintf("%020d.%020d.%05d.%020d", e.Generation, e.N, lane, row)
}

func (w *writer) setTarget(stream, target string) {
	w.mu.Lock()
	w.targets[stream] = target
	w.mu.Unlock()
}

func (w *writer) target(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.targets[stream]
}

// StartReads drops the staged rows of reads that the attempt starts over.
func (w *writer) StartReads(ctx context.Context, starts map[string]time.Time) error {
	w.starts = starts
	current := map[string]string{}
	for stream, start := range starts {
		current[hash(stream)] = readName(w.cfg.Pipeline, stream, start) + "#"
	}
	markers, err := w.Markers(ctx)
	if err != nil {
		return err
	}
	return w.dropParts(ctx, readPrefix(w.cfg.Pipeline), func(p string) bool {
		for _, m := range markers {
			if m.Swap != "" && strings.HasPrefix(p, m.Swap+"#") {
				return false
			}
		}
		sh, _, _ := strings.Cut(strings.TrimPrefix(p, readPrefix(w.cfg.Pipeline)), "#")
		cur, ok := current[sh]
		return ok && !strings.HasPrefix(p, cur)
	})
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.lane(e, stream).ended = true
	return nil
}

func (w *writer) lane(e destination.Epoch, stream string) *laneStream {
	if w.epochs[e] == nil {
		w.epochs[e] = map[string]*laneStream{}
	}
	ls := w.epochs[e][stream]
	if ls == nil {
		ls = &laneStream{buckets: map[int]bool{}}
		if start, ok := w.starts[stream]; ok && w.streams[stream].Mode == destination.Overwrite {
			ls.read = readName(w.cfg.Pipeline, stream, start)
		}
		w.epochs[e][stream] = ls
	}
	return ls
}

// keyed reports whether a stream's items are keyed on its primary key:
// merged, or overwritten with a key. Others are keyed on _openetl_id.
func keyed(s destination.Stream) bool {
	return len(s.PrimaryKey) > 0 && (s.Mode == destination.Merge || s.Mode == destination.Overwrite)
}

type keyAttr struct {
	name string
	typ  types.ScalarAttributeType
}

// keySchema is the key a stream's table has: the primary key's columns,
// hash and range, typed as their values are; _openetl_key for a key of
// more columns; else _openetl_id and _openetl_row.
func keySchema(s destination.Stream, schema *arrow.Schema) []keyAttr {
	switch {
	case !keyed(s):
		return []keyAttr{{idColumn, types.ScalarAttributeTypeS}, {rowColumn, types.ScalarAttributeTypeS}}
	case len(s.PrimaryKey) > 2:
		return []keyAttr{{keyColumn, types.ScalarAttributeTypeS}}
	}
	var out []keyAttr
	for _, c := range s.PrimaryKey {
		t := types.ScalarAttributeTypeS
		if i := index(schema, c); i >= 0 {
			t = scalarType(schema.Field(i))
		}
		out = append(out, keyAttr{c, t})
	}
	return out
}

// stagedRow is a staged item: a row, or a patch.
type stagedRow struct {
	bucket int
	item   map[string]types.AttributeValue
	// The columns whose target values a merged row keeps: those held with
	// "last", and those an update didn't send.
	keep  []string
	patch bool
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	return w.stage(ctx, e, stream, batch, false)
}

// Patch stages patches with the epoch's rows; Commit applies each bucket's
// after its rows.
func (w *writer) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	return w.stage(ctx, e, stream, batch, true)
}

func dataInvalid(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, err)
}

func (w *writer) stage(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch, patch bool) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("dynamodb: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Mode == destination.Overwrite {
		if _, ok := w.starts[stream]; !ok {
			return fmt.Errorf("dynamodb: stream %q overwrites, but its read has no start", stream)
		}
	}
	ls := w.lane(e, stream)
	ls.target = w.prefix + tableName(batch.Schema(), stream)
	w.setTarget(stream, ls.target)
	rows, err := w.rows(cfg, batch, patch)
	if err != nil {
		return fmt.Errorf("dynamodb: stream %q: %w", stream, err)
	}
	reqs := make([]types.WriteRequest, 0, len(rows))
	for _, r := range rows {
		w.row++
		if !keyed(cfg) && !r.patch {
			r.item[rowColumn] = str("-")
			if cfg.Dedupe != destination.DedupeTarget {
				r.item[rowColumn] = str(order(e, w.cfg.Lane, w.row))
			}
		}
		it := map[string]types.AttributeValue{
			"p": str(stagePart(w.cfg.Pipeline, stream, e, w.cfg.Lane, r.bucket)),
			"n": str(order(e, w.cfg.Lane, w.row)),
			"d": &types.AttributeValueMemberM{Value: r.item},
		}
		if len(r.keep) > 0 {
			it["k"] = &types.AttributeValueMemberSS{Value: r.keep}
		}
		if r.patch {
			it["x"] = &types.AttributeValueMemberBOOL{Value: true}
		}
		if n := size(it); n > maxItem {
			return dataInvalid(fmt.Errorf("dynamodb: a row of stream %q is about %d bytes as DynamoDB counts them, with what staging adds, over the 400 KB an item can be", stream, n))
		}
		ls.buckets[r.bucket] = true
		reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: it}})
	}
	if err := writeBatches(ctx, w.db, w.control, reqs); err != nil {
		return err
	}
	ls.rows += int64(len(rows))
	return nil
}

func index(schema *arrow.Schema, name string) int {
	if i := schema.FieldIndices(name); len(i) > 0 {
		return i[0]
	}
	return -1
}

// columns are a batch's converters by column, nil for the metadata columns
// that don't go into items as they are. _openetl_seq is a number, which
// orders a key's versions exactly: timestamps in their unit since 1970.
func columns(schema *arrow.Schema) []convert {
	out := make([]convert, schema.NumFields())
	for i, f := range schema.Fields() {
		switch f.Name {
		case heldColumn, unchangedColumn:
		case seqColumn:
			if _, ok := f.Type.(*arrow.TimestampType); ok {
				out[i] = func(a arrow.Array, i int) (types.AttributeValue, error) {
					return num(strconv.FormatInt(int64(storage(a).(*array.Timestamp).Value(i)), 10)), nil
				}
				continue
			}
			out[i] = converter(f)
		default:
			out[i] = converter(f)
		}
	}
	return out
}

// rows are a batch's rows, or patches, as staged items. A null is no
// attribute; a held column's is left out, unless its target value is kept.
// A patch names the version whose held cells it fills, v, by _openetl_id,
// and for a keyed stream the key, k, and the value of each column its
// _openetl_held lists, f, from the column of that name or the one whose
// metadata says it fills it.
func (w *writer) rows(cfg destination.Stream, batch arrow.RecordBatch, patch bool) ([]stagedRow, error) {
	schema := batch.Schema()
	convs := columns(schema)
	keys := keySchema(cfg, schema)
	idCol, heldCol, unchangedCol := index(schema, idColumn), index(schema, heldColumn), index(schema, unchangedColumn)
	if idCol < 0 && (!keyed(cfg) || patch) {
		return nil, fmt.Errorf("its batches have no %s", idColumn)
	}
	if patch && heldCol < 0 {
		return nil, fmt.Errorf("patches need %s", heldColumn)
	}
	last := map[string]bool{}
	fills := map[string]int{}
	for c, f := range schema.Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" {
			last[f.Name] = true
		}
		name := f.Name
		if v, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			name = v
		}
		if _, ok := fills[name]; !ok || name != f.Name {
			fills[name] = c
		}
	}
	out := make([]stagedRow, 0, batch.NumRows())
	for i := range int(batch.NumRows()) {
		item := map[string]types.AttributeValue{}
		for c, conv := range convs {
			if conv == nil || batch.Column(c).IsNull(i) {
				continue
			}
			v, err := conv(batch.Column(c), i)
			if err != nil {
				return nil, dataInvalid(fmt.Errorf("column %q: %w", schema.Field(c).Name, err))
			}
			item[schema.Field(c).Name] = v
		}
		key, err := rowKey(cfg, keys, item)
		if err != nil {
			return nil, err
		}
		var version types.AttributeValue
		if idCol >= 0 {
			version = item[idColumn]
		}
		if patch {
			f := map[string]types.AttributeValue{}
			for _, col := range docstore.ListStrings(batch.Column(heldCol), i) {
				c, ok := fills[col]
				if !ok {
					return nil, fmt.Errorf("a patch fills %q, which it has no column for", col)
				}
				v := types.AttributeValue(&types.AttributeValueMemberNULL{Value: true})
				if !batch.Column(c).IsNull(i) {
					if v, err = convs[c](batch.Column(c), i); err != nil {
						return nil, dataInvalid(err)
					}
				}
				f[col] = v
			}
			p := map[string]types.AttributeValue{"v": version, "f": &types.AttributeValueMemberM{Value: f}}
			if !keyed(cfg) {
				key = map[string]types.AttributeValue{idColumn: version}
				p["q"] = &types.AttributeValueMemberBOOL{Value: true}
			}
			p["k"] = &types.AttributeValueMemberM{Value: key}
			out = append(out, stagedRow{bucket: bucketOf(key), item: p, patch: true})
			continue
		}
		maps.Copy(item, key)
		var keep []string
		if heldCol >= 0 {
			if held := docstore.ListStrings(batch.Column(heldCol), i); len(held) > 0 {
				m := map[string]types.AttributeValue{}
				for _, c := range held {
					m[c] = version
					if last[c] && cfg.Mode == destination.Merge {
						keep = append(keep, c)
					}
				}
				item[heldColumn] = &types.AttributeValueMemberM{Value: m}
			}
		}
		if unchangedCol >= 0 && cfg.Mode == destination.Merge {
			keep = append(keep, docstore.ListStrings(batch.Column(unchangedCol), i)...)
		}
		slices.Sort(keep)
		out = append(out, stagedRow{bucket: bucketOf(key), item: item, keep: slices.Compact(keep)})
	}
	return out, nil
}

// rowKey is a row's key in its stream's table: for a key of more than two
// columns, their values as a DynamoDB JSON list; unkeyed, its _openetl_id
// alone, which stage completes.
func rowKey(cfg destination.Stream, keys []keyAttr, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	switch {
	case !keyed(cfg):
		return keyOf(keys[:1], item)
	case keys[0].name != keyColumn:
		return keyOf(keys, item)
	}
	vals := make([]types.AttributeValue, len(cfg.PrimaryKey))
	for i, c := range cfg.PrimaryKey {
		v, ok := item[c]
		if !ok {
			return nil, dataInvalid(fmt.Errorf("key column %q is null", c))
		}
		vals[i] = v
	}
	return map[string]types.AttributeValue{keyColumn: str(string(appendJSON(nil, &types.AttributeValueMemberL{Value: vals}, true)))}, nil
}

// keyOf is an item's key in a table keyed on keys.
func keyOf(keys []keyAttr, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	key := map[string]types.AttributeValue{}
	for _, k := range keys {
		v, ok := item[k.name]
		if !ok {
			return nil, dataInvalid(fmt.Errorf("key column %q is null", k.name))
		}
		kv, err := asKey(v, k.typ)
		if err != nil {
			return nil, dataInvalid(fmt.Errorf("key column %q: %w", k.name, err))
		}
		if s, _ := text(kv); s == "" && k.typ != types.ScalarAttributeTypeN {
			return nil, dataInvalid(fmt.Errorf("key column %q is empty, which DynamoDB doesn't take in a key", k.name))
		}
		key[k.name] = kv
	}
	return key, nil
}

func bucketOf(key map[string]types.AttributeValue) int {
	h := fnv.New32a()
	_, _ = h.Write(appendMap(nil, key, true))
	return int(h.Sum32() % buckets)
}

// staged is a lane's committable: what it staged of an epoch.
type staged struct {
	Generation uint64         `json:"generation"`
	N          uint64         `json:"n"`
	Lane       int            `json:"lane"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name string `json:"name"`
	// Its table; empty where the lane had no rows of it.
	Target string                `json:"target,omitempty"`
	Mode   destination.WriteMode `json:"mode"`
	Key    []string              `json:"key,omitempty"`
	Dedupe destination.Dedupe    `json:"dedupe,omitempty"`
	Hard   bool                  `json:"hard,omitempty"`
	Rows   int64                 `json:"rows,omitempty"`
	// The buckets the lane staged rows in.
	Buckets []int  `json:"buckets,omitempty"`
	Read    string `json:"read,omitempty"`
	Ended   bool   `json:"ended,omitempty"`
	// Every lane's staged partitions, by bucket, in lane order, once
	// mergeLanes has merged them.
	parts map[int][]string
}

func (s stagedStream) config() destination.Stream {
	d := destination.HardDelete
	if !s.Hard {
		d = destination.SoftDelete
	}
	return destination.Stream{Name: s.Name, Mode: s.Mode, PrimaryKey: s.Key, Dedupe: s.Dedupe, Deletes: d}
}

func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	st := staged{Generation: e.Generation, N: e.N, Lane: w.cfg.Lane}
	lanes := w.epochs[e]
	delete(w.epochs, e)
	for _, name := range slices.Sorted(maps.Keys(lanes)) {
		ls, cfg := lanes[name], w.streams[name]
		st.Streams = append(st.Streams, stagedStream{
			Name: name, Target: ls.target, Mode: cfg.Mode, Key: cfg.PrimaryKey, Dedupe: cfg.Dedupe,
			Hard: cfg.Deletes == destination.HardDelete, Rows: ls.rows, Read: ls.read, Ended: ls.ended,
			Buckets: slices.Sorted(maps.Keys(ls.buckets)),
		})
	}
	return json.Marshal(st)
}

// mergeLanes parses every lane's committable, and merges what they say of
// each stream: its staged partitions, in lane order.
func (w *writer) mergeLanes(e destination.Epoch, committables []destination.Staged) ([]stagedStream, error) {
	byName := map[string]*stagedStream{}
	var order []string
	for _, b := range committables {
		if len(b) == 0 {
			continue
		}
		var st staged
		if err := json.Unmarshal(b, &st); err != nil {
			return nil, fmt.Errorf("dynamodb: a committable: %w", err)
		}
		if st.Generation != e.Generation || st.N != e.N {
			return nil, fmt.Errorf("dynamodb: a committable of epoch %d.%d, not %d.%d", st.Generation, st.N, e.Generation, e.N)
		}
		for _, s := range st.Streams {
			m := byName[s.Name]
			if m == nil {
				m = &stagedStream{Name: s.Name, Mode: s.Mode, Key: s.Key, Dedupe: s.Dedupe, Hard: s.Hard, parts: map[int][]string{}}
				byName[s.Name] = m
				order = append(order, s.Name)
			}
			m.Target = cmpOr(m.Target, s.Target)
			m.Read = cmpOr(m.Read, s.Read)
			m.Ended = m.Ended || s.Ended
			m.Rows += s.Rows
			for _, b := range s.Buckets {
				m.parts[b] = append(m.parts[b], stagePart(w.cfg.Pipeline, s.Name, e, st.Lane, b))
			}
		}
	}
	out := make([]stagedStream, 0, len(order))
	for _, name := range order {
		s := byName[name]
		if s.Target == "" {
			s.Target = cmpOr(w.target(name), w.prefix+tableName(arrow.NewSchema(nil, nil), name))
		}
		out = append(out, *s)
	}
	return out, nil
}

// validName says why DynamoDB doesn't take a table's name, which tableName
// and the config keep to the characters it takes, if it doesn't.
func validName(name string) error {
	if len(name) < 3 || len(name) > 255 {
		return fmt.Errorf("dynamodb: the table name %q is %d characters, and DynamoDB takes 3 to 255", name, len(name))
	}
	return nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

type marker = docstore.Marker

func markerAttribute(m marker) types.AttributeValue {
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		"g": num(strconv.FormatUint(m.Generation, 10)), "n": num(strconv.FormatUint(m.N, 10)), "swap": str(m.Swap), "t": str(m.Target),
	}}
}

// The fence item's attributes: gen, its generation, and applied, a map of
// each stream's marker by its name.

func uintOf(av types.AttributeValue) uint64 {
	if n, ok := av.(*types.AttributeValueMemberN); ok {
		v, _ := strconv.ParseUint(n.Value, 10, 64)
		return v
	}
	return 0
}

func stringOf(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// Markers is the pipeline's markers: ErrFenced if a newer generation
// than the writer's has raised its fence.
func (w *writer) Markers(ctx context.Context) (map[string]marker, error) {
	res, err := w.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(w.control), Key: fenceKey(w.cfg.Pipeline), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, classify(fmt.Errorf("dynamodb: read the fence: %w", err))
	}
	if uintOf(res.Item["gen"]) > w.cfg.Generation {
		return nil, destination.ErrFenced
	}
	out := map[string]marker{}
	applied, _ := res.Item["applied"].(*types.AttributeValueMemberM)
	if applied == nil {
		return out, nil
	}
	for name, v := range applied.Value {
		m, isMap := v.(*types.AttributeValueMemberM)
		if !isMap {
			continue
		}
		out[name] = marker{
			Stream: name, Generation: uintOf(m.Value["g"]), N: uintOf(m.Value["n"]),
			Swap: stringOf(m.Value["swap"]), Target: stringOf(m.Value["t"]),
		}
	}
	return out, nil
}

// SetMarkers replaces the streams' markers, unless a newer generation has
// raised the fence: one item's update, so it's either all or none, of the
// markers as the fence had them, which only the writer of the fence's
// generation changes.
func (w *writer) SetMarkers(ctx context.Context, ms []marker) error {
	markers, err := w.Markers(ctx)
	if err != nil {
		return err
	}
	for _, m := range ms {
		markers[m.Stream] = m
	}
	applied := map[string]types.AttributeValue{}
	for name, m := range markers {
		applied[name] = markerAttribute(m)
	}
	_, err = w.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(w.control), Key: fenceKey(w.cfg.Pipeline),
		UpdateExpression:          aws.String("SET #a = :a"),
		ConditionExpression:       aws.String("#g <= :g"),
		ExpressionAttributeNames:  map[string]string{"#a": "applied", "#g": "gen"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":a": &types.AttributeValueMemberM{Value: applied}, ":g": num(strconv.FormatUint(w.cfg.Generation, 10))},
	})
	if conditionFailed(err) {
		return destination.ErrFenced
	}
	if err != nil {
		return classify(fmt.Errorf("dynamodb: mark %s applied: %w", w.cfg.Pipeline, err))
	}
	return nil
}

func conditionFailed(err error) bool {
	_, ok := errors.AsType[*types.ConditionalCheckFailedException](err)
	return ok
}

// Commit applies each stream whose marker isn't the epoch's yet: with
// ScopePipeline, every stream's rows and then their markers together, with
// ScopeStream, stream by stream.
func (w *writer) Commit(ctx context.Context, e destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	streams, err := w.mergeLanes(e, committables)
	if err != nil {
		return destination.Committed{}, err
	}
	return docstore.Settle(ctx, w, w.cfg.Scope, e, streams)
}

// Apply writes a stream's staged rows of every lane; an overwrite's go to
// its read's partitions, until the read ends and swap writes them.
// collapsed: the rows dedupe skipped.
func (w *writer) Apply(ctx context.Context, s stagedStream) (collapsed uint64, err error) {
	if s.Mode == destination.Overwrite {
		return w.copyToRead(ctx, s)
	}
	return w.applyParts(ctx, s.config(), s.Target, s.parts, "")
}

// copyToRead copies an epoch's staged rows of an overwrite stream into its
// read's partitions, keeping their order. Deduped within each epoch, it
// leaves out a row whose _openetl_id an earlier row of the epoch has, as
// the swap, which writes every epoch's rows, can't: collapsed counts them.
func (w *writer) copyToRead(ctx context.Context, s stagedStream) (collapsed uint64, err error) {
	for b, parts := range s.parts {
		seen := map[string]bool{}
		for _, p := range parts {
			var reqs []types.WriteRequest
			err := w.query(ctx, p, func(it map[string]types.AttributeValue) error {
				if d, ok := it["d"].(*types.AttributeValueMemberM); ok && it["x"] == nil && s.Dedupe == destination.DedupeEpoch {
					id := stringOf(d.Value[idColumn])
					if seen[id] {
						collapsed++
						return nil
					}
					seen[id] = true
				}
				it["p"] = str(fmt.Sprintf("%s#%d", s.Read, b))
				reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: it}})
				return nil
			})
			if err != nil {
				return 0, err
			}
			if err := writeBatches(ctx, w.db, w.control, reqs); err != nil {
				return 0, err
			}
		}
	}
	return collapsed, nil
}

// Swap writes an ended overwrite read's rows to its target, each tagged
// with the read, deletes the target's items the read didn't write, and then
// clears the marker's Swap. It applies alike however often it runs, and
// isn't atomic: while it runs, readers see the old items and the new.
func (w *writer) Swap(ctx context.Context, m marker) error {
	if m.Swap == "" {
		return nil
	}
	tag := m.Swap[strings.LastIndex(m.Swap, "#")+1:]
	parts := map[int][]string{}
	for b := range buckets {
		parts[b] = []string{fmt.Sprintf("%s#%d", m.Swap, b)}
	}
	cfg := w.streams[m.Stream]
	cfg.Mode = destination.Overwrite
	if _, err := w.applyParts(ctx, cfg, m.Target, parts, tag); err != nil {
		return err
	}
	if err := w.sweep(ctx, m.Target, tag); err != nil {
		return err
	}
	for _, p := range parts {
		if err := w.deletePartitions(ctx, p); err != nil {
			return err
		}
	}
	m.Swap = ""
	return w.SetMarkers(ctx, []marker{m})
}

// sweep deletes the items of target that the read tagged tag didn't write.
func (w *writer) sweep(ctx context.Context, target, tag string) error {
	d, err := w.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(target)})
	if apiCode(err) == "ResourceNotFoundException" {
		return nil
	}
	if err != nil {
		return classify(fmt.Errorf("dynamodb: describe %s: %w", target, err))
	}
	names := map[string]string{"#r": readColumn}
	var proj []string
	for i, k := range d.Table.KeySchema {
		names[fmt.Sprintf("#k%d", i)] = aws.ToString(k.AttributeName)
		proj = append(proj, fmt.Sprintf("#k%d", i))
	}
	in := &dynamodb.ScanInput{
		TableName: aws.String(target), ConsistentRead: aws.Bool(true),
		FilterExpression:          aws.String("attribute_not_exists(#r) OR #r <> :t"),
		ProjectionExpression:      aws.String(strings.Join(proj, ", ")),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: map[string]types.AttributeValue{":t": str(tag)},
	}
	pages := dynamodb.NewScanPaginator(w.db, in)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return classify(fmt.Errorf("dynamodb: scan %s: %w", target, err))
		}
		reqs := make([]types.WriteRequest, len(page.Items))
		for i, it := range page.Items {
			reqs[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: it}}
		}
		if err := writeBatches(ctx, w.db, target, reqs); err != nil {
			return err
		}
	}
	return nil
}

// applyParts applies the staged rows of each bucket's partitions to target,
// the buckets in parallel, each's rows in order and then its patches. With
// a tag, as an overwrite read's, each item it puts is tagged.
func (w *writer) applyParts(ctx context.Context, cfg destination.Stream, target string, parts map[int][]string, tag string) (uint64, error) {
	var collapsed atomic.Uint64
	g, gctx := errgroup.WithContext(ctx)
	for _, ps := range parts {
		g.Go(func() error {
			var patches []map[string]types.AttributeValue
			seen := map[string]bool{}
			for _, p := range ps {
				err := w.query(gctx, p, func(it map[string]types.AttributeValue) error {
					d, _ := it["d"].(*types.AttributeValueMemberM)
					if d == nil {
						return nil
					}
					if _, ok := it["x"]; ok {
						patches = append(patches, d.Value)
						return nil
					}
					if cfg.Dedupe == destination.DedupeEpoch && tag == "" {
						id := stringOf(d.Value[idColumn])
						if seen[id] {
							collapsed.Add(1)
							return nil
						}
						seen[id] = true
					}
					var keep []string
					if k, ok := it["k"].(*types.AttributeValueMemberSS); ok {
						keep = k.Value
					}
					skipped, err := w.applyRow(gctx, cfg, target, d.Value, keep, tag)
					if skipped {
						collapsed.Add(1)
					}
					return err
				})
				if err != nil {
					return err
				}
			}
			for _, p := range patches {
				if err := w.applyPatch(gctx, target, p); err != nil {
					return err
				}
			}
			return nil
		})
	}
	err := g.Wait()
	return collapsed.Load(), err
}

// query calls f with each item of a control table's partition, in order.
func (w *writer) query(ctx context.Context, p string, f func(map[string]types.AttributeValue) error) error {
	pages := dynamodb.NewQueryPaginator(w.db, &dynamodb.QueryInput{
		TableName: aws.String(w.control), ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("#p = :p"),
		ExpressionAttributeNames:  map[string]string{"#p": "p"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": str(p)},
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return classify(fmt.Errorf("dynamodb: read staging: %w", err))
		}
		for _, it := range page.Items {
			if err := f(it); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRow writes a staged row. A keyed stream's put, or delete, leaves an
// item with a higher _openetl_seq as it is, except an overwrite read's,
// which replaces what's there. A soft delete keeps the item's other
// attributes, held ones too, and updates its metadata. A merged row's kept
// columns keep the target's values and holds. An append is a put, which,
// deduped on the target, skips an _openetl_id the table has: skipped says
// so.
func (w *writer) applyRow(ctx context.Context, cfg destination.Stream, target string, item map[string]types.AttributeValue, keep []string, tag string) (skipped bool, err error) {
	var cond *string
	names, values := map[string]string{}, map[string]types.AttributeValue{}
	if seq, ok := item[seqColumn]; ok && keyed(cfg) && tag == "" {
		cond = aws.String("attribute_not_exists(#s) OR #s <= :s")
		names["#s"], values[":s"] = seqColumn, seq
	}
	if !keyed(cfg) {
		if tag != "" {
			item[readColumn] = str(tag)
		} else if cfg.Dedupe == destination.DedupeTarget {
			cond = aws.String("attribute_not_exists(#i)")
			names["#i"] = idColumn
		}
		err := w.put(ctx, target, item, cond, names, values)
		if conditionFailed(err) {
			return cfg.Dedupe == destination.DedupeTarget, nil
		}
		return false, err
	}
	key := map[string]types.AttributeValue{}
	for _, k := range keySchema(cfg, arrow.NewSchema(nil, nil)) {
		key[k.name] = item[k.name]
	}
	if stringOf(item[opColumn]) == "delete" {
		if cfg.Deletes == destination.HardDelete {
			_, err := w.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(target), Key: key, ConditionExpression: cond,
				ExpressionAttributeNames: nonEmpty(names), ExpressionAttributeValues: nonEmpty(values),
			})
			if err != nil && !conditionFailed(err) {
				return false, classify(fmt.Errorf("dynamodb: delete from %s: %w", target, err))
			}
			return false, nil
		}
		cur, err := w.get(ctx, target, key)
		if err != nil {
			return false, err
		}
		if cur != nil {
			for _, c := range []string{idColumn, opColumn, seqColumn, syncedColumn} {
				if v, ok := item[c]; ok {
					cur[c] = v
				}
			}
			item = cur
		}
		item[deletedColumn] = &types.AttributeValueMemberBOOL{Value: true}
	} else if len(keep) > 0 {
		cur, err := w.get(ctx, target, key)
		if err != nil {
			return false, err
		}
		// The item's holds on the kept columns, which the row's own override,
		// so that a held version's patch still finds its hold.
		holds := map[string]types.AttributeValue{}
		curHeld, _ := cur[heldColumn].(*types.AttributeValueMemberM)
		for _, c := range keep {
			if v, ok := cur[c]; ok {
				item[c] = v
			} else {
				delete(item, c)
			}
			if curHeld != nil && curHeld.Value[c] != nil {
				holds[c] = curHeld.Value[c]
			}
		}
		if h, ok := item[heldColumn].(*types.AttributeValueMemberM); ok {
			maps.Copy(holds, h.Value)
		}
		if len(holds) > 0 {
			item[heldColumn] = &types.AttributeValueMemberM{Value: holds}
		}
	}
	if tag != "" {
		item[readColumn] = str(tag)
	}
	err = w.put(ctx, target, item, cond, names, values)
	if conditionFailed(err) {
		return false, nil
	}
	return false, err
}

func nonEmpty[V any](m map[string]V) map[string]V {
	if len(m) == 0 {
		return nil
	}
	return m
}

func (w *writer) put(ctx context.Context, target string, item map[string]types.AttributeValue, cond *string, names map[string]string, values map[string]types.AttributeValue) error {
	_, err := w.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(target), Item: item, ConditionExpression: cond,
		ExpressionAttributeNames: nonEmpty(names), ExpressionAttributeValues: nonEmpty(values),
	})
	if err != nil && !conditionFailed(err) {
		if apiCode(err) == "ValidationException" {
			return dataInvalid(fmt.Errorf("dynamodb: write to %s: %w", target, err))
		}
		return classify(fmt.Errorf("dynamodb: write to %s: %w", target, err))
	}
	return err
}

func (w *writer) get(ctx context.Context, target string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	res, err := w.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(target), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, classify(fmt.Errorf("dynamodb: read from %s: %w", target, err))
	}
	return res.Item, nil
}

// applyPatch fills each column a patch has, in the item its key names, or
// unkeyed, each item of its _openetl_id, where that column is still held by
// the patch's version, and clears the hold.
func (w *writer) applyPatch(ctx context.Context, target string, p map[string]types.AttributeValue) error {
	key, _ := p["k"].(*types.AttributeValueMemberM)
	fills, _ := p["f"].(*types.AttributeValueMemberM)
	if key == nil || fills == nil {
		return nil
	}
	if _, ok := p["q"]; !ok {
		cur, err := w.get(ctx, target, key.Value)
		if err != nil || cur == nil {
			return err
		}
		return w.fill(ctx, target, cur, stringOf(p["v"]), fills.Value)
	}
	pages := dynamodb.NewQueryPaginator(w.db, &dynamodb.QueryInput{
		TableName: aws.String(target), ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("#i = :i"),
		ExpressionAttributeNames:  map[string]string{"#i": idColumn},
		ExpressionAttributeValues: map[string]types.AttributeValue{":i": key.Value[idColumn]},
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return classify(fmt.Errorf("dynamodb: read from %s: %w", target, err))
		}
		for _, cur := range page.Items {
			if err := w.fill(ctx, target, cur, stringOf(p["v"]), fills.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

// fill fills an item's columns that version holds with fills' values,
// unless the item is no longer the version cur read, as when a replaced
// attempt's patch read it: what wrote over it or removed it since came
// after the patch's epoch was applied.
func (w *writer) fill(ctx context.Context, target string, cur map[string]types.AttributeValue, version string, fills map[string]types.AttributeValue) error {
	held, _ := cur[heldColumn].(*types.AttributeValueMemberM)
	if held == nil {
		return nil
	}
	changed := false
	for col, v := range fills {
		if stringOf(held.Value[col]) != version {
			continue
		}
		if _, null := v.(*types.AttributeValueMemberNULL); null {
			delete(cur, col)
		} else {
			cur[col] = v
		}
		delete(held.Value, col)
		changed = true
	}
	if !changed {
		return nil
	}
	if len(held.Value) == 0 {
		delete(cur, heldColumn)
	}
	err := w.put(ctx, target, cur, aws.String("#i = :i"), map[string]string{"#i": idColumn}, map[string]types.AttributeValue{":i": cur[idColumn]})
	if conditionFailed(err) {
		return nil
	}
	return err
}

// deletePartitions deletes the control table's items in the partitions.
func (w *writer) deletePartitions(ctx context.Context, parts []string) error {
	for _, p := range parts {
		var reqs []types.WriteRequest
		err := w.query(ctx, p, func(it map[string]types.AttributeValue) error {
			reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"p": it["p"], "n": it["n"]}}})
			return nil
		})
		if err != nil {
			return err
		}
		if err := writeBatches(ctx, w.db, w.control, reqs); err != nil {
			return err
		}
	}
	return nil
}

// dropParts deletes the control table's items whose partition starts with
// prefix and drop says to drop.
func (w *writer) dropParts(ctx context.Context, prefix string, drop func(p string) bool) error {
	in := &dynamodb.ScanInput{
		TableName: aws.String(w.control), ConsistentRead: aws.Bool(true),
		ProjectionExpression:     aws.String("#p, #n"),
		ExpressionAttributeNames: map[string]string{"#p": "p", "#n": "n"},
	}
	if prefix != "" {
		in.FilterExpression = aws.String("begins_with(#p, :p)")
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":p": str(prefix)}
	}
	pages := dynamodb.NewScanPaginator(w.db, in)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return classify(fmt.Errorf("dynamodb: scan %s: %w", w.control, err))
		}
		var reqs []types.WriteRequest
		for _, it := range page.Items {
			if drop(stringOf(it["p"])) {
				reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: it}})
			}
		}
		if err := writeBatches(ctx, w.db, w.control, reqs); err != nil {
			return err
		}
	}
	return nil
}

// Recover raises the fence to gen, then settles e: Applied where every
// stream's marker is e's, Redone after applying the streams whose marker
// isn't from their staging, Lost where their staging is gone. It then drops
// what earlier generations staged.
func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	_, err := w.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(w.control), Key: fenceKey(w.cfg.Pipeline),
		UpdateExpression:          aws.String("SET #g = :g"),
		ConditionExpression:       aws.String("attribute_not_exists(#g) OR #g <= :g"),
		ExpressionAttributeNames:  map[string]string{"#g": "gen"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":g": num(strconv.FormatUint(gen, 10))},
	})
	if conditionFailed(err) {
		return 0, destination.ErrFenced
	}
	if err != nil {
		return 0, classify(fmt.Errorf("dynamodb: raise the fence: %w", err))
	}
	rec := destination.Applied
	if !e.IsZero() {
		if rec, err = w.settleRecorded(ctx, e, committables); err != nil {
			return 0, err
		}
	}
	// A swap an earlier epoch's marker still has, which a crash cut short.
	markers, err := w.Markers(ctx)
	if err != nil {
		return 0, err
	}
	for _, name := range slices.Sorted(maps.Keys(markers)) {
		if err := w.Swap(ctx, markers[name]); err != nil {
			return 0, err
		}
	}
	prefix := stagePrefix(w.cfg.Pipeline)
	return rec, w.dropParts(ctx, prefix, func(p string) bool {
		g, _, _ := strings.Cut(strings.TrimPrefix(p, prefix), "#")
		v, err := strconv.ParseUint(g, 10, 64)
		return err == nil && v < gen
	})
}

func (w *writer) settleRecorded(ctx context.Context, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	streams, err := w.mergeLanes(e, committables)
	if err != nil {
		return 0, err
	}
	return docstore.SettleRecorded(ctx, "dynamodb", w, w.cfg.Scope, e, streams)
}

func (w *writer) Mark(s stagedStream, e destination.Epoch) marker {
	return docstore.Mark(e, s.Name, s.Mode == destination.Overwrite && s.Ended, s.Read, s.Target)
}

func (w *writer) DropStaging(ctx context.Context, s stagedStream) error {
	for _, parts := range s.parts {
		if err := w.deletePartitions(ctx, parts); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) Staging(ctx context.Context, s stagedStream) (present, missing int, err error) {
	for _, ps := range s.parts {
		for _, p := range ps {
			res, err := w.db.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String(w.control), ConsistentRead: aws.Bool(true), Limit: aws.Int32(1),
				KeyConditionExpression:    aws.String("#p = :p"),
				ExpressionAttributeNames:  map[string]string{"#p": "p"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":p": str(p)},
			})
			if err != nil {
				return 0, 0, classify(fmt.Errorf("dynamodb: read staging: %w", err))
			}
			if len(res.Items) > 0 {
				present++
			} else {
				missing++
			}
		}
	}
	return present, missing, nil
}

func (w *writer) Close(context.Context) error { return nil }

// batchAPI is what writeBatches calls.
type batchAPI interface {
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// unprocessedPause is how long writeBatches waits before sending a batch's
// unprocessed items again, the nth time in a row: between half and all of a
// bound that doubles from 50ms to 5s, at random.
var unprocessedPause = func(n int) time.Duration {
	bound := min(5*time.Second, 50*time.Millisecond<<min(n, 7))
	return bound/2 + rand.N(bound/2+1)
}

// writeBatches sends reqs to table 25 at a time, sending again, after a
// pause, what DynamoDB left unprocessed, as it does when the table's
// throughput is exceeded; a throttled request is the client's to retry.
func writeBatches(ctx context.Context, db batchAPI, table string, reqs []types.WriteRequest) error {
	for chunk := range slices.Chunk(reqs, 25) {
		pending := chunk
		for n := 0; len(pending) > 0; n++ {
			res, err := db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{table: pending}})
			if apiCode(err) == "ValidationException" {
				return dataInvalid(fmt.Errorf("dynamodb: write to %s: %w", table, err))
			}
			if err != nil {
				return classify(fmt.Errorf("dynamodb: write to %s: %w", table, err))
			}
			pending = res.UnprocessedItems[table]
			if len(pending) == 0 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(unprocessedPause(n)):
			}
		}
	}
	return nil
}

// ensureTable creates a table keyed on keys, billed on demand, unless it's
// there, and waits for it to be active.
func (w *writer) ensureTable(ctx context.Context, name string, keys []keyAttr) error {
	in := &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest}
	for i, k := range keys {
		kt := types.KeyTypeHash
		if i > 0 {
			kt = types.KeyTypeRange
		}
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: aws.String(k.name), KeyType: kt})
		in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(k.name), AttributeType: k.typ})
	}
	d, err := w.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	switch {
	case err == nil && d.Table.TableStatus == types.TableStatusActive:
		return nil
	case err == nil:
	case apiCode(err) == "ResourceNotFoundException":
		if _, err := w.db.CreateTable(ctx, in); err != nil && apiCode(err) != "ResourceInUseException" {
			return classify(fmt.Errorf("dynamodb: create table %s: %w", name, err))
		}
	default:
		return classify(fmt.Errorf("dynamodb: describe %s: %w", name, err))
	}
	if err := dynamodb.NewTableExistsWaiter(w.db).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}, 5*time.Minute); err != nil {
		return classify(fmt.Errorf("dynamodb: wait for table %s: %w", name, err))
	}
	return nil
}
