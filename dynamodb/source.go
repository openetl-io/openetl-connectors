package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl-connectors/docstore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	Connection
	// The tables to offer. Empty: every table the credentials can list.
	Tables []string `json:"tables"`
	// How many items discovery reads of each table, from random segments,
	// to find its attributes and their types. 0: every item.
	SampleSize int `json:"sampleSize"`
	// Parallel scan segments a table is read in. 0: one per GiB the table
	// holds, up to 64.
	Segments int `json:"segments"`
	// Items per Scan request. 0: as many as fit DynamoDB's 1 MB page.
	ScanLimit int `json:"scanLimit"`
	// Each stream is the table's key attributes and data, the whole item in
	// DynamoDB JSON, instead of a column per top-level attribute.
	Packed bool `json:"packed"`
}

// dataColumn is a packed stream's item.
const dataColumn = "data"

// Source reads a region's tables, each a stream keyed on the table's key: a
// full refresh scans it, in parallel segments where it's read in splits; an
// incremental read scans it for the items whose cursor attribute is at or
// past a floor; cdc scans it once and then reads its DynamoDB stream
// (cdc.go).
type Source struct {
	cfg    SourceConfig
	region string
}

var (
	_ source.Source            = (*Source)(nil)
	_ source.Splitter          = (*Source)(nil)
	_ source.RetentionReporter = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	var cfg SourceConfig
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, configInvalid(err)
	}
	if cfg.SampleSize < 0 || cfg.Segments < 0 || cfg.Segments > 1_000_000 || cfg.ScanLimit < 0 {
		return nil, configInvalid(errors.New("dynamodb: sampleSize, segments and scanLimit can't be negative, and segments is at most 1000000"))
	}
	return &Source{cfg: cfg}, nil
}

// connect makes clients that retry, for a read.
func (s *Source) connect(ctx context.Context) (*clients, error) { return s.cfg.connect(ctx, 10) }

// table is what DescribeTable says of a table that a read needs.
type table struct {
	name string
	// The hash key, and the range key if there is one.
	keys []string
	// The key attributes' types: S, N or B.
	keyTypes map[string]string
	// Empty where the table has no stream enabled.
	streamARN, viewType string
	sizeBytes, items    int64
}

func describe(ctx context.Context, cl *clients, name string) (*table, error) {
	res, err := cl.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		if apiCode(err) == "ResourceNotFoundException" {
			return nil, configInvalid(fmt.Errorf("dynamodb: table %s doesn't exist", name))
		}
		return nil, classify(fmt.Errorf("dynamodb: describe table %s: %w", name, err))
	}
	d := res.Table
	t := &table{name: name, keyTypes: map[string]string{}, sizeBytes: aws.ToInt64(d.TableSizeBytes), items: aws.ToInt64(d.ItemCount)}
	for _, kt := range []types.KeyType{types.KeyTypeHash, types.KeyTypeRange} {
		for _, k := range d.KeySchema {
			if k.KeyType == kt {
				t.keys = append(t.keys, aws.ToString(k.AttributeName))
			}
		}
	}
	for _, a := range d.AttributeDefinitions {
		t.keyTypes[aws.ToString(a.AttributeName)] = string(a.AttributeType)
	}
	if d.StreamSpecification != nil && aws.ToBool(d.StreamSpecification.StreamEnabled) && d.LatestStreamArn != nil {
		t.streamARN, t.viewType = aws.ToString(d.LatestStreamArn), string(d.StreamSpecification.StreamViewType)
	}
	return t, nil
}

// tables are the tables offered as streams, in order.
func (s *Source) tables(ctx context.Context, cl *clients) ([]string, error) {
	var names []string
	p := dynamodb.NewListTablesPaginator(cl.db, &dynamodb.ListTablesInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, classify(fmt.Errorf("dynamodb: list tables: %w", err))
		}
		names = append(names, page.TableNames...)
	}
	if len(s.cfg.Tables) > 0 {
		names = slices.DeleteFunc(names, func(n string) bool { return !slices.Contains(s.cfg.Tables, n) })
	}
	slices.Sort(names)
	return names, nil
}

// The type of a cdc stream's seq (cdc.go).
var seqType = docstore.SeqType

// Discover offers each table with its key attributes first, typed by their
// definitions, and the other top-level attributes its items have, each
// declared as attrTypes says, from every item unless sampleSize says how
// many to read; packed, with the key attributes and data. A table with a
// stream enabled is offered for cdc too.
func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	cl, err := s.connect(ctx)
	if err != nil {
		return source.Catalog{}, err
	}
	names, err := s.tables(ctx, cl)
	if err != nil {
		return source.Catalog{}, err
	}
	var cat source.Catalog
	for _, name := range names {
		t, err := describe(ctx, cl, name)
		if err != nil {
			return source.Catalog{}, err
		}
		attrs, err := s.attributes(ctx, cl, t)
		if err != nil {
			return source.Catalog{}, err
		}
		var fields []arrow.Field
		for _, k := range t.keys {
			fields = append(fields, arrow.Field{Name: k, Type: keyType(t.keyTypes[k], *attrs[k])})
		}
		if s.cfg.Packed {
			fields = append(fields, arrow.Field{Name: dataColumn, Type: jsonType, Nullable: true})
		} else {
			for _, a := range slices.Sorted(maps.Keys(attrs)) {
				if !slices.Contains(t.keys, a) {
					fields = append(fields, arrow.Field{Name: a, Type: attrs[a].arrowType(), Nullable: true})
				}
			}
		}
		cs := source.CatalogStream{
			Name: name, PrimaryKey: t.keys, SplitsByKey: true, Replay: source.Exact,
			Schema:    arrow.NewSchema(fields, nil),
			SyncModes: []source.SyncMode{source.FullRefresh, source.Incremental},
		}
		if t.streamARN != "" {
			cs.SyncModes, cs.Seq = append(cs.SyncModes, source.CDC), seqType
		}
		cat.Streams = append(cat.Streams, cs)
	}
	return cat, nil
}

// attributes are the types of the table's top-level attributes' values,
// key attributes included, over every item, or over sampleSize items from
// random segments. Packed, only the keys': read alone, and only where a key
// is a number, whose type depends on its values.
func (s *Source) attributes(ctx context.Context, cl *clients, t *table) (map[string]*attrTypes, error) {
	var mu sync.Mutex
	out := map[string]*attrTypes{}
	for _, k := range t.keys {
		out[k] = &attrTypes{}
	}
	var proj []string
	if s.cfg.Packed {
		if !slices.ContainsFunc(t.keys, func(k string) bool { return t.keyTypes[k] == "N" }) {
			return out, nil
		}
		proj = t.keys
	}
	n := 0
	add := func(items []map[string]types.AttributeValue) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, it := range items {
			for k, v := range it {
				if s.cfg.Packed && !slices.Contains(t.keys, k) {
					continue
				}
				a := out[k]
				if a == nil {
					a = &attrTypes{}
					out[k] = a
				}
				a.add(v)
			}
		}
		n += len(items)
		return s.cfg.SampleSize > 0 && n >= s.cfg.SampleSize
	}
	scan := func(ctx context.Context, seg, total int) error {
		return s.scanPages(ctx, cl, t.name, seg, total, nil, nil, proj, false, func(items []map[string]types.AttributeValue, _ map[string]types.AttributeValue) (bool, error) {
			return add(items), nil
		})
	}
	if s.cfg.SampleSize > 0 && t.items > int64(s.cfg.SampleSize) {
		// Segments of about a quarter of the sample each, read in random
		// order until the sample is full: random items, not the first in
		// the table's hash order, so that the next discovery finds what this
		// one missed.
		total := int(min(1_000_000, max(1, t.items*4/int64(s.cfg.SampleSize))))
		for _, seg := range rand.Perm(total) {
			if err := scan(ctx, seg, total); err != nil {
				return nil, err
			}
			if n >= s.cfg.SampleSize {
				break
			}
		}
		return out, nil
	}
	total := 1
	if s.cfg.SampleSize == 0 {
		total = s.segments(t)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for seg := range total {
		g.Go(func() error { return scan(gctx, seg, total) })
	}
	return out, g.Wait()
}

// segments is how many parallel scan segments a table is read in.
func (s *Source) segments(t *table) int {
	if s.cfg.Segments > 0 {
		return s.cfg.Segments
	}
	return int(min(64, max(1, (t.sizeBytes+(1<<30)-1)>>30)))
}

// scanPages scans one segment of total (total 1: the whole table) from the
// key after, with the filter if there is one, calling page with each page's
// items, of only the attributes proj names unless it's nil, and the key it
// ended at, nil at the segment's end. page returns true to stop.
func (s *Source) scanPages(ctx context.Context, cl *clients, tableName string, seg, total int, after map[string]types.AttributeValue, filter *scanFilter, proj []string, consistent bool, page func(items []map[string]types.AttributeValue, last map[string]types.AttributeValue) (bool, error)) error {
	in := &dynamodb.ScanInput{TableName: aws.String(tableName), ExclusiveStartKey: after, ConsistentRead: aws.Bool(consistent)}
	if total > 1 {
		in.Segment, in.TotalSegments = aws.Int32(int32(seg)), aws.Int32(int32(total))
	}
	if s.cfg.ScanLimit > 0 {
		in.Limit = aws.Int32(int32(min(s.cfg.ScanLimit, 1<<30)))
	}
	if filter != nil {
		in.FilterExpression = aws.String("#c >= :f")
		in.ExpressionAttributeNames = map[string]string{"#c": filter.cursor}
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":f": filter.floor}
	}
	if proj != nil {
		in.ExpressionAttributeNames = map[string]string{}
		var names []string
		for i, a := range proj {
			names = append(names, fmt.Sprintf("#k%d", i))
			in.ExpressionAttributeNames[names[i]] = a
		}
		in.ProjectionExpression = aws.String(strings.Join(names, ", "))
	}
	for {
		res, err := cl.db.Scan(ctx, in)
		if err != nil {
			if apiCode(err) == "ResourceNotFoundException" {
				return configInvalid(fmt.Errorf("dynamodb: table %s doesn't exist", tableName))
			}
			return classify(fmt.Errorf("dynamodb: scan %s: %w", tableName, err))
		}
		last := res.LastEvaluatedKey
		if len(last) == 0 {
			last = nil
		}
		stop, err := page(res.Items, last)
		if err != nil || stop || last == nil {
			return err
		}
		in.ExclusiveStartKey = last
	}
}

// scanFilter is an incremental read's: the items whose cursor is at or past
// floor.
type scanFilter struct {
	cursor string
	floor  types.AttributeValue
}

// Splits are a full refresh's or an incremental read's parallel scan
// segments, the table's size deciding how many, unless the last read's
// splits haven't ended, or say where the next starts: then those. A cdc
// stream is read whole.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if cs.SyncMode == source.CDC || len(ended) == 1 && ended[0].ID == source.WholeStream {
		return source.Whole(ended)
	}
	if slices.ContainsFunc(ended, func(sp source.Split) bool { return len(sp.State) > 0 }) {
		return ended, nil
	}
	if len(ended) > 1 {
		out := make([]source.Split, len(ended))
		for i, sp := range ended {
			out[i] = source.Split{ID: sp.ID}
		}
		return out, nil
	}
	cl, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	t, err := describe(ctx, cl, cs.Name)
	if err != nil {
		return nil, err
	}
	n := s.segments(t)
	if n == 1 {
		return []source.Split{{ID: source.WholeStream}}, nil
	}
	out := make([]source.Split, n)
	for i := range out {
		out[i] = source.Split{ID: segmentID(i, n)}
	}
	return out, nil
}

func segmentID(seg, total int) string { return fmt.Sprintf("segment-%d-of-%d", seg, total) }

func parseSegmentID(id string) (seg, total int, err error) {
	if id == source.WholeStream {
		return 0, 1, nil
	}
	if _, err := fmt.Sscanf(id, "segment-%d-of-%d", &seg, &total); err != nil || seg < 0 || seg >= total {
		return 0, 0, fmt.Errorf("dynamodb: split %q isn't a scan segment", id)
	}
	return seg, total, nil
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if len(req.Streams) == 0 {
		return nil
	}
	cl, err := s.connect(ctx)
	if err != nil {
		return err
	}
	scans := req
	scans.Streams = nil
	var cdc []source.ConfiguredStream
	for _, cs := range req.Streams {
		if cs.SyncMode == source.CDC {
			cdc = append(cdc, cs)
		} else {
			scans.Streams = append(scans.Streams, cs)
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	if len(scans.Streams) > 0 {
		g.Go(func() error {
			return source.ReadSplits(gctx, scans, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, em source.SplitEmitter) error {
				return s.readScan(ctx, cl, cs, sp, em)
			})
		})
	}
	for _, cs := range cdc {
		g.Go(func() error {
			return s.readCDC(gctx, cl, cs, out, req.Continuous)
		})
	}
	return g.Wait()
}

// segState is where a scan segment's read is: a split's state, or a whole
// stream's.
type segState struct {
	// The key the last page ended at: the scan resumes after it. Nil: at
	// the segment's start.
	After item `json:"after,omitempty"`
	// Incremental: the filter's floor, nil for every item; the highest
	// cursor value the read before this one saw; and the highest this read
	// has seen so far.
	Floor *value `json:"floor,omitempty"`
	Last  *value `json:"last,omitempty"`
	Max   *value `json:"max,omitempty"`
	// A cdc snapshot's segment read to its end.
	Done bool `json:"done,omitempty"`
}

// readScan reads a split, or a stream whole, by scanning its segment from
// its state, with a resume point after every page. An incremental read
// reads the items whose cursor is at or past the highest value the read
// before last saw, and completes saying where the next read starts: at the
// highest value the read before this one saw. A write that a scan has
// passed comes after every value the read before saw, so the next read's
// floor is at or below it, however late in this read it came.
//
// A read with an until, a resume point's state, reads to the item at its
// key and stops there.
func (s *Source) readScan(ctx context.Context, cl *clients, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	seg, total, err := parseSegmentID(sp.ID)
	if err != nil {
		return err
	}
	var st, until segState
	if len(sp.State) > 0 {
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return fmt.Errorf("dynamodb: state of %s: %w", cs.Name, err)
		}
	}
	if sp.ID == source.WholeStream && sp.Until == nil {
		sp.Until = cs.Until
	}
	if sp.Until != nil {
		if err := json.Unmarshal(sp.Until, &until); err != nil {
			return fmt.Errorf("dynamodb: until of %s: %w", cs.Name, err)
		}
		if until.After == nil {
			return completeAt(out, sp.Until)
		}
	}
	incremental := cs.SyncMode == source.Incremental
	if incremental && cs.Cursor == "" {
		return configInvalid(fmt.Errorf("dynamodb: %s is read incrementally, which needs a cursor attribute", cs.Name))
	}
	t, err := describe(ctx, cl, cs.Name)
	if err != nil {
		return err
	}
	var filter *scanFilter
	if incremental && st.Floor != nil {
		filter = &scanFilter{cursor: cs.Cursor, floor: st.Floor.AttributeValue}
	}
	b := s.newBatcher(cs, t, false)
	defer b.Release()
	reached := false
	err = s.scanPages(ctx, cl, t.name, seg, total, st.After.attrs(), filter, nil, false, func(items []map[string]types.AttributeValue, last map[string]types.AttributeValue) (bool, error) {
		if until.After != nil {
			var n int
			n, reached = untilCut(items, last, until.After.attrs())
			items = items[:n]
		}
		for _, it := range items {
			if err := b.add(it, "", decimal128.Num{}, true); err != nil {
				return false, err
			}
			if incremental {
				if c, ok := it[cs.Cursor]; ok {
					st.Max = highest(st.Max, c)
				}
			}
		}
		if err := b.Flush(out); err != nil {
			return false, err
		}
		if reached || last == nil {
			return reached, nil
		}
		st.After = itemOf(last)
		next, err := json.Marshal(st)
		if err != nil {
			return false, err
		}
		return false, out.ResumePoint(next)
	})
	if err != nil {
		return err
	}
	if sp.Until != nil {
		return completeAt(out, sp.Until)
	}
	if !incremental {
		return out.Complete(nil)
	}
	next, err := json.Marshal(segState{Floor: st.Last, Last: highest(st.Last, st.Max.av())})
	if err != nil {
		return err
	}
	return out.Complete(next)
}

// completeAt ends a read at until: a resume point there, and the
// completion.
func completeAt(out source.SplitEmitter, until []byte) error {
	if err := out.ResumePoint(until); err != nil {
		return err
	}
	return out.Complete(until)
}

// untilCut is how many of a scan page's items a read until the item at key
// reads: those up to that item, or all; reached says it's there, at that
// item or at the page's end.
func untilCut(items []map[string]types.AttributeValue, last, key map[string]types.AttributeValue) (n int, reached bool) {
	want := keyText(key, key)
	for i, it := range items {
		if keyText(it, key) == want {
			return i + 1, true
		}
	}
	return len(items), last != nil && keyText(last, key) == want
}

// keyText is it's values of key's attributes, as typed JSON; "" if it lacks
// one.
func keyText(it, key map[string]types.AttributeValue) string {
	m := make(map[string]types.AttributeValue, len(key))
	for k := range key {
		v, ok := it[k]
		if !ok {
			return ""
		}
		m[k] = v
	}
	return string(appendJSON(nil, &types.AttributeValueMemberM{Value: m}, true))
}

// highest is the higher of a cursor's highest value so far and v. Values of
// different types don't compare: numbers win over strings, and strings
// over binaries, whatever their order, so that the choice doesn't depend on
// which the read met first; booleans and the rest don't count.
func highest(cur *value, v types.AttributeValue) *value {
	rank := func(av types.AttributeValue) int {
		switch av.(type) {
		case *types.AttributeValueMemberN:
			return 3
		case *types.AttributeValueMemberS:
			return 2
		case *types.AttributeValueMemberB:
			return 1
		}
		return 0
	}
	switch {
	case v == nil || rank(v) == 0:
		return cur
	case cur == nil || rank(v) > rank(cur.AttributeValue):
		return &value{v}
	case rank(v) < rank(cur.AttributeValue):
		return cur
	}
	if c, _ := compareValues(v, cur.AttributeValue); c > 0 {
		return &value{v}
	}
	return cur
}

// value is an attribute value that keeps its type through JSON, as DynamoDB
// JSON: a cursor in a state.
type value struct{ types.AttributeValue }

func (v *value) av() types.AttributeValue {
	if v == nil {
		return nil
	}
	return v.AttributeValue
}

func (v value) MarshalJSON() ([]byte, error) { return appendJSON(nil, v.AttributeValue, true), nil }

func (v *value) UnmarshalJSON(b []byte) error {
	av, err := parseTyped(b)
	if err != nil {
		return err
	}
	v.AttributeValue = av
	return nil
}

// item is a key or an item that keeps its types through JSON.
type item map[string]value

func itemOf(m map[string]types.AttributeValue) item {
	if m == nil {
		return nil
	}
	out := make(item, len(m))
	for k, v := range m {
		out[k] = value{v}
	}
	return out
}

func (it item) attrs() map[string]types.AttributeValue {
	if it == nil {
		return nil
	}
	out := make(map[string]types.AttributeValue, len(it))
	for k, v := range it {
		out[k] = v.AttributeValue
	}
	return out
}

// parseTyped reads an attribute value in DynamoDB JSON, as appendJSON
// writes it typed.
func parseTyped(b []byte) (types.AttributeValue, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("dynamodb: a typed value: %w", err)
	}
	if len(m) != 1 {
		return nil, fmt.Errorf("dynamodb: a typed value has %d types", len(m))
	}
	for k, raw := range m {
		switch k {
		case "S":
			var v string
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberS{Value: v}, err
		case "N":
			var v string
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberN{Value: v}, err
		case "B":
			var v []byte
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberB{Value: v}, err
		case "BOOL":
			var v bool
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberBOOL{Value: v}, err
		case "NULL":
			return &types.AttributeValueMemberNULL{Value: true}, nil
		case "SS":
			var v []string
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberSS{Value: v}, err
		case "NS":
			var v []string
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberNS{Value: v}, err
		case "BS":
			var v [][]byte
			err := json.Unmarshal(raw, &v)
			return &types.AttributeValueMemberBS{Value: v}, err
		case "M":
			var v map[string]json.RawMessage
			if err := json.Unmarshal(raw, &v); err != nil {
				return nil, err
			}
			out := make(map[string]types.AttributeValue, len(v))
			for mk, mv := range v {
				av, err := parseTyped(mv)
				if err != nil {
					return nil, err
				}
				out[mk] = av
			}
			return &types.AttributeValueMemberM{Value: out}, nil
		case "L":
			var v []json.RawMessage
			if err := json.Unmarshal(raw, &v); err != nil {
				return nil, err
			}
			out := make([]types.AttributeValue, len(v))
			for i, e := range v {
				av, err := parseTyped(e)
				if err != nil {
					return nil, err
				}
				out[i] = av
			}
			return &types.AttributeValueMemberL{Value: out}, nil
		}
		return nil, fmt.Errorf("dynamodb: a typed value of unknown type %q", k)
	}
	return nil, nil
}

// newFieldError is the error of an attribute the run doesn't declare.
// Discovery is given the config alone, and the source keeps nothing between
// calls, so nothing tells the next discovery about the attribute: one that
// reads every item finds it, and a sample can miss it.
func (s *Source) newFieldError(stream, attr string) error {
	err := fmt.Errorf("dynamodb: %s has a new attribute %q; the read starts again so that discovery declares it", stream, attr)
	if s.cfg.SampleSize > 0 {
		err = fmt.Errorf("%w, but discovery reads a sample of %d items, which can miss it again: set sampleSize to 0 to read every item, or packed to read items whole", err, s.cfg.SampleSize)
	}
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
}
