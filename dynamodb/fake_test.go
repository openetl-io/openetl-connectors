package dynamodb

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// memDB is an in-memory DynamoDB of the calls the connectors make, with the
// condition, filter and update expressions they write. Each table is keyed
// on string attributes.
type memDB struct {
	mu     sync.Mutex
	tables map[string]*memTable
	// Every Scan's input.
	scans []*dynamodb.ScanInput
	// What the next GetItem runs after it has read, and before it returns.
	afterGet func()
}

type memTable struct {
	keys  []string
	items map[string]map[string]types.AttributeValue
}

// newMemDB has the tables, each keyed on its attributes: hash, then range.
func newMemDB(tables map[string][]string) *memDB {
	m := &memDB{tables: map[string]*memTable{}}
	for name, keys := range tables {
		m.tables[name] = &memTable{keys: keys, items: map[string]map[string]types.AttributeValue{}}
	}
	return m
}

func (m *memDB) table(name *string) (*memTable, error) {
	t := m.tables[aws.ToString(name)]
	if t == nil {
		return nil, &types.ResourceNotFoundException{Message: aws.String("no table " + aws.ToString(name))}
	}
	return t, nil
}

func (t *memTable) id(item map[string]types.AttributeValue) string {
	k := map[string]types.AttributeValue{}
	for _, n := range t.keys {
		k[n] = item[n]
	}
	return string(appendMap(nil, k, true))
}

// all is the table's items in key order.
func (t *memTable) all() []map[string]types.AttributeValue {
	var out []map[string]types.AttributeValue
	for _, id := range slices.Sorted(maps.Keys(t.items)) {
		out = append(out, maps.Clone(t.items[id]))
	}
	return out
}

// eval says whether it meets an expression of terms joined by OR, each of
// factors joined by AND, in parentheses or not: attribute_not_exists(a),
// begins_with(a, v), a OP v, and NOT of one of them.
func eval(expr *string, names map[string]string, values map[string]types.AttributeValue, it map[string]types.AttributeValue) bool {
	if expr == nil {
		return true
	}
	for term := range strings.SplitSeq(*expr, " OR ") {
		term = strings.TrimSpace(term)
		if strings.HasPrefix(term, "(") && strings.HasSuffix(term, ")") {
			term = term[1 : len(term)-1]
		}
		all := true
		for f := range strings.SplitSeq(term, " AND ") {
			all = all && factor(f, names, values, it)
		}
		if all {
			return true
		}
	}
	return false
}

func factor(f string, names map[string]string, values map[string]types.AttributeValue, it map[string]types.AttributeValue) bool {
	f = strings.TrimSpace(f)
	if g, ok := strings.CutPrefix(f, "NOT "); ok {
		return !factor(g, names, values, it)
	}
	name := func(s string) string {
		s = strings.TrimSpace(s)
		if n, ok := names[s]; ok {
			return n
		}
		return s
	}
	if a, ok := strings.CutPrefix(f, "attribute_not_exists("); ok {
		_, has := it[name(strings.TrimSuffix(a, ")"))]
		return !has
	}
	if a, ok := strings.CutPrefix(f, "begins_with("); ok {
		attr, v, _ := strings.Cut(strings.TrimSuffix(a, ")"), ",")
		s, _ := it[name(attr)].(*types.AttributeValueMemberS)
		return s != nil && strings.HasPrefix(s.Value, values[strings.TrimSpace(v)].(*types.AttributeValueMemberS).Value)
	}
	for _, op := range []string{"<=", ">=", "<>", "="} {
		l, r, ok := strings.Cut(f, " "+op+" ")
		if !ok {
			continue
		}
		a, has := it[name(l)]
		if !has {
			return false
		}
		c, ok := compareValues(a, values[strings.TrimSpace(r)])
		if !ok {
			c = 1
		}
		return op == "<=" && c <= 0 || op == ">=" && c >= 0 || op == "<>" && c != 0 || op == "=" && c == 0
	}
	return false
}

// clone copies an item and the maps in it, as a call to DynamoDB does.
func clone(it map[string]types.AttributeValue) map[string]types.AttributeValue {
	if it == nil {
		return nil
	}
	out := maps.Clone(it)
	for k, v := range out {
		if m, ok := v.(*types.AttributeValueMemberM); ok {
			out[k] = &types.AttributeValueMemberM{Value: clone(m.Value)}
		}
	}
	return out
}

func failed() error {
	return &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
}

func (m *memDB) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, reqs := range in.RequestItems {
		t, err := m.table(aws.String(name))
		if err != nil {
			return nil, err
		}
		if len(reqs) > 25 {
			return nil, fmt.Errorf("ValidationException: %d requests", len(reqs))
		}
		seen := map[string]bool{}
		for _, r := range reqs {
			var it map[string]types.AttributeValue
			if r.PutRequest != nil {
				it = r.PutRequest.Item
			} else {
				it = r.DeleteRequest.Key
			}
			if seen[t.id(it)] {
				return nil, fmt.Errorf("ValidationException: Provided list of item keys contains duplicates")
			}
			seen[t.id(it)] = true
		}
		for _, r := range reqs {
			if r.PutRequest != nil {
				t.items[t.id(r.PutRequest.Item)] = maps.Clone(r.PutRequest.Item)
			} else {
				delete(t.items, t.id(r.DeleteRequest.Key))
			}
		}
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (m *memDB) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	t, err := m.table(in.TableName)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	out := &dynamodb.GetItemOutput{Item: clone(t.items[t.id(in.Key)])}
	after := m.afterGet
	m.afterGet = nil
	m.mu.Unlock()
	if after != nil {
		after()
	}
	return out, nil
}

func (m *memDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	id := t.id(in.Item)
	if !eval(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, t.items[id]) {
		return nil, failed()
	}
	t.items[id] = clone(in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *memDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	id := t.id(in.Key)
	if !eval(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, t.items[id]) {
		return nil, failed()
	}
	delete(t.items, id)
	return &dynamodb.DeleteItemOutput{}, nil
}

// UpdateItem takes SET a = v, ... .
func (m *memDB) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	id := t.id(in.Key)
	if !eval(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, t.items[id]) {
		return nil, failed()
	}
	it := t.items[id]
	if it == nil {
		it = maps.Clone(in.Key)
	}
	for set := range strings.SplitSeq(strings.TrimPrefix(aws.ToString(in.UpdateExpression), "SET "), ",") {
		l, r, _ := strings.Cut(set, "=")
		it[in.ExpressionAttributeNames[strings.TrimSpace(l)]] = in.ExpressionAttributeValues[strings.TrimSpace(r)]
	}
	t.items[id] = it
	return &dynamodb.UpdateItemOutput{}, nil
}

// Query takes a key condition a = v.
func (m *memDB) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	var out []map[string]types.AttributeValue
	for _, it := range t.all() {
		if eval(in.KeyConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, it) {
			out = append(out, it)
		}
	}
	if in.Limit != nil && len(out) > int(*in.Limit) {
		out = out[:*in.Limit]
	}
	return &dynamodb.QueryOutput{Items: out}, nil
}

func (m *memDB) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scans = append(m.scans, in)
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	var out []map[string]types.AttributeValue
	for _, it := range t.all() {
		if !eval(in.FilterExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, it) {
			continue
		}
		if in.ProjectionExpression != nil {
			p := map[string]types.AttributeValue{}
			for a := range strings.SplitSeq(*in.ProjectionExpression, ",") {
				n := in.ExpressionAttributeNames[strings.TrimSpace(a)]
				if v, ok := it[n]; ok {
					p[n] = v
				}
			}
			it = p
		}
		out = append(out, it)
	}
	return &dynamodb.ScanOutput{Items: out}, nil
}

func (m *memDB) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, err := m.table(in.TableName)
	if err != nil {
		return nil, err
	}
	d := &types.TableDescription{TableName: in.TableName, TableStatus: types.TableStatusActive}
	for i, k := range t.keys {
		kt := types.KeyTypeHash
		if i > 0 {
			kt = types.KeyTypeRange
		}
		d.KeySchema = append(d.KeySchema, types.KeySchemaElement{AttributeName: aws.String(k), KeyType: kt})
		d.AttributeDefinitions = append(d.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(k), AttributeType: types.ScalarAttributeTypeS})
	}
	return &dynamodb.DescribeTableOutput{Table: d}, nil
}

func (m *memDB) CreateTable(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for _, k := range in.KeySchema {
		keys = append(keys, aws.ToString(k.AttributeName))
	}
	m.tables[aws.ToString(in.TableName)] = &memTable{keys: keys, items: map[string]map[string]types.AttributeValue{}}
	return &dynamodb.CreateTableOutput{}, nil
}

func (m *memDB) ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &dynamodb.ListTablesOutput{TableNames: slices.Sorted(maps.Keys(m.tables))}, nil
}

// items are a table's items.
func (m *memDB) items(name string) []map[string]types.AttributeValue {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tables[name].all()
}

// memWriter is a writer of generation 1 into db, of pipeline p, without a
// namespace or a prefix, which has recovered.
func memWriter(t *testing.T, db *memDB, streams ...destination.Stream) *writer {
	t.Helper()
	w := &writer{
		db: db, cfg: destination.OpenConfig{Pipeline: "p", Generation: 1, Streams: streams, Scope: destination.ScopePipeline},
		control: controlTable, streams: map[string]destination.Stream{}, targets: map[string]string{},
		epochs: map[destination.Epoch]map[string]*laneStream{},
	}
	for _, s := range streams {
		w.streams[s.Name] = s
	}
	if _, err := w.Recover(t.Context(), 1, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	return w
}

// memCommit writes b to stream in epoch n of generation 1, and commits it.
func memCommit(t *testing.T, w *writer, n uint64, stream string, b arrow.RecordBatch) destination.Committed {
	t.Helper()
	defer b.Release()
	e := destination.Epoch{Generation: 1, N: n}
	if err := w.Write(t.Context(), e, stream, b); err != nil {
		t.Fatal(err)
	}
	staged, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.Commit(t.Context(), e, []destination.Staged{staged})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// memBatch builds a batch of string columns; nil is null.
func memBatch(names []string, rows ...[]*string) arrow.RecordBatch {
	var fields []arrow.Field
	for _, n := range names {
		fields = append(fields, arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer b.Release()
	for _, r := range rows {
		for i, v := range r {
			if v == nil {
				b.Field(i).AppendNull()
			} else {
				b.Field(i).(*array.StringBuilder).Append(*v)
			}
		}
	}
	return b.NewRecordBatch()
}

func sp(s string) *string { return &s }
