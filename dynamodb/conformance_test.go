//go:build integration

package dynamodb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// kinds are items with every attribute type, at their extremes, and an
// attribute whose type varies.
func kinds() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"id": avI(1), "s": avS("héllo ✓"), "n": avN("12345678901234567890123456789012345678"), "f": avN("-1.5"),
			"b": &types.AttributeValueMemberB{Value: []byte{0, 255}}, "bool": &types.AttributeValueMemberBOOL{Value: true},
			"null": &types.AttributeValueMemberNULL{Value: true},
			"m":    &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"a": &types.AttributeValueMemberL{Value: []types.AttributeValue{avI(1), avS("x")}}}},
			"l":    &types.AttributeValueMemberL{Value: []types.AttributeValue{avN("1E+125"), &types.AttributeValueMemberNULL{Value: true}}},
			"ss":   &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			"ns":   &types.AttributeValueMemberNS{Value: []string{"1", "2.5"}},
			"bs":   &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}},
			"i":    avN("9223372036854775807"), "varies": avI(1), "wide": avN("1E-130"),
		},
		{"id": avI(2), "i": avN("-9223372036854775808"), "f": avN("0.000000001"), "varies": avS("text"), "wide": avN("9.9999999999999999999999999999999999999E+125"), "s": avS("")},
		{"id": avI(3), "varies": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}, "l": &types.AttributeValueMemberL{}},
	}
}

func TestSourceConformance(t *testing.T) {
	// Items as columns, each table read in three parallel scan segments.
	t.Run("fields", func(t *testing.T) { sourceConformance(t, `, "segments": 3`) })
	// Items whole, each table read in one.
	t.Run("packed", func(t *testing.T) { sourceConformance(t, `, "packed": true`) })
}

func sourceConformance(t *testing.T, extra string) {
	cl := testClient(t)
	p := prefix(t)
	users, events, kindsT, orders, keysOnly := p+"users", p+"events", p+"kinds", p+"orders", p+"keysonly"
	createTable(t, cl, users, "", key{"id", types.ScalarAttributeTypeN})
	createTable(t, cl, events, "", key{"pk", types.ScalarAttributeTypeS}, key{"sk", types.ScalarAttributeTypeN})
	createTable(t, cl, kindsT, "", key{"id", types.ScalarAttributeTypeN})
	createTable(t, cl, orders, types.StreamViewTypeNewAndOldImages, key{"id", types.ScalarAttributeTypeN})
	createTable(t, cl, keysOnly, types.StreamViewTypeKeysOnly, key{"id", types.ScalarAttributeTypeS})
	for i := 1; i <= 120; i++ {
		put(t, cl, users, map[string]types.AttributeValue{"id": avI(i), "name": avS(fmt.Sprint("u", i)), "joined": avS(fmt.Sprintf("2020-01-%02dT00:00:00Z", i%28+1))})
	}
	for i := 1; i <= 90; i++ {
		put(t, cl, events, map[string]types.AttributeValue{"pk": avS(fmt.Sprint("p", i%7)), "sk": avI(i), "at": avI(1000 + i), "body": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"n": avI(i)}}})
	}
	put(t, cl, kindsT, kinds()...)
	for i := 1; i <= 70; i++ {
		put(t, cl, orders, map[string]types.AttributeValue{"id": avI(i), "total": avN(fmt.Sprintf("%d.25", i))})
	}
	for i := 1; i <= 30; i++ {
		put(t, cl, keysOnly, map[string]types.AttributeValue{"id": avS(fmt.Sprint("k", i)), "v": avI(i)})
	}
	var mu sync.Mutex
	next := map[string]int{orders: 71, events: 91, keysOnly: 31}
	tables := fmt.Sprintf(`, "tables": [%q, %q, %q, %q, %q], "scanLimit": 25`, users, events, kindsT, orders, keysOnly)
	cfg := func(endpoint, key string) []byte {
		return []byte("{" + connConfig(endpoint, key) + tables + extra + "}")
	}
	conformance.SourceSuite{
		Launch: conformance.Exec(conformance.Build(t, "../cmd/source-dynamodb")),
		Config: cfg(endpoint(), "openetl"),
		// DynamoDB Local takes any key of letters and digits; it refuses
		// others as AWS refuses a key it doesn't know.
		BadCredentials: cfg(endpoint(), "not-a-key!"),
		Unreachable:    cfg(closedPort, "openetl"),
		StopsAtUntil:   true,
		AddRecords: func(ctx context.Context, stream string) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			const k = 5
			for range k {
				i := next[stream]
				next[stream]++
				var it map[string]types.AttributeValue
				switch stream {
				case orders:
					it = map[string]types.AttributeValue{"id": avI(i), "total": avN("2.5")}
				case events:
					it = map[string]types.AttributeValue{"pk": avS("new"), "sk": avI(i), "at": avI(5000 + i), "body": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}}
				case keysOnly:
					it = map[string]types.AttributeValue{"id": avS(fmt.Sprint("k", i)), "v": avI(i)}
				default:
					return 0, fmt.Errorf("can't add records to %s", stream)
				}
				put(t, cl, stream, it)
			}
			return k, nil
		},
		Streams: []conformance.Stream{
			{Name: users, Mode: source.FullRefresh},
			{Name: events, Mode: source.Incremental, Cursor: "at"},
			{Name: kindsT, Mode: source.FullRefresh},
			{Name: orders, Mode: source.CDC},
			{Name: keysOnly, Mode: source.CDC},
		},
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	cl := testClient(t)
	cfg := func(endpoint, key string) []byte { return []byte("{" + connConfig(endpoint, key) + "}") }
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-dynamodb")),
		Config:         cfg(endpoint(), "openetl"),
		BadCredentials: cfg(endpoint(), "not-a-key!"),
		Unreachable:    cfg(closedPort, "openetl"),
		// An item's attributes take values of any type.
		Untyped: true,
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, cl, namespace+"."+stream)
		},
		Drop: func(ctx context.Context, namespace string) error {
			dropPrefix(ctx, cl, namespace+".")
			return nil
		},
	}.Test(t)
}
