package dynamodb

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestParseNumber(t *testing.T) {
	for in, want := range map[string]string{
		"0001.2300": "1.23", "-0": "0", "1e3": "1000", "1E-130": "0." + string(make0(129)) + "1",
		"12345678901234567890123456789012345678": "12345678901234567890123456789012345678",
		"-1.5":                                   "-1.5", "9.9E+2": "990", "0.000000001": "0.000000001", "100": "100",
	} {
		n, err := parseNumber(in)
		if err != nil || n.text != want {
			t.Errorf("parseNumber(%q) = %q, %v; want %q", in, n.text, err, want)
		}
	}
	for _, bad := range []string{"", "x", "1e", "1e5000", "--1", "1.2.3"} {
		if _, err := parseNumber(bad); err == nil {
			t.Errorf("parseNumber(%q) didn't fail", bad)
		}
	}
}

func make0(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}
	return b
}

func TestNumeric(t *testing.T) {
	for in, want := range map[string]bool{
		"0": true, "1E-130": true, "1E-131": false, "9.9999999999999999999999999999999999999E+125": true, "1E+126": false,
		"12345678901234567890123456789012345678": true, "123456789012345678901234567890123456789": false,
		"1.7976931348623157e+308": false, "5e-324": false, "3.4028235e+38": true, "NaN": false,
	} {
		if got := numeric(in); got != want {
			t.Errorf("numeric(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestFloat(t *testing.T) {
	for f, want := range map[float64]string{
		math.Inf(1): "S", math.MaxFloat64: "S", math.SmallestNonzeroFloat64: "S", 0.1: "N", -2.5: "N",
	} {
		if got := kind(float(f, 64)); got != want {
			t.Errorf("float(%v) is %s, want %s", f, got, want)
		}
	}
	if got := kind(float(math.NaN(), 64)); got != "S" {
		t.Errorf("float(NaN) is %s, want S", got)
	}
}

func TestCompareValues(t *testing.T) {
	for _, c := range []struct {
		a, b types.AttributeValue
		want int
	}{
		{num("10"), num("9"), 1}, {num("-10"), num("-9"), -1}, {num("1.50"), num("1.5"), 0},
		{num("12345678901234567890123456789012345678"), num("12345678901234567890123456789012345677"), 1},
		{str("b"), str("a"), 1}, {&types.AttributeValueMemberB{Value: []byte{1}}, &types.AttributeValueMemberB{Value: []byte{2}}, -1},
	} {
		got, ok := compareValues(c.a, c.b)
		if !ok || got != c.want {
			t.Errorf("compareValues(%v, %v) = %d, %v; want %d", c.a, c.b, got, ok, c.want)
		}
	}
	if _, ok := compareValues(num("1"), str("1")); ok {
		t.Error("a number and a string compared")
	}
}

// Every attribute type goes through typed JSON and back, and its plain
// JSON is valid JSON, control characters too.
func TestTypedJSON(t *testing.T) {
	it := map[string]types.AttributeValue{
		"s": str("a\x00\"\\\n✓"), "n": num("-1.5"), "b": &types.AttributeValueMemberB{Value: []byte{0, 255}},
		"bool": &types.AttributeValueMemberBOOL{Value: true}, "null": &types.AttributeValueMemberNULL{Value: true},
		"m":  &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"x": &types.AttributeValueMemberL{Value: []types.AttributeValue{num("1"), str("y")}}}},
		"ss": &types.AttributeValueMemberSS{Value: []string{"a", "b"}}, "ns": &types.AttributeValueMemberNS{Value: []string{"1", "2.5"}},
		"bs": &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}},
	}
	whole := &types.AttributeValueMemberM{Value: it}
	back, err := parseTyped(appendJSON(nil, whole, true))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(appendJSON(nil, back, true)), string(appendJSON(nil, whole, true)); got != want {
		t.Errorf("typed JSON round trip:\n got %s\nwant %s", got, want)
	}
	plain := appendJSON(nil, whole, false)
	if !json.Valid(plain) {
		t.Errorf("plain JSON isn't valid: %s", plain)
	}
}

func TestAttrTypes(t *testing.T) {
	for _, c := range []struct {
		vals []types.AttributeValue
		want arrow.DataType
	}{
		{[]types.AttributeValue{num("1"), num("-9223372036854775808")}, arrow.PrimitiveTypes.Int64},
		{[]types.AttributeValue{num("1"), num("9223372036854775808")}, decimalType},
		{[]types.AttributeValue{num("1.5"), &types.AttributeValueMemberNULL{Value: true}}, decimalType},
		{[]types.AttributeValue{num("1E-130")}, arrow.BinaryTypes.String},
		{[]types.AttributeValue{num("1"), str("x")}, arrow.BinaryTypes.String},
		{[]types.AttributeValue{&types.AttributeValueMemberSS{Value: []string{"a"}}}, jsonType},
		{[]types.AttributeValue{&types.AttributeValueMemberB{Value: []byte{1}}}, arrow.BinaryTypes.Binary},
		{[]types.AttributeValue{&types.AttributeValueMemberBOOL{}}, arrow.FixedWidthTypes.Boolean},
	} {
		var a attrTypes
		for _, v := range c.vals {
			a.add(v)
		}
		if got := a.arrowType(); !arrow.TypeEqual(got, c.want) {
			t.Errorf("%v: %v, want %v", c.vals, got, c.want)
		}
	}
}

func TestJSONAttribute(t *testing.T) {
	av, err := parseJSONValue(`{"a": [1, 12345678901234567890123456789, {"b": null}], "c": "✓", "d": true, "e": 1e400}`)
	if err != nil {
		t.Fatal(err)
	}
	m := av.(*types.AttributeValueMemberM).Value
	l := m["a"].(*types.AttributeValueMemberL).Value
	if l[1].(*types.AttributeValueMemberN).Value != "12345678901234567890123456789" {
		t.Errorf("a big number lost digits: %v", l[1])
	}
	if kind(m["e"]) != "S" {
		t.Errorf("1e400, past N's range, is %s, want S", kind(m["e"]))
	}
}

func TestSize(t *testing.T) {
	it := map[string]types.AttributeValue{"ab": str("xyz"), "m": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": num("12")}}}
	// ab: 2+3; m: 1 + 3 + 1 + (k: 1+2).
	if got := size(it); got != 13 {
		t.Errorf("size = %d, want 13", got)
	}
}
