package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// A source sends every attribute as text, which the engine parses into the
// type discovery declared: a read knows a run's columns but not their
// types, so text is what parses into any of them. Each value has one
// spelling: strings as they are, numbers in plain decimal, binaries in
// base64, booleans as JSON spells them, and maps, lists and sets as JSON.

// kind is an attribute value's DynamoDB type: S, N, B, BOOL, NULL, M, L,
// SS, NS or BS.
func kind(av types.AttributeValue) string {
	switch av.(type) {
	case *types.AttributeValueMemberS:
		return "S"
	case *types.AttributeValueMemberN:
		return "N"
	case *types.AttributeValueMemberB:
		return "B"
	case *types.AttributeValueMemberBOOL:
		return "BOOL"
	case *types.AttributeValueMemberNULL:
		return "NULL"
	case *types.AttributeValueMemberM:
		return "M"
	case *types.AttributeValueMemberL:
		return "L"
	case *types.AttributeValueMemberSS:
		return "SS"
	case *types.AttributeValueMemberNS:
		return "NS"
	case *types.AttributeValueMemberBS:
		return "BS"
	}
	return ""
}

// number is a DynamoDB number in plain decimal, without exponent, leading
// zeros or trailing zeros after its point: "-12.5", "1000", "0.001". A
// number holds 38 significant digits, from 1e-130 to below 1e126.
type number struct {
	text string
	// Digits before and after the point.
	intDigits, scale int
}

// parseNumber reads N's text, which DynamoDB may spell with an exponent.
func parseNumber(s string) (number, error) {
	bad := func() (number, error) { return number{}, fmt.Errorf("dynamodb: %q isn't a number", s) }
	neg := strings.HasPrefix(s, "-")
	m := strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	if neg && strings.HasPrefix(s, "-+") {
		return bad()
	}
	exp := 0
	if i := strings.IndexAny(m, "eE"); i >= 0 {
		e, err := strconv.Atoi(m[i+1:])
		if err != nil || e > 1000 || e < -1000 {
			return bad()
		}
		m, exp = m[:i], e
	}
	whole, frac, _ := strings.Cut(m, ".")
	digits := whole + frac
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return bad()
	}
	// The value is digits × 10^(exp - len(frac)).
	exp -= len(frac)
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return number{text: "0", intDigits: 1}, nil
	}
	for strings.HasSuffix(digits, "0") {
		digits, exp = digits[:len(digits)-1], exp+1
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	n := number{}
	switch {
	case exp >= 0:
		b.WriteString(digits)
		b.WriteString(strings.Repeat("0", exp))
		n.intDigits = len(digits) + exp
	case -exp >= len(digits):
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -exp-len(digits)))
		b.WriteString(digits)
		n.intDigits, n.scale = 0, -exp
	default:
		b.WriteString(digits[:len(digits)+exp])
		b.WriteByte('.')
		b.WriteString(digits[len(digits)+exp:])
		n.intDigits, n.scale = len(digits)+exp, -exp
	}
	n.text = b.String()
	return n, nil
}

// int64Fits says an integer's text is an int64's.
func int64Fits(n number) bool {
	if n.scale > 0 {
		return false
	}
	_, err := strconv.ParseInt(n.text, 10, 64)
	return err == nil
}

// text is v's spelling; false for NULL.
func text(av types.AttributeValue) (string, bool) {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, true
	case *types.AttributeValueMemberN:
		if n, err := parseNumber(v.Value); err == nil {
			return n.text, true
		}
		return v.Value, true
	case *types.AttributeValueMemberB:
		return base64.StdEncoding.EncodeToString(v.Value), true
	case *types.AttributeValueMemberBOOL:
		return strconv.FormatBool(v.Value), true
	case *types.AttributeValueMemberNULL, nil:
		return "", false
	}
	return string(appendJSON(nil, av, false)), true
}

// appendJSON appends av as JSON. Plain JSON spells maps as objects, lists
// and sets as arrays, numbers as JSON numbers in plain decimal, binaries in
// base64 and NULL as null. Typed JSON is DynamoDB's own, which keeps every
// type: {"S":"x"}, {"N":"5"}, {"M":{...}}.
func appendJSON(b []byte, av types.AttributeValue, typed bool) []byte {
	if typed {
		k := kind(av)
		b = append(b, `{"`...)
		b = append(b, k...)
		b = append(b, `":`...)
		b = appendValue(b, av, true)
		return append(b, '}')
	}
	return appendValue(b, av, false)
}

func appendValue(b []byte, av types.AttributeValue, typed bool) []byte {
	num := func(s string) []byte {
		if typed {
			return appendString(b, s)
		}
		if n, err := parseNumber(s); err == nil {
			s = n.text
		}
		return append(b, s...)
	}
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return appendString(b, v.Value)
	case *types.AttributeValueMemberN:
		return num(v.Value)
	case *types.AttributeValueMemberB:
		return appendString(b, base64.StdEncoding.EncodeToString(v.Value))
	case *types.AttributeValueMemberBOOL:
		return strconv.AppendBool(b, v.Value)
	case *types.AttributeValueMemberNULL:
		if typed {
			return append(b, "true"...)
		}
		return append(b, "null"...)
	case *types.AttributeValueMemberM:
		return appendMap(b, v.Value, typed)
	case *types.AttributeValueMemberL:
		b = append(b, '[')
		for i, e := range v.Value {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendJSON(b, e, typed)
		}
		return append(b, ']')
	case *types.AttributeValueMemberSS:
		b = append(b, '[')
		for i, e := range v.Value {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, e)
		}
		return append(b, ']')
	case *types.AttributeValueMemberNS:
		b = append(b, '[')
		for i, e := range v.Value {
			if i > 0 {
				b = append(b, ',')
			}
			b = num(e)
		}
		return append(b, ']')
	case *types.AttributeValueMemberBS:
		b = append(b, '[')
		for i, e := range v.Value {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, base64.StdEncoding.EncodeToString(e))
		}
		return append(b, ']')
	}
	return append(b, "null"...)
}

// appendMap appends an item or a map as a JSON object, its keys in order.
func appendMap(b []byte, m map[string]types.AttributeValue, typed bool) []byte {
	b = append(b, '{')
	for i, k := range slices.Sorted(maps.Keys(m)) {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendString(b, k)
		b = append(b, ':')
		b = appendJSON(b, m[k], typed)
	}
	return append(b, '}')
}

func appendString(b []byte, s string) []byte {
	j, _ := json.Marshal(s)
	return append(b, j...)
}

var (
	decimalType = &arrow.Decimal128Type{Precision: 38, Scale: 9}
	jsonType    = func() arrow.DataType {
		t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			panic(err)
		}
		return t
	}()
)

// attrTypes collects what discovery finds of one attribute's values: their
// DynamoDB types, and for numbers, how many digits they have either side of
// the point.
type attrTypes struct {
	kinds map[string]bool
	// Every number is an int64.
	ints bool
	// The most digits any number has before and after its point.
	intDigits, scale int
	// A number has more digits than decimal(38, s) holds for any s.
	wide bool
}

func (a *attrTypes) add(av types.AttributeValue) {
	if a.kinds == nil {
		a.kinds, a.ints = map[string]bool{}, true
	}
	k := kind(av)
	a.kinds[k] = true
	if k != "N" {
		return
	}
	n, err := parseNumber(av.(*types.AttributeValueMemberN).Value)
	if err != nil {
		a.wide = true
		return
	}
	a.ints = a.ints && int64Fits(n)
	a.intDigits, a.scale = max(a.intDigits, n.intDigits), max(a.scale, n.scale)
	a.wide = a.wide || a.intDigits+a.scale > 38
}

// arrowType is the type an attribute is declared as: numbers are int64
// where every one is, else decimal(38, 9), or decimal(38, s) for the most
// digits after the point that fit, else text; strings are text, binaries
// binary and booleans boolean; maps, lists and sets JSON; and an attribute
// whose values have more than one type, or only NULL, text.
func (a attrTypes) arrowType() arrow.DataType {
	kinds := slices.DeleteFunc(slices.Collect(maps.Keys(a.kinds)), func(k string) bool { return k == "NULL" })
	json := 0
	for _, k := range kinds {
		switch k {
		case "M", "L", "SS", "NS", "BS":
			json++
		}
	}
	if json > 0 && json == len(kinds) {
		return jsonType
	}
	if len(kinds) != 1 {
		return arrow.BinaryTypes.String
	}
	switch kinds[0] {
	case "N":
		return a.numberType()
	case "B":
		return arrow.BinaryTypes.Binary
	case "BOOL":
		return arrow.FixedWidthTypes.Boolean
	}
	return arrow.BinaryTypes.String
}

func (a attrTypes) numberType() arrow.DataType {
	switch {
	case a.ints:
		return arrow.PrimitiveTypes.Int64
	case a.wide:
		return arrow.BinaryTypes.String
	case a.intDigits <= 29 && a.scale <= 9:
		return decimalType
	}
	return &arrow.Decimal128Type{Precision: 38, Scale: int32(38 - a.intDigits)}
}

// keyType is a key attribute's type, from its definition: a string, a
// binary, or a number as arrowType has it, which every number of the
// table's keys fits, as discovery found them; a table with no items has
// int64.
func keyType(def string, a attrTypes) arrow.DataType {
	switch def {
	case "S":
		return arrow.BinaryTypes.String
	case "B":
		return arrow.BinaryTypes.Binary
	}
	if a.kinds == nil {
		return arrow.PrimitiveTypes.Int64
	}
	return a.numberType()
}

// compareValues orders two cursor values of one type: numbers by value,
// strings and binaries by their bytes. ok is false for values of different
// types, or of a type without an order.
func compareValues(a, b types.AttributeValue) (int, bool) {
	switch x := a.(type) {
	case *types.AttributeValueMemberN:
		y, ok := b.(*types.AttributeValueMemberN)
		if !ok {
			return 0, false
		}
		return compareNumbers(x.Value, y.Value), true
	case *types.AttributeValueMemberS:
		y, ok := b.(*types.AttributeValueMemberS)
		if !ok {
			return 0, false
		}
		return strings.Compare(x.Value, y.Value), true
	case *types.AttributeValueMemberB:
		y, ok := b.(*types.AttributeValueMemberB)
		if !ok {
			return 0, false
		}
		return strings.Compare(string(x.Value), string(y.Value)), true
	}
	return 0, false
}

// compareNumbers orders two numbers' texts by value.
func compareNumbers(a, b string) int {
	x, errA := parseNumber(a)
	y, errB := parseNumber(b)
	if errA != nil || errB != nil {
		return strings.Compare(a, b)
	}
	xneg, yneg := strings.HasPrefix(x.text, "-"), strings.HasPrefix(y.text, "-")
	switch {
	case xneg && !yneg:
		return -1
	case !xneg && yneg:
		return 1
	}
	c := compareAbs(strings.TrimPrefix(x.text, "-"), x.intDigits, strings.TrimPrefix(y.text, "-"), y.intDigits)
	if xneg {
		return -c
	}
	return c
}

func compareAbs(a string, ai int, b string, bi int) int {
	az, bz := a == "0", b == "0"
	switch {
	case az && bz:
		return 0
	case az:
		return -1
	case bz:
		return 1
	}
	// Compare by the position of the first significant digit, then digit by
	// digit.
	lead := func(s string, intDigits int) (int, string) {
		if intDigits > 0 {
			return intDigits, strings.Replace(s, ".", "", 1)
		}
		frac := strings.TrimPrefix(s, "0.")
		t := strings.TrimLeft(frac, "0")
		return -(len(frac) - len(t)), t
	}
	pa, da := lead(a, ai)
	pb, db := lead(b, bi)
	if pa != pb {
		if pa > pb {
			return 1
		}
		return -1
	}
	return strings.Compare(da, db)
}

// floatText spells a float as the engine parses it.
func floatText(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
