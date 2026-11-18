package mysql

import (
	"bytes"
	encbin "encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

// kind is how a source column's values are read.
type kind int

const (
	kBool kind = iota + 1
	kInt16
	kInt32
	kInt64
	// BIGINT UNSIGNED, as text the engine parses as decimal(20, 0).
	kUint64
	kFloat32
	kFloat64
	kDecimal
	kString
	kEnum
	kSet
	kUUID
	kJSON
	kBinary
	kBit
	kDate
	kDatetime
	kTimestamp
	kTime
	kYear
	kGeometry
)

// kindOf maps a column's information_schema type to how it's read; 0 if it
// can't be.
func kindOf(c sourceColumn) kind {
	unsigned := strings.Contains(c.columnType, "unsigned")
	switch c.dataType {
	case "tinyint":
		if strings.HasPrefix(c.columnType, "tinyint(1)") && !unsigned {
			return kBool
		}
		return kInt16
	case "smallint":
		if unsigned {
			return kInt32
		}
		return kInt16
	case "mediumint":
		return kInt32
	case "int", "integer":
		if unsigned {
			return kInt64
		}
		return kInt32
	case "bigint":
		if unsigned {
			return kUint64
		}
		return kInt64
	case "float":
		return kFloat32
	case "double", "real":
		return kFloat64
	case "decimal", "numeric":
		return kDecimal
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "inet4", "inet6":
		if c.json {
			return kJSON
		}
		return kString
	case "enum":
		return kEnum
	case "set":
		return kSet
	case "uuid":
		return kUUID
	case "json":
		return kJSON
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return kBinary
	case "bit":
		if c.precision == 1 {
			return kBool
		}
		return kBit
	case "date":
		return kDate
	case "datetime":
		return kDatetime
	case "timestamp":
		return kTimestamp
	case "time":
		return kTime
	case "year":
		return kYear
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection":
		return kGeometry
	}
	return 0
}

// arrowTypes are the type a kind's values are sent as, and the type the
// stream declares, which the engine parses text to.
func arrowTypes(c sourceColumn) (sent, declared arrow.DataType, err error) {
	switch c.kind {
	case kBool:
		return arrow.FixedWidthTypes.Boolean, nil, nil
	case kInt16, kYear:
		return arrow.PrimitiveTypes.Int16, nil, nil
	case kInt32:
		return arrow.PrimitiveTypes.Int32, nil, nil
	case kInt64:
		return arrow.PrimitiveTypes.Int64, nil, nil
	case kUint64:
		return arrow.BinaryTypes.String, &arrow.Decimal128Type{Precision: 20, Scale: 0}, nil
	case kFloat32:
		return arrow.PrimitiveTypes.Float32, nil, nil
	case kFloat64:
		return arrow.PrimitiveTypes.Float64, nil, nil
	case kDecimal:
		return arrow.BinaryTypes.String, declaredDecimal(c.precision, c.scale), nil
	case kString, kEnum, kSet:
		return arrow.BinaryTypes.String, nil, nil
	case kUUID:
		return arrow.BinaryTypes.String, extensions.NewUUIDType(), nil
	case kJSON:
		t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		return t, nil, err
	case kBinary, kBit, kGeometry:
		return arrow.BinaryTypes.Binary, nil, nil
	case kDate:
		return arrow.FixedWidthTypes.Date32, nil, nil
	case kDatetime:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil, nil
	case kTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil, nil
	case kTime:
		return arrow.BinaryTypes.String, &arrow.Time64Type{Unit: arrow.Microsecond}, nil
	}
	return nil, nil, fmt.Errorf("column %q has type %s, which the source can't read", c.name, c.columnType)
}

// declaredDecimal is the decimal a DECIMAL(p, s) column is declared as: its
// own where Decimal128 holds it, otherwise decimal(38, s) with s at most 9.
func declaredDecimal(p, s int) *arrow.Decimal128Type {
	if p >= 1 && p <= 38 {
		return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(min(s, p))}
	}
	return &arrow.Decimal128Type{Precision: 38, Scale: int32(min(s, 9))}
}

// adder appends a column's value, as the binary protocol reads it (int64,
// float32, float64 or []byte) or binlogValue gives it, to its builder.
type adder func(v any) error

func newAdder(c sourceColumn, b array.Builder) adder {
	switch c.kind {
	case kBool:
		bb := b.(*array.BooleanBuilder)
		return func(v any) error {
			switch x := v.(type) {
			case int64:
				// MySQL's: any value but zero is true.
				bb.Append(x != 0)
			case []byte:
				bb.Append(len(bytes.Trim(x, "\x00")) > 0)
			default:
				return unexpected(v)
			}
			return nil
		}
	case kInt16, kYear:
		ib := b.(*array.Int16Builder)
		return intAdder(math.MinInt16, math.MaxInt16, func(x int64) { ib.Append(int16(x)) })
	case kInt32:
		ib := b.(*array.Int32Builder)
		return intAdder(math.MinInt32, math.MaxInt32, func(x int64) { ib.Append(int32(x)) })
	case kInt64:
		ib := b.(*array.Int64Builder)
		return intAdder(math.MinInt64, math.MaxInt64, ib.Append)
	case kFloat32:
		fb := b.(*array.Float32Builder)
		return func(v any) error {
			switch x := v.(type) {
			case float32:
				fb.Append(x)
			case float64:
				fb.Append(float32(x))
			default:
				return unexpected(v)
			}
			return nil
		}
	case kFloat64:
		fb := b.(*array.Float64Builder)
		return func(v any) error {
			switch x := v.(type) {
			case float64:
				fb.Append(x)
			case float32:
				fb.Append(float64(x))
			default:
				return unexpected(v)
			}
			return nil
		}
	case kUint64, kDecimal, kString, kEnum, kSet, kUUID, kTime:
		sb := b.(*array.StringBuilder)
		return func(v any) error {
			t, ok := text(v)
			if !ok {
				return unexpected(v)
			}
			if c.kind == kTime {
				// The binlog leaves out a fraction of zeros; SELECT doesn't.
				if i := bytes.IndexByte(t, '.'); i >= 0 && len(bytes.Trim(t[i+1:], "0")) == 0 {
					t = t[:i]
				}
			}
			sb.BinaryBuilder.Append(t)
			return nil
		}
	case kJSON:
		sb := b.(*array.ExtensionBuilder).Builder.(*array.StringBuilder)
		var buf bytes.Buffer
		return func(v any) error {
			t, ok := text(v)
			if !ok {
				return unexpected(v)
			}
			if len(t) == 0 {
				// What MySQL reads an empty JSON value as.
				t = []byte("null")
			}
			// Without the spaces MySQL's SELECT puts in and its binlog
			// doesn't.
			buf.Reset()
			if err := json.Compact(&buf, t); err != nil {
				return err
			}
			sb.BinaryBuilder.Append(buf.Bytes())
			return nil
		}
	case kBinary, kBit, kGeometry:
		bb := b.(*array.BinaryBuilder)
		return func(v any) error {
			x, ok := v.([]byte)
			if !ok {
				return unexpected(v)
			}
			bb.Append(x)
			return nil
		}
	case kDate:
		db := b.(*array.Date32Builder)
		return func(v any) error {
			t, ok, err := parseTemporal(v, "2006-01-02")
			switch {
			case err != nil:
				return err
			case !ok:
				db.AppendNull()
			default:
				db.Append(arrow.Date32FromTime(t))
			}
			return nil
		}
	case kDatetime, kTimestamp:
		tb := b.(*array.TimestampBuilder)
		return func(v any) error {
			t, ok, err := parseTemporal(v, "2006-01-02 15:04:05.999999")
			switch {
			case err != nil:
				return err
			case !ok:
				tb.AppendNull()
			default:
				tb.Append(arrow.Timestamp(t.UnixMicro()))
			}
			return nil
		}
	}
	return func(v any) error { return unexpected(v) }
}

func unexpected(v any) error { return fmt.Errorf("an unexpected %T", v) }

func intAdder(lo, hi int64, add func(int64)) adder {
	return func(v any) error {
		x, ok := v.(int64)
		if !ok {
			// BIGINT UNSIGNED past int64's range, as text.
			return fmt.Errorf("a value %v that the read's type can't hold; the read starts again to declare the column's new type", v)
		}
		if x < lo || x > hi {
			return fmt.Errorf("a value %d that the read's type can't hold; the read starts again to declare the column's new type", x)
		}
		add(x)
		return nil
	}
}

// text is a value as text: numbers as MySQL spells them.
func text(v any) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	case int64:
		return strconv.AppendInt(nil, x, 10), true
	}
	return nil, false
}

// parseTemporal reads a date or date-time as MySQL spells it. A zero date,
// or one with a zero month or day, which MySQL keeps where its SQL mode
// allows them, is no date: ok is false.
func parseTemporal(v any, layout string) (time.Time, bool, error) {
	t, isText := text(v)
	if !isText || len(t) < 10 {
		return time.Time{}, false, unexpected(v)
	}
	if string(t[5:7]) == "00" || string(t[8:10]) == "00" {
		return time.Time{}, false, nil
	}
	p, err := time.ParseInLocation(layout, string(t), time.UTC)
	if err != nil {
		return time.Time{}, false, err
	}
	return p, true, nil
}

// binlogColumn is what the binlog says of a column that binlogValue needs.
type binlogColumn struct {
	enum, set []string
}

// binlogValue is a value as the binlog decoder gives it, as the binary
// protocol reads the same value: integers as int64 (BIGINT UNSIGNED past
// int64 as decimal text), floats as they are, and the rest as bytes, text in
// UTF-8.
func binlogValue(c sourceColumn, bc binlogColumn, v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case int8:
		return enumOrInt(c, bc, int64(x))
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return enumOrInt(c, bc, x)
	case int:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return strconv.AppendUint(nil, x, 10), nil
		}
		return int64(x), nil
	case float32, float64:
		return x, nil
	case string:
		return textValue(c, []byte(x))
	case []byte:
		return textValue(c, x)
	}
	return nil, fmt.Errorf("an unexpected %T in the binlog", v)
}

// enumOrInt is an ENUM's index or a SET's bits as their members, and a BIT's
// bits as bytes, as SELECT reads them.
func enumOrInt(c sourceColumn, bc binlogColumn, x int64) (any, error) {
	switch c.kind {
	case kEnum:
		switch {
		case x == 0:
			// The empty string MySQL keeps for a value an ENUM doesn't have.
			return []byte{}, nil
		case int(x) > len(bc.enum):
			return nil, fmt.Errorf("ENUM index %d past the binlog's %d values; the binlog needs binlog_row_metadata = FULL", x, len(bc.enum))
		}
		return textValue(c, []byte(bc.enum[x-1]))
	case kSet:
		var members []string
		for i, m := range bc.set {
			if x&(1<<i) != 0 {
				members = append(members, m)
			}
		}
		return textValue(c, []byte(strings.Join(members, ",")))
	case kBit:
		var b [8]byte
		encbin.BigEndian.PutUint64(b[:], uint64(x))
		return b[8-(c.precision+7)/8:], nil
	}
	return x, nil
}

// textValue converts text in the column's character set to UTF-8, and pads a
// BINARY(n) value, which the binlog keeps without its trailing zero bytes.
func textValue(c sourceColumn, b []byte) (any, error) {
	// MariaDB's UUID, INET4 and INET6 are bytes in the binlog, without
	// their trailing zero bytes.
	switch c.dataType {
	case "uuid", "inet6":
		var a [16]byte
		copy(a[:], b)
		if c.dataType == "inet6" {
			return []byte(netip.AddrFrom16(a).String()), nil
		}
		h := hex.EncodeToString(a[:])
		return []byte(h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]), nil
	case "inet4":
		var a [4]byte
		copy(a[:], b)
		return []byte(netip.AddrFrom4(a).String()), nil
	}
	switch c.kind {
	case kBinary:
		if c.dataType == "binary" && len(b) < c.octets {
			b = append(bytes.Clone(b), make([]byte, c.octets-len(b))...)
		}
		return b, nil
	case kString, kEnum, kSet, kJSON:
		if c.decoder == nil {
			return b, nil
		}
		out, err := c.decoder.Bytes(b)
		if err != nil {
			return nil, fmt.Errorf("column %q: %s text: %w", c.name, c.charset, err)
		}
		return out, nil
	}
	return b, nil
}

// charsetDecoder decodes a MySQL character set to UTF-8: nil for those that
// are UTF-8 already.
func charsetDecoder(charset string) (*encoding.Decoder, error) {
	switch charset {
	case "", "utf8mb4", "utf8mb3", "utf8", "ascii", "binary":
		return nil, nil
	case "latin1":
		// MySQL's latin1 is Windows-1252.
		return charmap.Windows1252.NewDecoder(), nil
	case "ucs2", "utf16":
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder(), nil
	case "utf16le":
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder(), nil
	case "utf32":
		return utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM).NewDecoder(), nil
	}
	names := map[string]string{
		"latin2": "iso-8859-2", "latin5": "iso-8859-9", "latin7": "iso-8859-13", "greek": "iso-8859-7", "hebrew": "iso-8859-8",
		"cp1250": "windows-1250", "cp1251": "windows-1251", "cp1256": "windows-1256", "cp1257": "windows-1257", "cp866": "ibm866",
		"koi8r": "koi8-r", "koi8u": "koi8-u", "macroman": "macintosh", "sjis": "shift_jis", "cp932": "shift_jis",
		"ujis": "euc-jp", "eucjpms": "euc-jp", "euckr": "euc-kr", "gbk": "gbk", "gb2312": "gbk", "gb18030": "gb18030", "big5": "big5",
		"tis620": "windows-874",
	}
	if e, err := htmlindex.Get(names[charset]); err == nil {
		return e.NewDecoder(), nil
	}
	return nil, errors.New("character set " + charset)
}
