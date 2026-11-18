package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// dcol is a target column of the destination. typ is spelled as
// catalogType spells a column's type.
type dcol struct {
	name string
	typ  string
	// The bind of row i, which is not null, as go-ora takes it, or an error.
	value func(arr arrow.Array, i int) any
	// What SQL makes of the bind: a format with one %s, the bind; empty for
	// the bind as it is.
	conv string
	// A merge key's: a null or empty value is refused, as Oracle stores an
	// empty value as null, which no merge matches.
	key bool
	// The most bytes a value takes; 0 for no limit.
	limit int
}

const (
	colID      = "_openetl_id"
	colOp      = "_openetl_op"
	colSeq     = "_openetl_seq"
	colDeleted = "_openetl_deleted"
	// A changelog's: the columns an update didn't send, as a JSON array. In
	// a merge's target, those it never received.
	colUnchanged = "_openetl_unchanged"
	// A row's held columns, as a JSON array; in the target, a JSON object
	// from each to the _openetl_id of the version whose value is held.
	colHeld   = destination.HeldColumn
	colSynced = "_openetl_synced_at"
	// Each staging row's place in it, as it was staged.
	colOrd = "_openetl_ord"

	metaExtension = "ARROW:extension:name"
	// The engine names each stream's table in its schema's metadata, and
	// renamed columns' source names: a JSON object from destination name to
	// source name.
	metaTable       = "openetl.table"
	metaSourceNames = "openetl.source_names"

	// Oracle's limits: a VARCHAR2's bytes without MAX_STRING_SIZE =
	// EXTENDED, a RAW's, and a table's columns.
	maxVarchar = 4000
	maxRaw     = 2000
	maxCols    = 1000
	// A target's columns, of maxCols: its staging tables have
	// _openetl_ord besides.
	maxTargetCols = maxCols - 1
)

var metaColumns = []string{colID, colOp, colSeq, colSynced, colDeleted, colUnchanged, colHeld}

// destColumns maps a batch's fields to columns. Text and binary columns in
// key are VARCHAR2 and RAW, sized to fit, together, in an index. boolean:
// the server has BOOLEAN.
func destColumns(schema *arrow.Schema, key []string, boolean bool) ([]dcol, error) {
	sized := 0
	for _, k := range key {
		if i := schema.FieldIndices(k); len(i) > 0 {
			if c, err := destColumn(schema.Field(i[0]), boolean); err == nil && (c.typ == "CLOB" || c.typ == "BLOB") {
				sized++
			}
		}
	}
	cols := make([]dcol, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		if err := validName("column", f.Name); err != nil {
			return nil, err
		}
		c, err := destColumn(f, boolean)
		if err != nil {
			return nil, err
		}
		if slices.Contains(key, f.Name) {
			c.key = true
			switch c.typ {
			case "CLOB":
				c.limit = maxVarchar / sized
				c.typ = fmt.Sprintf("VARCHAR2(%d BYTE)", c.limit)
			case "BLOB":
				c.limit = min(maxRaw, maxVarchar/sized)
				c.typ = fmt.Sprintf("RAW(%d)", c.limit)
			}
		}
		cols = append(cols, c)
	}
	return cols, nil
}

// Formats of the text that timestamps and dates are bound as.
const (
	tsLayout = "2006-01-02 15:04:05.000000000"
	tsFormat = "'SYYYY-MM-DD HH24:MI:SS.FF9'"
)

func destColumn(f arrow.Field, boolean bool) (dcol, error) {
	c := dcol{name: f.Name}
	dt := f.Type
	ext := ""
	if et, ok := dt.(arrow.ExtensionType); ok {
		ext, dt = et.ExtensionName(), et.StorageType()
	} else if v, ok := f.Metadata.GetValue(metaExtension); ok {
		ext = v
	}
	switch {
	case ext == "arrow.json" && textual(dt):
		c.typ, c.value = "CLOB", textValue
		return c, nil
	case ext == "arrow.uuid" && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.typ, c.limit = "VARCHAR2(36 BYTE)", 36
		c.value = func(a arrow.Array, i int) any {
			u, err := uuid.FromBytes(plain(a).(*array.FixedSizeBinary).Value(i))
			if err != nil {
				return err
			}
			return u.String()
		}
		return c, nil
	case f.Name == colOp && textual(dt):
		// Compared in applies, which a CLOB can't be.
		c.typ, c.value, c.limit = "VARCHAR2(32 BYTE)", textValue, 32
		return c, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		c.typ = "CLOB"
	case *arrow.BooleanType:
		if boolean {
			c.typ = "BOOLEAN"
			c.value = func(a arrow.Array, i int) any { return plain(a).(*array.Boolean).Value(i) }
		} else {
			c.typ = "NUMBER(1)"
			c.value = func(a arrow.Array, i int) any {
				if plain(a).(*array.Boolean).Value(i) {
					return int64(1)
				}
				return int64(0)
			}
		}
	case *arrow.Int8Type:
		c.typ = "NUMBER(3)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Int8).Value(i)) }
	case *arrow.Int16Type:
		c.typ = "NUMBER(5)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Int16).Value(i)) }
	case *arrow.Int32Type:
		c.typ = "NUMBER(10)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Int32).Value(i)) }
	case *arrow.Int64Type:
		c.typ = "NUMBER(19)"
		c.value = func(a arrow.Array, i int) any { return plain(a).(*array.Int64).Value(i) }
	case *arrow.Uint8Type:
		c.typ = "NUMBER(3)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Uint8).Value(i)) }
	case *arrow.Uint16Type:
		c.typ = "NUMBER(5)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Uint16).Value(i)) }
	case *arrow.Uint32Type:
		c.typ = "NUMBER(10)"
		c.value = func(a arrow.Array, i int) any { return int64(plain(a).(*array.Uint32).Value(i)) }
	case *arrow.Uint64Type:
		c.typ, c.conv = "NUMBER(20)", "TO_NUMBER(%s)"
		c.value = func(a arrow.Array, i int) any { return strconv.FormatUint(plain(a).(*array.Uint64).Value(i), 10) }
	case *arrow.Float32Type:
		// As text, which keeps NaN and the infinities BINARY_FLOAT has.
		c.typ, c.conv = "BINARY_FLOAT", "TO_BINARY_FLOAT(%s)"
		c.value = func(a arrow.Array, i int) any {
			return strconv.FormatFloat(float64(plain(a).(*array.Float32).Value(i)), 'g', -1, 32)
		}
	case *arrow.Float64Type:
		c.typ, c.conv = "BINARY_DOUBLE", "TO_BINARY_DOUBLE(%s)"
		c.value = func(a arrow.Array, i int) any {
			return strconv.FormatFloat(plain(a).(*array.Float64).Value(i), 'g', -1, 64)
		}
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.typ, c.value = "CLOB", textValue
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		c.typ = "BLOB"
		c.value = func(a arrow.Array, i int) any { return plain(a).(interface{ Value(int) []byte }).Value(i) }
	case *arrow.Date32Type:
		c.typ, c.conv = "DATE", "TO_DATE(%s, 'SYYYY-MM-DD')"
		c.value = func(a arrow.Array, i int) any {
			return inYears(plain(a).(*array.Date32).Value(i).ToTime(), "2006-01-02")
		}
	case *arrow.Time64Type:
		// A time of day is the interval since midnight.
		c.typ, c.conv = "INTERVAL DAY(0) TO SECOND(6)", "TO_DSINTERVAL(%s)"
		if t.Unit == arrow.Nanosecond {
			c.typ = "INTERVAL DAY(0) TO SECOND(9)"
		}
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any {
			d := time.Duration(plain(a).(*array.Time64).Value(i)) * unit.Multiplier()
			if d < 0 || d >= 24*time.Hour {
				return fmt.Errorf("%s isn't a time of day", d)
			}
			return fmt.Sprintf("0 %02d:%02d:%02d.%09d", int64(d/time.Hour), int64(d/time.Minute%60), int64(d/time.Second%60), int64(d%time.Second))
		}
	case *arrow.TimestampType:
		// In UTC, with a zone or without one, as the field has.
		p := 6
		if t.Unit == arrow.Nanosecond {
			p = 9
		}
		c.typ, c.conv = fmt.Sprintf("TIMESTAMP(%d)", p), "TO_TIMESTAMP(%s, "+tsFormat+")"
		if t.TimeZone != "" {
			c.typ, c.conv = c.typ+" WITH TIME ZONE", "FROM_TZ(TO_TIMESTAMP(%s, "+tsFormat+"), '+00:00')"
		}
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any {
			return inYears(plain(a).(*array.Timestamp).Value(i).ToTime(unit), tsLayout)
		}
	case *arrow.Decimal128Type:
		c.typ, c.conv = decimalType(t.Precision, t.Scale)
		c.value = func(a arrow.Array, i int) any { return plain(a).(*array.Decimal128).Value(i).ToString(t.Scale) }
	case *arrow.Decimal256Type:
		c.typ, c.conv = decimalType(t.Precision, t.Scale)
		c.value = func(a arrow.Array, i int) any { return plain(a).(*array.Decimal256).Value(i).ToString(t.Scale) }
	case *arrow.MonthDayNanoIntervalType:
		// Oracle's intervals hold months or days, not both: JSON text.
		c.typ, c.limit = "VARCHAR2(100 BYTE)", 100
		c.value = func(a arrow.Array, i int) any {
			v := plain(a).(*array.MonthDayNanoInterval).Value(i)
			return fmt.Sprintf(`{"months": %d, "days": %d, "nanoseconds": %d}`, v.Months, v.Days, v.Nanoseconds)
		}
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		c.typ = "CLOB"
		c.value = func(a arrow.Array, i int) any {
			var b strings.Builder
			enc := json.NewEncoder(&b)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(plain(a).GetOneForMarshal(i)); err != nil {
				return err
			}
			return strings.TrimSuffix(b.String(), "\n")
		}
	default:
		return c, fmt.Errorf("oracle: column %q has unsupported Arrow type %s", f.Name, f.Type)
	}
	return c, nil
}

// decimalType is NUMBER where Oracle's 38 digits hold the type, and text
// where they don't.
func decimalType(p, s int32) (string, string) {
	if p <= 38 && s >= 0 {
		if s == 0 {
			return fmt.Sprintf("NUMBER(%d)", p), "TO_NUMBER(%s)"
		}
		return fmt.Sprintf("NUMBER(%d,%d)", p, s), "TO_NUMBER(%s)"
	}
	return fmt.Sprintf("VARCHAR2(%d BYTE)", min(int(p)+abs(int(s))+3, maxVarchar)), ""
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// inYears formats t, or is an error if Oracle's DATE and TIMESTAMP can't
// hold it.
func inYears(t time.Time, layout string) any {
	if t.Year() < 1 || t.Year() > 9999 {
		return fmt.Errorf("%s is outside the years 1 to 9999 that the destination writes", t.Format(time.RFC3339Nano))
	}
	return t.Format(layout)
}

var errEmptyKey = errors.New("it's empty, which Oracle stores as null, and a merge can't match a null key")

// bind is the value of row i of a, which isn't null, checked against c's
// limit and, for a key, refused if empty.
func (c dcol) bind(a arrow.Array, i int) (any, error) {
	v := c.value(a, i)
	n := 0
	switch x := v.(type) {
	case error:
		return nil, x
	case string:
		n = len(x)
	case []byte:
		n = len(x)
	default:
		return v, nil
	}
	switch {
	case c.key && n == 0:
		return nil, errEmptyKey
	case c.limit > 0 && n > c.limit:
		return nil, fmt.Errorf("its value is %d bytes, and its column, %s, holds %d", n, c.typ, c.limit)
	}
	return v, nil
}

func plain(a arrow.Array) arrow.Array {
	if e, ok := a.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return a
}

func textual(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return true
	}
	return false
}

func textValue(a arrow.Array, i int) any { return plain(a).(interface{ Value(int) string }).Value(i) }

// targetTable is the table the engine named, or for batches that didn't
// pass through the engine, the stream's name as it is.
func targetTable(schema *arrow.Schema, stream string) string {
	if t, ok := schema.Metadata().GetValue(metaTable); ok {
		return t
	}
	return stream
}

// catalogType spells a column's type from ALL_TAB_COLS as destColumn does.
func catalogType(dataType string, precision, scale *int64, length, charLength int64, charUsed string) string {
	switch dataType {
	case "NUMBER":
		switch {
		case precision == nil && (scale == nil || *scale != 0):
			return "NUMBER"
		case precision == nil:
			return "NUMBER(38)"
		case scale == nil || *scale == 0:
			return fmt.Sprintf("NUMBER(%d)", *precision)
		}
		return fmt.Sprintf("NUMBER(%d,%d)", *precision, *scale)
	case "FLOAT":
		if precision != nil {
			return fmt.Sprintf("FLOAT(%d)", *precision)
		}
	case "VARCHAR2", "CHAR":
		if charUsed == "C" {
			return fmt.Sprintf("%s(%d CHAR)", dataType, charLength)
		}
		return fmt.Sprintf("%s(%d BYTE)", dataType, length)
	case "NVARCHAR2", "NCHAR":
		return fmt.Sprintf("%s(%d)", dataType, charLength)
	case "RAW":
		return fmt.Sprintf("RAW(%d)", length)
	}
	return dataType
}

var (
	numberType   = regexp.MustCompile(`^NUMBER\((\d+)(?:,(-?\d+))?\)$`)
	sizedType    = regexp.MustCompile(`^(VARCHAR2|RAW)\((\d+)( BYTE)?\)$`)
	instantType  = regexp.MustCompile(`^TIMESTAMP\((\d)\)( WITH TIME ZONE| WITH LOCAL TIME ZONE)?$`)
	daySecondTyp = regexp.MustCompile(`^INTERVAL DAY\((\d)\) TO SECOND\((\d)\)$`)
)

// number is a NUMBER type's precision and scale.
func number(t string) (p, s int, ok bool) {
	m := numberType.FindStringSubmatch(t)
	if m == nil {
		return 0, 0, false
	}
	p, _ = strconv.Atoi(m[1])
	s, _ = strconv.Atoi(m[2])
	return p, s, true
}

func binaryType(t string) bool { return t == "BLOB" || strings.HasPrefix(t, "RAW(") }

// fits reports whether every value of a column of type from has one in a
// column of type to that a reader takes as the same: integers in
// BINARY_DOUBLE count, as they do for the engine.
func fits(from, to string) bool {
	if from == to {
		return true
	}
	switch to {
	case "CLOB":
		_, _, num := number(from)
		return num || from == "NUMBER" || strings.HasPrefix(from, "VARCHAR2(") || from == "BINARY_FLOAT" || from == "BINARY_DOUBLE"
	case "BLOB":
		return strings.HasPrefix(from, "RAW(")
	case "NUMBER":
		_, _, ok := number(from)
		return ok || from == "BOOLEAN"
	case "BINARY_DOUBLE":
		_, s, ok := number(from)
		return from == "BINARY_FLOAT" || ok && s == 0
	case "BINARY_FLOAT":
		p, s, ok := number(from)
		return ok && s == 0 && p <= 5
	}
	p2, s2, ok := number(to)
	if ok {
		if from == "BOOLEAN" {
			return p2-s2 >= 1
		}
		p1, s1, ok := number(from)
		return ok && s2 >= s1 && p2-s2 >= p1-s1
	}
	if m2 := sizedType.FindStringSubmatch(to); m2 != nil {
		m1 := sizedType.FindStringSubmatch(from)
		return m1 != nil && m1[1] == m2[1] && m1[3] == m2[3] && atoi(m1[2]) <= atoi(m2[2])
	}
	if m2 := instantType.FindStringSubmatch(to); m2 != nil {
		if from == "DATE" {
			return m2[2] == ""
		}
		m1 := instantType.FindStringSubmatch(from)
		return m1 != nil && m1[2] == m2[2] && m1[1] <= m2[1]
	}
	if m2 := daySecondTyp.FindStringSubmatch(to); m2 != nil {
		m1 := daySecondTyp.FindStringSubmatch(from)
		return m1 != nil && m1[1] <= m2[1] && m1[2] <= m2[2]
	}
	return false
}

// alterable reports whether ALTER TABLE MODIFY changes a column with
// values from type from to type to, which fits it: Oracle makes a NUMBER,
// VARCHAR2, RAW, TIMESTAMP or INTERVAL longer or more precise, and a DATE a
// TIMESTAMP, and changes no other type of a column that has values.
func alterable(from, to string) bool {
	if from == to || !fits(from, to) {
		return false
	}
	if _, _, ok := number(from); ok {
		_, _, ok := number(to)
		return ok || to == "NUMBER"
	}
	// A VARCHAR2 or RAW doesn't become a LOB in place.
	return sizedType.MatchString(from) && sizedType.MatchString(to) || instantType.MatchString(to) || daySecondTyp.MatchString(from)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// convert is expr, of type from, as a value of type to, which it fits:
// Oracle doesn't convert every such pair implicitly, nor mix them in CASE.
func convert(expr, from, to string) string {
	if from == to {
		return expr
	}
	switch {
	case to == "CLOB":
		return "TO_CLOB(" + expr + ")"
	case to == "BLOB":
		return "TO_BLOB(" + expr + ")"
	case to == "BINARY_DOUBLE", to == "BINARY_FLOAT":
		return "TO_" + to + "(" + expr + ")"
	case from == "BOOLEAN":
		return "CASE " + expr + " WHEN TRUE THEN 1 WHEN FALSE THEN 0 END"
	case instantType.MatchString(to):
		return "CAST(" + expr + " AS " + to + ")"
	}
	return expr
}
