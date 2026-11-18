package oracle

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// kind is how a source column's values are read.
type kind int

const (
	kInt16 kind = iota + 1
	kInt32
	kInt64
	// NUMBER that isn't an integer of at most 18 digits: sent as text, which
	// the engine parses as the declared decimal.
	kDecimal
	// BINARY_FLOAT.
	kFloat32
	// BINARY_DOUBLE, and FLOAT, whose binary precision float64 can round.
	kFloat64
	kBool
	kString
	kJSON
	kBinary
	// DATE and TIMESTAMP(0) to (6), in microseconds without a time zone.
	kTimestamp
	// TIMESTAMP(7) to (9): text with all its digits, which the engine parses
	// into microseconds, or nanoseconds for a stream that keeps them.
	kTimestampNS
	// TIMESTAMP WITH (LOCAL) TIME ZONE, (0) to (6): an instant in UTC.
	kZoned
	// (7) to (9): text, as kTimestampNS.
	kZonedNS
	kYearMonth
	kDaySecond
)

// column is a table's column as the source reads it.
type column struct {
	name string
	// ALL_TAB_COLS's DATA_TYPE, such as NUMBER or TIMESTAMP(6) WITH TIME ZONE.
	dataType string
	// NUMBER's precision and scale; -1 where the column has none.
	precision, scale int
	notNull          bool
	kind             kind
	// SEGMENT_COLUMN_ID: where the column's value is in a row's redo.
	seg int
}

type table struct {
	name string
	cols []column
	// Indexes into cols, in key order: the primary key's, or else a unique
	// index's on NOT NULL columns.
	key []int
	// Why LogMiner can't read the table's changes; empty if it can.
	noCDC string
}

var (
	timestampType = regexp.MustCompile(`^TIMESTAMP\((\d)\)( WITH (LOCAL )?TIME ZONE)?$`)
	intervalType  = regexp.MustCompile(`^INTERVAL (YEAR|DAY)\(\d\) TO (MONTH|SECOND\(\d\))$`)
)

// kindOf maps a column's type to how it's read; 0 if the source can't read
// it, such as BFILE, VECTOR and object types.
func kindOf(c column) kind {
	switch c.dataType {
	case "NUMBER":
		if c.scale == 0 && c.precision > 0 {
			switch {
			case c.precision <= 4:
				return kInt16
			case c.precision <= 9:
				return kInt32
			case c.precision <= 18:
				return kInt64
			}
		}
		return kDecimal
	case "FLOAT", "BINARY_DOUBLE":
		return kFloat64
	case "BINARY_FLOAT":
		return kFloat32
	case "BOOLEAN":
		return kBool
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG", "ROWID", "UROWID", "XMLTYPE":
		return kString
	case "JSON":
		return kJSON
	case "RAW", "BLOB", "LONG RAW":
		return kBinary
	case "DATE":
		return kTimestamp
	}
	if m := timestampType.FindStringSubmatch(c.dataType); m != nil {
		ns := m[1] > "6"
		switch {
		case m[2] == "" && ns:
			return kTimestampNS
		case m[2] == "":
			return kTimestamp
		case ns:
			return kZonedNS
		}
		return kZoned
	}
	if m := intervalType.FindStringSubmatch(c.dataType); m != nil {
		if m[1] == "YEAR" {
			return kYearMonth
		}
		return kDaySecond
	}
	return 0
}

// selectExpr is how a query reads the column of a table aliased t: JSON
// and XMLTYPE as their text.
func (c column) selectExpr() string {
	q := "t." + quoteIdent(c.name)
	switch c.dataType {
	case "JSON":
		return "JSON_SERIALIZE(" + q + " RETURNING CLOB)"
	case "XMLTYPE":
		return "XMLSERIALIZE(CONTENT " + q + " AS CLOB)"
	}
	if c.kind == kDaySecond {
		// go-ora keeps 6 of its fraction's 9 digits.
		return "TO_CHAR(" + q + ")"
	}
	return q
}

// arrowTypes are the type a column's values are sent as, and the type the
// stream declares where the engine parses text to it.
func arrowTypes(c column) (sent, declared arrow.DataType) {
	switch c.kind {
	case kInt16:
		return arrow.PrimitiveTypes.Int16, nil
	case kInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case kInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case kDecimal:
		return arrow.BinaryTypes.String, declaredDecimal(c.precision, c.scale)
	case kFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case kFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case kBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case kJSON:
		t, _ := extensions.NewJSONType(arrow.BinaryTypes.String)
		return t, nil
	case kBinary:
		return arrow.BinaryTypes.Binary, nil
	case kTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case kTimestampNS:
		return arrow.BinaryTypes.String, &arrow.TimestampType{Unit: arrow.Microsecond}
	case kZoned:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case kZonedNS:
		return arrow.BinaryTypes.String, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case kYearMonth, kDaySecond:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	}
	return arrow.BinaryTypes.String, nil
}

// declaredDecimal is the decimal a NUMBER(p, s) is declared as: its own
// where Decimal128 holds it; NUMBER(p, s) with s > p as decimal(s, s); a
// negative scale's integers as decimal(p - s, 0); and NUMBER without a
// precision, or one Decimal128 can't hold, as decimal(38, 9). A value the
// declared type can't hold is quarantined by the engine.
func declaredDecimal(p, s int) *arrow.Decimal128Type {
	switch {
	case p < 1:
		if s == 0 {
			// NUMBER(*, 0), such as INTEGER.
			return &arrow.Decimal128Type{Precision: 38, Scale: 0}
		}
		return &arrow.Decimal128Type{Precision: 38, Scale: 9}
	case s < 0:
		return &arrow.Decimal128Type{Precision: int32(min(p-s, 38)), Scale: 0}
	case s > p:
		return &arrow.Decimal128Type{Precision: int32(min(s, 38)), Scale: int32(min(s, 38))}
	}
	return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(s)}
}

// declaredSchema is the table's columns' logical types.
func declaredSchema(t table) *arrow.Schema {
	fields := make([]arrow.Field, len(t.cols))
	for i, c := range t.cols {
		sent, declared := arrowTypes(c)
		if declared == nil {
			declared = sent
		}
		fields[i] = arrow.Field{Name: c.name, Type: declared, Nullable: !c.notNull}
	}
	return arrow.NewSchema(fields, nil)
}

// describe reads the schema's tables, in name order, with only the columns
// the source reads: hidden and virtual columns, which the redo log doesn't
// carry, and columns of types it can't read are left out. An XMLTYPE stored
// as binary XML, the default, is listed as a virtual column over a hidden
// BLOB, and read. Tables the
// destination keeps its state in, materialized view logs, nested, temporary
// and dropped tables, and tables with no column it reads aren't listed.
// names, if set, limits the tables to those, and each must be listed.
func describe(ctx context.Context, db queryer, schema string, names []string) ([]table, error) {
	rows, err := db.QueryContext(ctx, `SELECT c.table_name, c.column_name, c.data_type,
	  nvl(c.data_precision, -1), nvl(c.data_scale, -1), c.nullable, nvl(c.segment_column_id, -1)
	FROM all_tab_cols c JOIN all_tables t ON t.owner = c.owner AND t.table_name = c.table_name
	WHERE c.owner = :1 AND c.hidden_column = 'NO' AND (c.virtual_column = 'NO' OR c.data_type = 'XMLTYPE')
	  AND t.dropped = 'NO' AND t.temporary = 'N' AND t.nested = 'NO' AND t.secondary = 'N'
	  AND (t.iot_type IS NULL OR t.iot_type = 'IOT')
	  AND upper(t.table_name) NOT LIKE '\_OPENETL\_%' ESCAPE '\'
	  AND t.table_name NOT LIKE 'MLOG$\_%' ESCAPE '\' AND t.table_name NOT LIKE 'RUPD$\_%' ESCAPE '\'
	ORDER BY c.table_name, c.column_id`, schema)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = rows.Close() }()
	var tables []table
	unread := map[string][]string{}
	for rows.Next() {
		var tname, nullable string
		var c column
		if err := rows.Scan(&tname, &c.name, &c.dataType, &c.precision, &c.scale, &nullable, &c.seg); err != nil {
			return nil, classify(err)
		}
		if len(names) > 0 && !slices.Contains(names, tname) {
			continue
		}
		if len(tables) == 0 || tables[len(tables)-1].name != tname {
			tables = append(tables, table{name: tname})
		}
		t := &tables[len(tables)-1]
		c.notNull = nullable == "N"
		if c.kind = kindOf(c); c.kind == 0 {
			unread[tname] = append(unread[tname], c.name+" "+c.dataType)
			continue
		}
		t.cols = append(t.cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	for _, n := range names {
		i := slices.IndexFunc(tables, func(t table) bool { return t.name == n })
		switch {
		case i < 0:
			return nil, configInvalid("oracle: there's no table %s.%s the user can read", schema, n)
		case len(tables[i].cols) == 0:
			return nil, configInvalid("oracle: the source can't read any column of %s.%s: %s", schema, n, strings.Join(unread[n], ", "))
		}
	}
	// A table without a column the source reads isn't offered.
	tables = slices.DeleteFunc(tables, func(t table) bool { return len(t.cols) == 0 })
	if err := keys(ctx, db, schema, tables); err != nil {
		return nil, err
	}
	for i := range tables {
		t := &tables[i]
		switch {
		case len(t.key) == 0:
			t.noCDC = "it has no primary key, nor a unique index on NOT NULL columns"
		case len(unread[t.name]) > 0:
			t.noCDC = "it has columns the source can't read: " + strings.Join(unread[t.name], ", ")
		}
		if len(t.name) > 30 {
			// LogMiner reports changes to tables and columns with longer names
			// as UNSUPPORTED.
			t.noCDC = "LogMiner can't read changes to a table whose name is longer than 30 bytes"
		}
		for _, c := range t.cols {
			switch {
			case c.kind == kBool || c.kind == kJSON:
				// LogMiner reports a change to such a row as UNSUPPORTED.
				t.noCDC = fmt.Sprintf("LogMiner can't read changes to its %s column %s", c.dataType, c.name)
			case c.dataType == "UROWID" || c.dataType == "LONG" || c.dataType == "LONG RAW":
				// LogMiner spells a UROWID as Unsupported Type, and LONGs
				// aren't supplementally logged.
				t.noCDC = fmt.Sprintf("LogMiner can't read changes to its %s column %s", c.dataType, c.name)
			case len(c.name) > 30:
				t.noCDC = fmt.Sprintf("LogMiner can't read changes to its column %s, whose name is longer than 30 bytes", c.name)
			}
		}
	}
	return tables, nil
}

// keys sets each table's key: its primary key, or else its first unique
// index on columns that are all NOT NULL and read.
func keys(ctx context.Context, db queryer, schema string, tables []table) error {
	rows, err := db.QueryContext(ctx, `SELECT table_name, rank, name, column_name FROM (
	  SELECT cc.table_name, 0 rank, k.constraint_name name, cc.column_name, cc.position pos
	  FROM all_constraints k JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
	  WHERE k.owner = :1 AND k.constraint_type = 'P' AND k.status = 'ENABLED'
	  UNION ALL
	  SELECT i.table_name, 1, i.index_name, ic.column_name, ic.column_position
	  FROM all_indexes i JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name
	  WHERE i.table_owner = :2 AND i.uniqueness = 'UNIQUE' AND i.index_type = 'NORMAL' AND i.status IN ('VALID', 'N/A'))
	ORDER BY table_name, rank, name, pos`, schema, schema)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = rows.Close() }()
	// A primary key's index has the constraint's name.
	type index struct {
		table, name string
		rank        int
		cols        []string
	}
	var idx []index
	for rows.Next() {
		var t, n, c string
		var rank int
		if err := rows.Scan(&t, &rank, &n, &c); err != nil {
			return classify(err)
		}
		if l := len(idx) - 1; l < 0 || idx[l].table != t || idx[l].rank != rank || idx[l].name != n {
			idx = append(idx, index{table: t, name: n, rank: rank})
		}
		idx[len(idx)-1].cols = append(idx[len(idx)-1].cols, c)
	}
	if err := rows.Err(); err != nil {
		return classify(err)
	}
	for i := range tables {
		t := &tables[i]
		for _, x := range idx {
			if x.table != t.name || t.key != nil {
				continue
			}
			var key []int
			for _, name := range x.cols {
				j := slices.IndexFunc(t.cols, func(c column) bool { return c.name == name })
				if j < 0 || !t.cols[j].notNull {
					key = nil
					break
				}
				key = append(key, j)
			}
			t.key = key
		}
	}
	return nil
}

// seqType is a changelog's _openetl_seq: a change's commit SCN times 10^12,
// plus its place in its transaction from 1; a snapshot's rows' place is 0.
// The delete of the old key an update moves a row from has the update's.
var seqType = &arrow.Decimal128Type{Precision: 38, Scale: 0}

const seqPlaces = 1_000_000_000_000

func seqOf(scn uint64, place int) decimal128.Num {
	return decimal128.FromU64(scn).Mul(decimal128.FromU64(seqPlaces)).Add(decimal128.FromI64(int64(place)))
}

// adder appends a column's value, as go-ora scans it or redo decoding gives
// it, to its builder.
type adder func(v any) error

func newAdder(c column, b array.Builder) adder {
	switch c.kind {
	case kInt16:
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
			case string:
				// FLOAT, a NUMBER of binary precision.
				f, err := strconv.ParseFloat(x, 64)
				if err != nil {
					return err
				}
				fb.Append(f)
			default:
				return unexpected(v)
			}
			return nil
		}
	case kBool:
		bb := b.(*array.BooleanBuilder)
		return func(v any) error {
			switch x := v.(type) {
			case bool:
				bb.Append(x)
			case int64:
				bb.Append(x != 0)
			case float64:
				bb.Append(x != 0)
			case string:
				bb.Append(x != "0" && !strings.EqualFold(x, "false"))
			default:
				return unexpected(v)
			}
			return nil
		}
	case kDecimal, kString, kTimestampNS, kZonedNS:
		sb := b.(*array.StringBuilder)
		return func(v any) error {
			switch x := v.(type) {
			case string:
				sb.Append(x)
			case []byte:
				sb.BinaryBuilder.Append(x)
			case time.Time:
				if c.kind == kZonedNS {
					sb.Append(era(x).UTC().Format("2006-01-02 15:04:05.999999999Z07:00"))
				} else {
					sb.Append(wall(x).Format("2006-01-02 15:04:05.999999999"))
				}
			default:
				return unexpected(v)
			}
			return nil
		}
	case kJSON:
		sb := b.(*array.ExtensionBuilder).Builder.(*array.StringBuilder)
		return func(v any) error {
			switch x := v.(type) {
			case string:
				sb.Append(x)
			case []byte:
				sb.BinaryBuilder.Append(x)
			default:
				return unexpected(v)
			}
			return nil
		}
	case kBinary:
		bb := b.(*array.BinaryBuilder)
		return func(v any) error {
			switch x := v.(type) {
			case []byte:
				bb.Append(x)
			case string:
				bb.AppendString(x)
			default:
				return unexpected(v)
			}
			return nil
		}
	case kTimestamp, kZoned:
		tb := b.(*array.TimestampBuilder)
		return func(v any) error {
			x, ok := v.(time.Time)
			if !ok {
				return unexpected(v)
			}
			if c.kind == kTimestamp {
				x = wall(x)
			} else {
				x = era(x)
			}
			tb.Append(arrow.Timestamp(x.UnixMicro()))
			return nil
		}
	case kYearMonth, kDaySecond:
		ib := b.(*array.MonthDayNanoIntervalBuilder)
		return func(v any) error {
			s, ok := v.(string)
			if !ok {
				return unexpected(v)
			}
			iv, err := parseInterval(s, c.kind == kYearMonth)
			if err != nil {
				return err
			}
			ib.Append(iv)
			return nil
		}
	}
	return func(v any) error { return unexpected(v) }
}

func unexpected(v any) error { return fmt.Errorf("an unexpected %T", v) }

func intAdder(lo, hi int64, add func(int64)) adder {
	return func(v any) error {
		var x int64
		switch t := v.(type) {
		case int64:
			x = t
		case string:
			var err error
			if x, err = strconv.ParseInt(t, 10, 64); err != nil {
				return err
			}
		default:
			return unexpected(v)
		}
		if x < lo || x > hi {
			return fmt.Errorf("a value %d past its type's range", x)
		}
		add(x)
		return nil
	}
}

// wall is t's date and time of day, as Oracle shows a DATE or TIMESTAMP,
// read as UTC.
func wall(t time.Time) time.Time {
	t = era(t)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// era is t with its year as Go counts years: Oracle numbers 1 BC -1, as
// go-ora keeps it, and Go 0.
func era(t time.Time) time.Time {
	if y := t.Year(); y < 0 {
		return time.Date(y+1, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
	}
	return t
}

// parseInterval reads an interval as Oracle spells it: [+-]Y-M for YEAR TO
// MONTH, [+-]D HH:MI:SS[.F] for DAY TO SECOND.
func parseInterval(s string, yearMonth bool) (arrow.MonthDayNanoInterval, error) {
	bad := fmt.Errorf("%q isn't an interval", s)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimLeft(s, "+-")
	if yearMonth {
		y, m, ok := strings.Cut(s, "-")
		years, err1 := strconv.ParseInt(y, 10, 64)
		months, err2 := strconv.ParseInt(m, 10, 64)
		if !ok || err1 != nil || err2 != nil {
			return arrow.MonthDayNanoInterval{}, bad
		}
		total := years*12 + months
		if neg && total > math.MaxInt32+1 || !neg && total > math.MaxInt32 {
			return arrow.MonthDayNanoInterval{}, fmt.Errorf("the interval %s is longer than an Arrow interval holds", s)
		}
		if neg {
			total = -total
		}
		return arrow.MonthDayNanoInterval{Months: int32(total)}, nil
	}
	d, clock, ok := strings.Cut(s, " ")
	days, err := strconv.ParseInt(d, 10, 32)
	if !ok || err != nil {
		return arrow.MonthDayNanoInterval{}, bad
	}
	hms, frac, _ := strings.Cut(clock, ".")
	parts := strings.Split(hms, ":")
	if len(parts) != 3 || len(frac) > 9 {
		return arrow.MonthDayNanoInterval{}, bad
	}
	var nanos int64
	for _, p := range parts {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return arrow.MonthDayNanoInterval{}, bad
		}
		nanos = nanos*60 + n
	}
	nanos *= int64(time.Second)
	if frac != "" {
		f, err := strconv.ParseInt(frac+strings.Repeat("0", 9-len(frac)), 10, 64)
		if err != nil {
			return arrow.MonthDayNanoInterval{}, bad
		}
		nanos += f
	}
	if neg {
		days, nanos = -days, -nanos
	}
	return arrow.MonthDayNanoInterval{Days: int32(days), Nanoseconds: nanos}, nil
}

// batcher builds one table's Arrow batches; a changelog's carry an op, a
// seq and an unchanged column after the table's.
type batcher struct {
	rb    *array.RecordBuilder
	adds  []adder
	names []string
	cdc   bool
	rows  int
	bytes int
}

func newBatcher(mem memory.Allocator, cols []column, cdc bool) *batcher {
	fields := make([]arrow.Field, 0, len(cols)+2)
	for _, c := range cols {
		sent, _ := arrowTypes(c)
		fields = append(fields, arrow.Field{Name: c.name, Type: sent, Nullable: true})
	}
	if cdc {
		fields = append(fields, source.OpField(), source.SeqField(seqType), source.UnchangedField())
	}
	b := &batcher{rb: array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil)), cdc: cdc}
	for i, c := range cols {
		b.adds = append(b.adds, newAdder(c, b.rb.Field(i)))
		b.names = append(b.names, c.name)
	}
	return b
}

// add appends a row; a nil value is null. A row that fails leaves the
// batcher unusable.
func (b *batcher) add(vals []any) error {
	for i, add := range b.adds {
		v := vals[i]
		if v == nil {
			// A null takes its slot in the builder, as a number does.
			b.rb.Field(i).AppendNull()
			b.bytes += 8
			continue
		}
		if err := add(v); err != nil {
			return fmt.Errorf("column %q: %w", b.names[i], err)
		}
		switch x := v.(type) {
		case []byte:
			b.bytes += len(x)
		case string:
			b.bytes += len(x)
		default:
			b.bytes += 8
		}
	}
	b.rows++
	return nil
}

// change appends a changelog row: its values, op, seq, and the columns an
// update didn't carry, whose values are nil.
func (b *batcher) change(vals []any, op string, seq decimal128.Num, unchanged []string) error {
	n := len(b.adds)
	b.rb.Field(n).(*array.StringBuilder).Append(op)
	b.rb.Field(n + 1).(*array.Decimal128Builder).Append(seq)
	lb := b.rb.Field(n + 2).(*array.ListBuilder)
	if len(unchanged) == 0 {
		lb.AppendNull()
	} else {
		lb.Append(true)
		vb := lb.ValueBuilder().(*array.StringBuilder)
		for _, c := range unchanged {
			vb.Append(c)
		}
	}
	return b.add(vals)
}

func (b *batcher) flush(emit func(arrow.RecordBatch) error) error {
	if b.rows == 0 {
		return nil
	}
	rec := b.rb.NewRecordBatch()
	b.rows, b.bytes = 0, 0
	if err := emit(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() { b.rb.Release() }
