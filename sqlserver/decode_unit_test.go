package sqlserver

import "testing"

// A CLR type's name comes from sys.types, and can be any text: a key column
// of one is converted to by name in a split's or a chunk's conditions.
func TestTypeNamesAreQuoted(t *testing.T) {
	for _, c := range []struct {
		col  sourceColumn
		want string
	}{
		{sourceColumn{typ: "int"}, "int"},
		{sourceColumn{typ: "sql_variant"}, "sql_variant"},
		{sourceColumn{typ: "datetime2", scale: 7}, "datetime2(7)"},
		{sourceColumn{typ: "nvarchar", maxLength: -1}, "nvarchar(max)"},
		{sourceColumn{typ: "Point"}, "[Point]"},
		{sourceColumn{typ: "int, 1)) x; DROP TABLE dbo.t; --"}, "[int, 1)) x; DROP TABLE dbo.t; --]"},
		{sourceColumn{typ: "a]b"}, "[a]]b]"},
	} {
		if got := c.col.sqlType(); got != c.want {
			t.Errorf("sqlType of %q = %q, want %q", c.col.typ, got, c.want)
		}
	}
	if got, want := (sourceColumn{typ: "x) --"}).fromText("@p1"), "CONVERT([x) --], @p1)"; got != want {
		t.Errorf("fromText = %q, want %q", got, want)
	}
}
