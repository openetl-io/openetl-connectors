package oracle

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/sijms/go-ora/v2/converters"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// expr is a value in LogMiner's SQL_REDO: a literal, NULL, a function of
// values such as TO_DATE('…', '…'), or values joined by ||.
type expr struct {
	null bool
	// A literal's text.
	text string
	lit  bool
	// A function's name, upper case, and its arguments.
	fn   string
	args []expr
	cat  []expr
}

// redoRow is an INSERT, UPDATE or DELETE as SQL_REDO spells it.
type redoRow struct {
	op string
	// An insert's values; an update's old values from its WHERE, which
	// supplemental logging of all columns fills, with its SET's over them; a
	// delete's old values. By column name.
	vals map[string]expr
}

// parseRedo parses a DML statement as LogMiner writes it with
// NO_ROWID_IN_STMT, such as
//
//	insert into "S"."T"("A","B") values ('1',NULL);
//	update "S"."T" set "B" = 'x' where "A" = '1' and "B" IS NULL;
//	delete from "S"."T" where "A" = '1' and "B" = 'x';
func parseRedo(sql string) (redoRow, error) {
	p := &redoParser{s: sql}
	r := redoRow{vals: map[string]expr{}}
	if rest, ok := strings.CutPrefix(sql, "XML DOC BEGIN:"); ok {
		// An XMLTYPE written: its row, its XMLTYPE unchanged until it's
		// read.
		p.s, r.op = rest, source.OpUpdate
		if !p.words("select") {
			return r, p.fail()
		}
		if _, ok := p.ident(); !ok || !p.words("from") || !p.table() || !p.words("where") {
			return r, p.fail()
		}
		return r, p.where(r.vals)
	}
	switch strings.ToLower(p.word()) {
	case "insert":
		r.op = source.OpInsert
		if !p.words("into") || !p.table() || !p.punct("(") {
			return r, p.fail()
		}
		var names []string
		for {
			n, ok := p.ident()
			if !ok {
				return r, p.fail()
			}
			names = append(names, n)
			if p.punct(")") {
				break
			}
			if !p.punct(",") {
				return r, p.fail()
			}
		}
		if !p.words("values") || !p.punct("(") {
			return r, p.fail()
		}
		for i, n := range names {
			if i > 0 && !p.punct(",") {
				return r, p.fail()
			}
			v, err := p.value()
			if err != nil {
				return r, err
			}
			r.vals[n] = v
		}
		if !p.punct(")") {
			return r, p.fail()
		}
	case "update":
		r.op = source.OpUpdate
		if !p.table() || !p.words("set") {
			return r, p.fail()
		}
		set := map[string]expr{}
		for {
			n, ok := p.ident()
			if !ok || !p.punct("=") {
				return r, p.fail()
			}
			v, err := p.value()
			if err != nil {
				return r, err
			}
			set[n] = v
			if !p.punct(",") {
				break
			}
		}
		if p.words("where") {
			if err := p.where(r.vals); err != nil {
				return r, err
			}
		}
		for n, v := range set {
			r.vals[n] = v
		}
	case "delete":
		r.op = source.OpDelete
		if !p.words("from") || !p.table() || !p.words("where") {
			return r, p.fail()
		}
		if err := p.where(r.vals); err != nil {
			return r, err
		}
	case "declare":
		// A LOB written in pieces: its row, selected for update, whose
		// values are the change's, its LOBs unchanged until they're read.
		r.op = source.OpUpdate
		i := strings.Index(p.s, "BEGIN")
		if i < 0 {
			return r, p.fail()
		}
		p.i = i + len("BEGIN")
		if !p.words("select") {
			return r, p.fail()
		}
		if _, ok := p.ident(); !ok || !p.words("into") || p.word() == "" || !p.words("from") || !p.table() || !p.words("where") {
			return r, p.fail()
		}
		if err := p.where(r.vals); err != nil {
			return r, err
		}
		if !p.words("for", "update") {
			return r, p.fail()
		}
		return r, nil
	default:
		return r, p.fail()
	}
	p.punct(";")
	if p.skip(); p.i < len(p.s) {
		return r, p.fail()
	}
	return r, nil
}

type redoParser struct {
	s string
	i int
}

func (p *redoParser) fail() error {
	at := min(p.i, len(p.s))
	return fmt.Errorf("can't parse LogMiner's SQL_REDO at byte %d: %.80q", at, p.s[at:])
}

func (p *redoParser) skip() {
	for p.i < len(p.s) && strings.ContainsRune(" \t\r\n", rune(p.s[p.i])) {
		p.i++
	}
}

func (p *redoParser) punct(s string) bool {
	p.skip()
	if strings.HasPrefix(p.s[p.i:], s) {
		p.i += len(s)
		return true
	}
	return false
}

// word reads a bare word: letters, digits, _, $ and #.
func (p *redoParser) word() string {
	p.skip()
	j := p.i
	for j < len(p.s) {
		c := p.s[j]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$' || c == '#' {
			j++
			continue
		}
		break
	}
	w := p.s[p.i:j]
	p.i = j
	return w
}

// words reads the bare words ws, in any case, or reads nothing.
func (p *redoParser) words(ws ...string) bool {
	at := p.i
	for _, w := range ws {
		if !strings.EqualFold(p.word(), w) {
			p.i = at
			return false
		}
	}
	return true
}

func (p *redoParser) quoted(q byte) (string, bool) {
	p.skip()
	if p.i >= len(p.s) || p.s[p.i] != q {
		return "", false
	}
	var b strings.Builder
	for j := p.i + 1; j < len(p.s); j++ {
		if p.s[j] != q {
			b.WriteByte(p.s[j])
			continue
		}
		if j+1 < len(p.s) && p.s[j+1] == q {
			b.WriteByte(q)
			j++
			continue
		}
		p.i = j + 1
		return b.String(), true
	}
	return "", false
}

// ident reads "NAME", or a."NAME", as an update of a LOB column names it.
func (p *redoParser) ident() (string, bool) {
	at := p.i
	if p.skip(); p.i < len(p.s) && p.s[p.i] != '"' && (p.word() == "" || !p.punct(".")) {
		p.i = at
		return "", false
	}
	n, ok := p.quoted('"')
	if !ok {
		p.i = at
	}
	return n, ok
}

// table reads "OWNER"."TABLE", and an alias after it.
func (p *redoParser) table() bool {
	if _, ok := p.quoted('"'); !ok {
		return false
	}
	if p.punct(".") {
		if _, ok := p.quoted('"'); !ok {
			return false
		}
	}
	at := p.i
	if w := strings.ToLower(p.word()); w == "set" || w == "where" {
		p.i = at
	}
	return true
}

// where reads a WHERE's conditions: "C" = value, "C" IS NULL, and
// ROWID = '…', joined by and.
func (p *redoParser) where(vals map[string]expr) error {
	for {
		n, ok := p.ident()
		switch {
		case ok && p.words("IS", "NULL"):
			vals[n] = expr{null: true}
		case ok && p.punct("="):
			v, err := p.value()
			if err != nil {
				return err
			}
			vals[n] = v
		case !ok && strings.EqualFold(p.word(), "ROWID") && p.punct("="):
			if _, ok := p.quoted('\''); !ok {
				return p.fail()
			}
		default:
			return p.fail()
		}
		if !p.words("and") {
			return nil
		}
	}
}

// value reads a value, or values joined by ||.
func (p *redoParser) value() (expr, error) {
	v, err := p.term()
	if err != nil {
		return v, err
	}
	if !p.punct("||") {
		return v, nil
	}
	cat := []expr{v}
	for {
		t, err := p.term()
		if err != nil {
			return v, err
		}
		cat = append(cat, t)
		if !p.punct("||") {
			return expr{cat: cat}, nil
		}
	}
}

func (p *redoParser) term() (expr, error) {
	if s, ok := p.quoted('\''); ok {
		return expr{lit: true, text: s}, nil
	}
	at := p.i
	w := p.word()
	switch {
	case w == "":
		return expr{}, p.fail()
	case strings.EqualFold(w, "NULL"):
		return expr{null: true}, nil
	case !p.punct("("):
		// Such as Unsupported Type, which LogMiner writes for a value it
		// can't spell.
		p.i = at
		return expr{}, p.fail()
	}
	e := expr{fn: strings.ToUpper(w)}
	if p.punct(")") {
		return e, nil
	}
	for {
		a, err := p.value()
		if err != nil {
			return e, err
		}
		e.args = append(e.args, a)
		if p.punct(")") {
			return e, nil
		}
		if !p.punct(",") {
			return e, p.fail()
		}
	}
}

// lobKinds are the columns the redo log doesn't carry the values of in
// full: a change's LOBs are read from the table as of its commit.
func (c column) lob() bool {
	switch c.dataType {
	case "CLOB", "NCLOB", "BLOB", "XMLTYPE":
		return true
	}
	return false
}

// redoValue is a column's value as SQL_REDO spells it, as go-ora would scan
// it.
func redoValue(c column, e expr) (any, error) {
	switch {
	case e.null:
		return nil, nil
	case e.cat != nil:
		var b strings.Builder
		for _, p := range e.cat {
			s, err := text(p)
			if err != nil {
				return nil, err
			}
			b.WriteString(s)
		}
		return b.String(), nil
	}
	switch e.fn {
	case "HEXTORAW":
		s, err := arg(e)
		if err != nil {
			return nil, err
		}
		return hex.DecodeString(s)
	case "TO_DATE", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ":
		s, err := arg(e)
		if err != nil {
			return nil, err
		}
		return parseTime(s)
	case "TO_YMINTERVAL", "TO_DSINTERVAL":
		return arg(e)
	case "UNISTR":
		return text(e)
	case "":
	default:
		return nil, fmt.Errorf("a value %s(…) the source can't read", e.fn)
	}
	switch c.kind {
	case kInt16, kInt32, kInt64, kDecimal:
		return plainNumber(e.text)
	case kFloat32:
		f, err := parseFloat(e.text, 32)
		return float32(f), err
	case kFloat64:
		if c.dataType == "FLOAT" {
			return plainNumber(e.text)
		}
		return parseFloat(e.text, 64)
	case kString, kBinary:
		return e.text, nil
	}
	return nil, fmt.Errorf("a literal %q for a %s", e.text, c.dataType)
}

func arg(e expr) (string, error) {
	if len(e.args) == 0 || !e.args[0].lit {
		return "", fmt.Errorf("a value %s(…) the source can't read", e.fn)
	}
	return e.args[0].text, nil
}

// text is a literal's or a UNISTR's text.
func text(e expr) (string, error) {
	switch {
	case e.lit:
		return e.text, nil
	case e.fn == "UNISTR":
		s, err := arg(e)
		if err != nil {
			return "", err
		}
		return unistr(s)
	}
	return "", fmt.Errorf("a value %s(…) in text the source can't read", e.fn)
}

// unistr decodes UNISTR's argument: \XXXX is a UTF-16 code unit, \\ a
// backslash.
func unistr(s string) (string, error) {
	var units []uint16
	var b strings.Builder
	flush := func() {
		if len(units) > 0 {
			b.WriteString(string(utf16.Decode(units)))
			units = units[:0]
		}
	}
	for i := 0; i < len(s); {
		switch {
		case s[i] != '\\':
			flush()
			r, n := utf8.DecodeRuneInString(s[i:])
			b.WriteRune(r)
			i += n
		case strings.HasPrefix(s[i:], `\\`):
			flush()
			b.WriteByte('\\')
			i += 2
		case i+5 <= len(s):
			u, err := strconv.ParseUint(s[i+1:i+5], 16, 16)
			if err != nil {
				return "", fmt.Errorf("UNISTR %q: %w", s, err)
			}
			units = append(units, uint16(u))
			i += 5
		default:
			return "", fmt.Errorf("UNISTR %q ends early", s)
		}
	}
	flush()
	return b.String(), nil
}

// plainNumber spells a number LogMiner may write in exponent form, such as
// 1.5E-130, in digits, as go-ora reads a NUMBER.
func plainNumber(s string) (string, error) {
	mant, exp, ok := strings.Cut(strings.ToUpper(s), "E")
	if !ok {
		return s, nil
	}
	e, err := strconv.Atoi(exp)
	if err != nil {
		return "", fmt.Errorf("%q isn't a number", s)
	}
	neg := strings.HasPrefix(mant, "-")
	mant = strings.TrimLeft(mant, "+-")
	ip, fp, _ := strings.Cut(mant, ".")
	digits := strings.TrimLeft(ip+fp, "0")
	point := len(ip) + e - (len(ip+fp) - len(digits))
	digits = strings.TrimRight(digits, "0")
	var out string
	switch {
	case digits == "":
		return "0", nil
	case point <= 0:
		out = "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		out = digits + strings.Repeat("0", point-len(digits))
	default:
		out = digits[:point] + "." + digits[point:]
	}
	if neg {
		out = "-" + out
	}
	return out, nil
}

// parseFloat reads BINARY_FLOAT's and BINARY_DOUBLE's spelling, with Nan,
// Inf and -Inf.
func parseFloat(s string, bits int) (float64, error) {
	switch strings.ToLower(s) {
	case "nan":
		return math.NaN(), nil
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, bits)
}

// parseTime reads a date or timestamp as the session's formats spell it:
// SYYYY-MM-DD HH24:MI:SS[.FF9][ TZH:TZM], with a minus for BC years. One
// without an offset is in UTC, as a TIMESTAMP WITH LOCAL TIME ZONE is in
// the session's time zone.
func parseTime(s string) (time.Time, error) {
	bad := fmt.Errorf("%q isn't a date-time", s)
	s = strings.TrimSpace(s)
	sign := 1
	if strings.HasPrefix(s, "-") {
		sign, s = -1, s[1:]
	}
	date, rest, _ := strings.Cut(s, " ")
	clock, zone, _ := strings.Cut(rest, " ")
	ymd := strings.Split(date, "-")
	if len(ymd) != 3 {
		return time.Time{}, bad
	}
	var n [6]int
	hms := strings.Split(clock, ":")
	if clock == "" {
		hms = nil
	}
	if len(hms) != 0 && len(hms) != 3 {
		return time.Time{}, bad
	}
	sec, frac, _ := strings.Cut(strings.Join(hms, ":"), ".")
	parts := append(ymd, strings.Split(sec, ":")...)
	for i, p := range parts {
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil || i >= len(n) {
			return time.Time{}, bad
		}
		n[i] = v
	}
	ns := 0
	if frac != "" {
		if len(frac) > 9 {
			return time.Time{}, bad
		}
		v, err := strconv.Atoi(frac + strings.Repeat("0", 9-len(frac)))
		if err != nil {
			return time.Time{}, bad
		}
		ns = v
	}
	loc := time.UTC
	if zone != "" {
		h, m, _ := strings.Cut(zone, ":")
		hh, err1 := strconv.Atoi(h)
		mm, err2 := strconv.Atoi(m)
		if err1 != nil || err2 != nil {
			return time.Time{}, bad
		}
		off := hh*3600 + mm*60
		if strings.HasPrefix(h, "-") {
			off = hh*3600 - mm*60
		}
		loc = time.FixedZone(zone, off)
	}
	return time.Date(sign*n[0], time.Month(n[1]), n[2], n[3], n[4], n[5], ns, loc), nil
}

// rawValue is a column's value from its internal format, as LogMiner gives
// it in HEXTORAW for a row it decodes with a dictionary that doesn't match
// the row's table, as go-ora would scan it. Text is in the database's
// character set, which this takes to be AL32UTF8, and national text in
// AL16UTF16.
func rawValue(c column, b []byte) (any, error) {
	switch c.dataType {
	case "NUMBER":
		return converters.NumberToString(b)
	case "FLOAT":
		s, err := converters.NumberToString(b)
		if err != nil {
			return nil, err
		}
		return strconv.ParseFloat(s, 64)
	case "BINARY_FLOAT":
		if len(b) != 4 {
			return nil, errors.New("a BINARY_FLOAT that isn't 4 bytes")
		}
		return converters.ConvertBinaryFloat(b), nil
	case "BINARY_DOUBLE":
		if len(b) != 8 {
			return nil, errors.New("a BINARY_DOUBLE that isn't 8 bytes")
		}
		return converters.ConvertBinaryDouble(b), nil
	case "VARCHAR2", "CHAR", "LONG", "ROWID":
		if !utf8.Valid(b) {
			return nil, errors.New("text that isn't UTF-8: the source reads redo it can't decode with the dictionary only in an AL32UTF8 database")
		}
		return string(b), nil
	case "NVARCHAR2", "NCHAR":
		if len(b)%2 != 0 {
			return nil, errors.New("national text that isn't UTF-16")
		}
		units := make([]uint16, len(b)/2)
		for i := range units {
			units[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
		}
		return string(utf16.Decode(units)), nil
	case "RAW", "LONG RAW":
		return b, nil
	}
	switch c.kind {
	case kTimestamp, kTimestampNS, kZoned, kZonedNS:
		return converters.DecodeDate(b)
	case kYearMonth:
		if len(b) != 5 {
			return nil, errors.New("an INTERVAL YEAR TO MONTH that isn't 5 bytes")
		}
		return converters.ConvertIntervalYM_DTY(b), nil
	case kDaySecond:
		if len(b) != 11 {
			return nil, errors.New("an INTERVAL DAY TO SECOND that isn't 11 bytes")
		}
		return converters.ConvertIntervalDS_DTY(b), nil
	}
	return nil, fmt.Errorf("a %s the source can't read from its internal format", c.dataType)
}
