package oracle

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// The SQL an apply runs. A column's value takes the type of the column it
// goes into: Oracle doesn't convert every type that fits another implicitly,
// nor mixes them in CASE. JSON is built and read with Oracle's SQL/JSON
// functions, from 19c on.

// val is rel's column c as a value of its type in to, from its type in from.
func val(rel, c string, from, to map[string]string) string {
	f := from[c]
	return convert(rel+"."+quoteIdent(c), f, cmpOr(to[c], f))
}

// has is whether the JSON array rel.col has name: a condition, false for
// null.
func has(rel, col, name string) string {
	return "JSON_EXISTS(" + rel + "." + quoteIdent(col) + `, '$[*]?(@ == $n)' PASSING ` + quoteLiteral(name) + ` AS "n")`
}

// jsonPath is a SQL literal of the JSON path to an object's member name,
// which as an Oracle name holds no double quote.
func jsonPath(name string) string {
	return quoteLiteral(`$."` + strings.ReplaceAll(name, `\`, `\\`) + `"`)
}

// holds is whether the JSON object rel.col has the member name.
func holds(rel, col, name string) string {
	return "JSON_EXISTS(" + rel + "." + quoteIdent(col) + ", " + jsonPath(name) + ")"
}

// jsonArray is a JSON array of the parts that aren't null, each a string;
// null if all are.
func jsonArray(parts []string) string {
	if len(parts) == 0 {
		return "NULL"
	}
	a := "JSON_ARRAY(" + strings.Join(parts, ", ") + " ABSENT ON NULL RETURNING CLOB)"
	return "CASE WHEN JSON_EXISTS(" + a + ", '$[0]') THEN " + a + " END"
}

// jsonObject is a JSON object from each member's name to its string value,
// leaving out those that are null; "NULL" for no members.
func jsonObject(members [][2]string) string {
	if len(members) == 0 {
		return "NULL"
	}
	parts := make([]string, len(members))
	for i, m := range members {
		parts[i] = "KEY " + quoteLiteral(m[0]) + " VALUE " + m[1]
	}
	return "JSON_OBJECT(" + strings.Join(parts, ", ") + " ABSENT ON NULL RETURNING CLOB)"
}

// nonEmpty is the JSON object o, or null if it has no members.
func nonEmpty(o string) string {
	if o == "NULL" {
		return o
	}
	return "CASE WHEN JSON_EXISTS(" + o + ", '$.*') THEN " + o + " END"
}

// heldObject maps each of rel's held columns to its _openetl_id, the
// version whose value is held; null for none. names are every column a
// staged row holds.
func heldObject(rel string, names []string) string {
	members := make([][2]string, len(names))
	for i, n := range names {
		members[i] = [2]string{n, "CASE WHEN " + has(rel, colHeld, n) + " THEN " + rel + "." + quoteIdent(colID) + " END"}
	}
	return nonEmpty(jsonObject(members))
}

// selectList is cols from rel, as the types in to, with a staged row's list
// of held columns made into the target's object.
func selectList(cols []string, rel string, held []string, from, to map[string]string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		v := val(rel, c, from, to)
		if c == colHeld && slices.Contains(cols, colID) {
			v = heldObject(rel, held)
		}
		out[i] = v + " AS " + quoteIdent(c)
	}
	return strings.Join(out, ", ")
}

// insert copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, the first staged, and with
// Dedupe only IDs the target doesn't have.
func (w *writer) insert(s stagedStream, p prepared) string {
	into := "INSERT INTO " + w.into(s) + " (" + quoteAll(p.cols) + ") "
	sel := selectList(p.cols, "s", p.held, p.types, p.into)
	id := quoteIdent(colID)
	if !s.deduped() {
		return into + "SELECT " + sel + " FROM " + p.rows + " s"
	}
	q := into + "SELECT " + sel + " FROM (SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x." + id + " ORDER BY x." + quoteIdent(colOrd) + `) AS "_openetl_rn" FROM ` +
		p.rows + ` x) s WHERE s."_openetl_rn" = 1`
	if s.Dedupe {
		q += " AND NOT EXISTS (SELECT 1 FROM " + w.into(s) + " t WHERE t." + id + " = s." + id + ")"
	}
	return q
}

// patch fills held cells from the epoch's patches: each column where the
// target still holds the version the patch comes from, matched by key for
// merge and by _openetl_id for append. Of patches from one version, the
// last staged fills it.
func (w *writer) patch(s stagedStream, p prepared) []string {
	cols := p.patch
	if len(cols) == 0 {
		return nil
	}
	q := quoteIdent
	match, by := []string{"p." + q(colID) + " = t." + q(colID)}, []string{"x." + q(colID)}
	if s.Mode == modeMerge {
		match, by = nil, nil
		for _, k := range s.Key {
			match = append(match, "p."+q(k)+" = t."+q(k))
			by = append(by, "x."+q(k))
		}
		by = append(by, "x."+q(colID))
	}
	var out []string
	for _, c := range cols {
		if slices.Contains(metaColumns, c) || slices.Contains(s.Key, c) {
			continue
		}
		held := c
		if f, ok := s.Fills[c]; ok {
			held = f
		}
		src := `(SELECT * FROM (SELECT x.*, ROW_NUMBER() OVER (PARTITION BY ` + strings.Join(by, ", ") + " ORDER BY x." + q(colOrd) + ` DESC) AS "_openetl_rn" FROM ` +
			p.patches + " x WHERE " + has("x", colHeld, held) + `) WHERE "_openetl_rn" = 1)`
		from := " FROM " + src + " p WHERE " + strings.Join(match, " AND ") + " AND p." + q(colID) + " = JSON_VALUE(t." + q(colHeld) + ", " + jsonPath(held) + ")"
		gone, _ := json.Marshal(map[string]any{held: nil})
		set := "t." + q(c) + " = (SELECT " + val("p", c, p.patchTypes, p.into) + from + "), t." + q(colHeld) + " = " +
			nonEmpty("JSON_MERGEPATCH(t."+q(colHeld)+", "+quoteLiteral(string(gone))+" RETURNING CLOB)")
		if slices.Contains(cols, colSynced) {
			set += ", t." + q(colSynced) + " = (SELECT " + val("p", colSynced, p.patchTypes, p.into) + from + ")"
		}
		out = append(out, "UPDATE "+w.target(s.Table)+" t SET "+set+" WHERE EXISTS (SELECT 1"+from+")")
	}
	return out
}

// merge upserts each key's latest staged version, by _openetl_seq and then
// staging order, unless the target's is newer. A delete keeps the row's
// other columns as they were, with its metadata columns updated, or with
// hard deletes removes the row. A column in unchanged that the latest
// version didn't send takes its value from the key's latest staged version
// that did, or else keeps the target's. An update's values all see the
// target's row as it was.
func (w *writer) merge(s stagedStream, p prepared) []string {
	cols := p.cols
	q := quoteIdent
	hasSeq, hasOp := slices.Contains(cols, colSeq), slices.Contains(cols, colOp)
	hasHeld := slices.Contains(cols, colHeld) && slices.Contains(cols, colID)
	hard := hasOp && s.HardDeletes
	latest := w.latest(s, p, hasSeq)
	on := make([]string, len(s.Key))
	for i, k := range s.Key {
		on[i] = "t." + q(k) + " = s." + q(k)
	}
	isDelete := "s." + q(colOp) + " = 'delete'"
	notDelete := "(s." + q(colOp) + " IS NULL OR s." + q(colOp) + " <> 'delete')"
	// Unless the target's version is newer; a missing sequence doesn't stop
	// an update.
	newer := "1 = 1"
	if hasSeq {
		newer = "(t." + q(colSeq) + " IS NULL OR s." + q(colSeq) + " IS NULL OR t." + q(colSeq) + " <= s." + q(colSeq) + ")"
	}
	vals := make([]string, len(cols))
	var sets []string
	for i, c := range cols {
		vals[i] = val("s", c, p.types, p.into)
		if slices.Contains(s.Key, c) {
			continue
		}
		v, cur := vals[i], "t."+q(c)
		switch {
		case c == colUnchanged:
			// The columns neither ever sent.
			parts := make([]string, len(p.everUnchanged))
			for j, n := range p.everUnchanged {
				parts[j] = "CASE WHEN " + has("s", c, n) + " AND " + has("t", c, n) + " THEN " + quoteLiteral(n) + " END"
			}
			v = "CASE WHEN s." + q(c) + " IS NULL OR " + cur + " IS NULL THEN NULL ELSE " + jsonArray(parts) + " END"
		case c == colHeld && hasHeld:
			// The row's, and the target's for the columns it didn't send.
			var kept [][2]string
			if slices.Contains(cols, colUnchanged) {
				for _, n := range p.everUnchanged {
					kept = append(kept, [2]string{n, "CASE WHEN " + has("s", colUnchanged, n) + " THEN JSON_VALUE(" + cur + ", " + jsonPath(n) + ") END"})
				}
			}
			v = nonEmpty("JSON_MERGEPATCH(NVL(" + jsonObject(kept) + ", '{}'), NVL(s." + q(c) + ", '{}') RETURNING CLOB)")
		case slices.Contains(p.unchanged, c):
			v = "CASE WHEN " + has("s", colUnchanged, c) + " THEN " + cur + " ELSE " + v + " END"
		}
		if hasHeld && slices.Contains(s.Last, c) && !slices.Contains(metaColumns, c) {
			v = "CASE WHEN " + holds("s", colHeld, c) + " THEN " + cur + " ELSE " + v + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metaColumns, c) || c == colUnchanged || c == colHeld) {
			v = "CASE WHEN " + isDelete + " THEN " + cur + " ELSE " + v + " END"
		}
		sets = append(sets, cur+" = "+v)
	}
	m := "MERGE INTO " + w.target(s.Table) + " t USING " + latest + " s ON (" + strings.Join(on, " AND ") + ")"
	if len(sets) > 0 {
		where := newer
		if hard {
			where += " AND " + notDelete
		}
		m += " WHEN MATCHED THEN UPDATE SET " + strings.Join(sets, ", ") + " WHERE " + where
	}
	m += " WHEN NOT MATCHED THEN INSERT (" + quoteAll(cols) + ") VALUES (" + strings.Join(vals, ", ") + ")"
	if !hard {
		return []string{m}
	}
	m += " WHERE " + notDelete
	del := "DELETE FROM " + w.target(s.Table) + " t WHERE EXISTS (SELECT 1 FROM " + latest + " s WHERE " + strings.Join(on, " AND ") + " AND " + isDelete + " AND " + newer + ")"
	return []string{m, del}
}

// latest is merge's derived table of each key's latest staged version. With
// unchanged columns, each comes from the key's latest earlier version that
// sent it, if any, which a window finds by its _openetl_ord; its
// _openetl_unchanged then leaves it out. A null _openetl_seq comes first.
func (w *writer) latest(s stagedStream, p prepared, hasSeq bool) string {
	cols := p.cols
	q := quoteIdent
	ord := q(colOrd)
	key := make([]string, len(s.Key))
	for i, k := range s.Key {
		key[i] = "x." + q(k)
	}
	part := "PARTITION BY " + strings.Join(key, ", ")
	first, inOrder := "x."+ord+" DESC", "x."+ord
	if hasSeq {
		first, inOrder = "x."+q(colSeq)+" DESC NULLS LAST, "+first, "x."+q(colSeq)+" NULLS FIRST, "+inOrder
	}
	window := []string{"ROW_NUMBER() OVER (" + part + " ORDER BY " + first + `) AS "_openetl_rn"`}
	// e is, for each unchanged column, the alias of the version that sent
	// it.
	e := map[string]string{}
	var joins string
	for i, c := range p.unchanged {
		at := fmt.Sprintf(`"_openetl_sent%d"`, i)
		e[c] = fmt.Sprintf("e%d", i)
		window = append(window, "LAST_VALUE(CASE WHEN "+has("x", colUnchanged, c)+" THEN NULL ELSE x."+ord+" END IGNORE NULLS) OVER ("+part+
			" ORDER BY "+inOrder+" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "+at)
		joins += " LEFT JOIN " + p.rows + " " + e[c] + " ON " + e[c] + "." + ord + " = l." + at
	}
	l := "(SELECT x.*, " + strings.Join(window, ", ") + " FROM " + p.rows + " x) l"
	if len(p.unchanged) == 0 {
		return "(SELECT " + selectList(cols, "l", p.held, p.types, nil) + " FROM " + l + ` WHERE l."_openetl_rn" = 1)`
	}
	found := func(c string) string {
		if _, ok := e[c]; !ok {
			return "1 = 0"
		}
		return "(" + has("l", colUnchanged, c) + " AND " + e[c] + "." + ord + " IS NOT NULL)"
	}
	hasHeld := slices.Contains(cols, colHeld) && slices.Contains(cols, colID)
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
		switch {
		case slices.Contains(p.unchanged, c):
			sel[i] = "CASE WHEN " + found(c) + " THEN " + e[c] + "." + q(c) + " ELSE l." + q(c) + " END AS " + q(c)
		case c == colUnchanged:
			parts := make([]string, len(p.everUnchanged))
			for j, n := range p.everUnchanged {
				parts[j] = "CASE WHEN " + has("l", c, n) + " AND NOT " + found(n) + " THEN " + quoteLiteral(n) + " END"
			}
			sel[i] = jsonArray(parts) + " AS " + q(c)
		case c == colHeld && hasHeld:
			// The version a column comes from may hold it.
			var members [][2]string
			for _, n := range slices.Compact(slices.Sorted(slices.Values(slices.Concat(p.held, p.unchanged)))) {
				own := "CASE WHEN " + has("l", colHeld, n) + " THEN l." + q(colID) + " END"
				v := own
				if from, ok := e[n]; ok {
					v = "COALESCE(CASE WHEN " + found(n) + " AND " + has(from, colHeld, n) + " THEN " + from + "." + q(colID) + " END, " + own + ")"
				}
				members = append(members, [2]string{n, v})
			}
			sel[i] = nonEmpty(jsonObject(members)) + " AS " + q(c)
		}
	}
	return "(SELECT " + strings.Join(sel, ", ") + " FROM " + l + joins + ` WHERE l."_openetl_rn" = 1)`
}
