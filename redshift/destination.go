// Package redshift has the Amazon Redshift destination: append, overwrite
// and merge writes, staged as Parquet files in S3 that COPY loads into one
// table per stream, epoch and writer lane, and applied as one behind a fence
// with a marker per stream in _openetl_loads.
//
// Redshift enforces no keys and has no upsert, so a merge works out each
// key's row, against the target's, in a temporary table, and updates,
// inserts and deletes from it in the apply's transaction. Each transaction
// that applies or raises the fence locks the fence's table first, which
// orders them.
package redshift

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN sdk.Secret `json:"dsn"`
	// Used when the pipeline names no namespace. Default "public".
	Schema string `json:"schema"`
	// Where the staged files wait for COPY.
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
	// Empty: the AWS default.
	Region string `json:"region"`
	// The role COPY assumes, roles chained with commas, or "default".
	IAMRole string `json:"iamRole"`
	// Upload and delete the staged files. Empty: the AWS default
	// credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	SessionToken    sdk.Secret `json:"sessionToken"`
}

type Destination struct {
	cfg  DestinationConfig
	pool *pgxpool.Config
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

// What COPY's text takes of the config, which it splices in.
var (
	roleARN    = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	keyPrefix  = regexp.MustCompile(`^[A-Za-z0-9!_.*()/-]*$`)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Schema: "public"}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("redshift: config: %w", err)
	}
	switch {
	case cfg.DSN.IsZero():
		return nil, errors.New("redshift: config needs a dsn")
	case !bucketName.MatchString(cfg.Bucket):
		return nil, fmt.Errorf("redshift: config needs a bucket, an S3 bucket name, not %q", cfg.Bucket)
	case !keyPrefix.MatchString(cfg.Prefix):
		return nil, fmt.Errorf("redshift: prefix %q has characters other than letters, digits and !-_.*()/", cfg.Prefix)
	case cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero():
		return nil, errors.New("redshift: accessKeyId and secretAccessKey go together")
	case cfg.IAMRole == "":
		return nil, errors.New("redshift: config needs iamRole: COPY reads Parquet files only as an IAM role")
	}
	if cfg.IAMRole != "default" {
		for r := range strings.SplitSeq(cfg.IAMRole, ",") {
			if !roleARN.MatchString(r) {
				return nil, fmt.Errorf("redshift: iamRole %q isn't an IAM role's ARN", r)
			}
		}
	}
	pc, err := pgxpool.ParseConfig(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("redshift: dsn: %w", err)
	}
	pc.MaxConns = 4
	// Text parameters and results, which Redshift takes for every type, and
	// no prepared statement to go stale as a column widens.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pc.AfterConnect = session
	return &Destination{cfg: cfg, pool: pc}, nil
}

// session sets what every connection needs: names as they're quoted, as
// Redshift otherwise lowercases them, and UTC, which TIMESTAMPTZ's casts
// from the staged times read them in.
func session(ctx context.Context, c *pgx.Conn) error {
	for _, q := range []string{"SET enable_case_sensitive_identifier TO true", "SET timezone TO 'UTC'"} {
		if _, err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("redshift: stream %q merges, and has no primary key", s.Name)
		}
	}
	pc := d.pool.Copy()
	if cfg.Lane > 0 {
		// Only lane 0 commits alongside its writes.
		pc.MaxConns = 1
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, classify(fmt.Errorf("redshift: connect: %w", err))
	}
	dia, err := probe(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, classify(fmt.Errorf("redshift: connect: %w", err))
	}
	st, err := d.store(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	dir, err := os.MkdirTemp("", "openetl-redshift-")
	if err != nil {
		pool.Close()
		return nil, err
	}
	w := &writer{
		d:       d,
		pool:    pool,
		store:   st,
		dir:     dir,
		dialect: dia,
		cfg:     cfg,
		schema:  cmp.Or(cfg.Namespace, d.cfg.Schema),
		Epochs:  sqlstage.NewEpochs[*stagingTable](),
		streams: map[string]destination.Stream{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

type writer struct {
	d     *Destination
	pool  *pgxpool.Pool
	store *store
	// The files of epochs not yet pre-committed.
	dir string
	dialect
	cfg     destination.OpenConfig
	schema  string
	streams map[string]destination.Stream
	sqlstage.Epochs[*stagingTable]
}

type (
	staged       = sqlstage.Staged
	stagedStream = sqlstage.Stream
)

const (
	modeOverwrite = sqlstage.ModeOverwrite
	modeMerge     = sqlstage.ModeMerge
)

const (
	idColumn      = "_openetl_id"
	opColumn      = "_openetl_op"
	seqColumn     = "_openetl_seq"
	deletedColumn = "_openetl_deleted"
	// A changelog's: the columns an update didn't send, as a JSON array,
	// whose values are null. In a merge's target, those it never received.
	unchangedColumn = "_openetl_unchanged"
	// A row's held columns, as a JSON object from each to the _openetl_id of
	// the version whose value is held. Its names are spelled by jsonName.
	heldColumn   = destination.HeldColumn
	syncedColumn = "_openetl_synced_at"
	// The engine names renamed columns' source names in each schema's
	// metadata: a JSON object from destination name to source name.
	sourceNamesKey = "openetl.source_names"
	// Each staging row's place in its table, as it was staged.
	ordColumn = "_openetl_ord"
	// Of the staging gathered for an apply: which staging table a row is
	// from, in order, and its place among its key's or its ID's versions.
	partColumn = "_openetl_part"
	rnColumn   = "_openetl_rn"

	fenceTable = "_openetl_fence"
	loadsTable = "_openetl_loads"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn, unchangedColumn, heldColumn}

// dialect is how the server reads string literals: with backslash escapes
// or without.
type dialect struct{ escapes bool }

// probe asks how the server reads a backslash in a literal.
func probe(ctx context.Context, pool *pgxpool.Pool) (dialect, error) {
	var n int
	err := pool.QueryRow(ctx, `SELECT OCTET_LENGTH('\\')`).Scan(&n)
	return dialect{escapes: n == 1}, err
}

func (d dialect) lit(s string) string {
	s = strings.ReplaceAll(s, "'", "''")
	if d.escapes {
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	return "'" + s + "'"
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func quoteAll(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return strings.Join(out, ", ")
}

func (w *writer) ident(name string) string { return quoteIdent(w.schema) + "." + quoteIdent(name) }

func hashName(prefix string, parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + hex.EncodeToString(h[:12])
}

func stagingName(pipeline, stream string, e destination.Epoch, lane int) string {
	parts := []string{pipeline, stream, strconv.FormatUint(e.Generation, 10), strconv.FormatUint(e.N, 10)}
	if lane > 0 {
		parts = append(parts, strconv.Itoa(lane))
	}
	return hashName("_openetl_stage_", parts...)
}

func patchName(pipeline, stream string, e destination.Epoch, lane int) string {
	return stagingName(pipeline, stream+"\x00patch", e, lane)
}

func readName(pipeline, stream string, start time.Time) string {
	return hashName("_openetl_read_", pipeline, stream, strconv.FormatInt(start.UnixMicro(), 10))
}

// keyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func keyColumns(schema *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := schema.Metadata().GetValue(sourceNamesKey); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", sourceNamesKey, err)
		}
		for dst, src := range names {
			renamed[src] = dst
		}
	}
	out := make([]string, len(key))
	for i, k := range key {
		out[i] = k
		if dst, ok := renamed[k]; ok {
			out[i] = dst
		}
		if len(schema.FieldIndices(out[i])) == 0 {
			return nil, fmt.Errorf("it merges on %q, which its batches don't have", k)
		}
	}
	return out, nil
}

// stagingLabel is each staging table's comment, so that Recover can find
// what earlier generations of the pipeline staged.
type stagingLabel struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
}

// readLabel is each read table's comment, so that applies and Recover can
// find reads that were abandoned.
type readLabel struct {
	Pipeline string `json:"pipeline"`
	Stream   string `json:"stream"`
	Table    string `json:"table"`
}

// columnDef is a column's definition. _openetl_deleted is false in rows from
// before it was added.
func columnDef(c column) string {
	def := quoteIdent(c.name) + " " + c.typ
	if c.name == deletedColumn {
		def += " NOT NULL DEFAULT false"
	}
	return def
}

func columnDefs(cols []column) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = columnDef(c)
	}
	return strings.Join(defs, ", ")
}

func names(cols []column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

func typeOf(cols []column, name string) string {
	if i := slices.IndexFunc(cols, func(c column) bool { return c.name == name }); i >= 0 {
		return cols[i].typ
	}
	return ""
}

// inTx runs fn in a transaction on one connection, and commits it.
func (w *writer) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return classify(err)
	}
	return classify(tx.Commit(ctx))
}

func (w *writer) exec(ctx context.Context, q string, args ...any) error {
	_, err := w.pool.Exec(ctx, q, args...)
	return classify(err)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	desc, err := w.MergeLanes("redshift", committables)
	if err != nil {
		return destination.Committed{}, err
	}
	c := destination.Committed{Collapsed: map[string]uint64{}}
	return c, w.apply(ctx, desc, desc.Streams, c.Collapsed)
}

// prepared is what a stream's apply reads before its transaction.
type prepared struct {
	// The staging's columns, at each one's widest type; none if the epoch
	// staged nothing for it.
	cols []column
	// Every lane's staging, as gather makes it.
	rows string
	// Overwrite: the read table's columns once it can take the staging's;
	// none if the read has no table.
	read []string
	// Merge: the columns an update in the staging didn't send, which the
	// apply resolves, and every name in the staging's _openetl_unchanged.
	unchanged, everUnchanged []string
	// Merge: every name in the staging's _openetl_held.
	held []string
	// The patches' staging's columns; none if there's nothing to patch.
	patch []column
	// Every lane's patches.
	patches string
}

// apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped append or overwrite stream skipped to
// collapsed.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.unapplied(ctx, desc, streams)
	if err != nil {
		return err
	}
	if len(todo) == 0 {
		// Applied, perhaps by a newer generation's Recover, which then stops
		// this one.
		return w.fencedOut(ctx)
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	if slices.ContainsFunc(todo, func(s stagedStream) bool { return s.Mode == modeOverwrite }) {
		err := w.fenced(ctx, func() error {
			return w.dropAbandonedReads(ctx, func(l readLabel, table string) bool {
				return slices.ContainsFunc(todo, func(s stagedStream) bool { return s.Mode == modeOverwrite && l.Stream == s.Name && table != s.Read })
			})
		})
		if err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, collapsed); err != nil {
		return err
	}
	for _, s := range todo {
		// The read's rows are the target's now.
		if s.Ended && s.Read != "" {
			_ = w.exec(ctx, "DROP TABLE IF EXISTS "+w.ident(s.Read))
		}
	}
	w.dropStaging(ctx, todo)
	return nil
}

// fencedOut fails if a newer generation raised the fence.
func (w *writer) fencedOut(ctx context.Context) error {
	var fence *int64
	err := w.pool.QueryRow(ctx, "SELECT MAX(generation) FROM "+w.ident(fenceTable)+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&fence)
	switch {
	case err != nil:
		return classify(err)
	case fence != nil && *fence > 0 && uint64(*fence) > w.cfg.Generation:
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, *fence, w.cfg.Generation)
	}
	return nil
}

// fence locks the fence's table in tx, which orders applies and Recover,
// and fails unless the pipeline's fence is this generation's or older. It
// writes the fence's row, so that a transaction that somehow read from
// before a newer Recover raised it fails to commit rather than applying.
func (w *writer) fence(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "LOCK "+w.ident(fenceTable)); err != nil {
		return classify(fmt.Errorf("redshift: lock fence: %w", err))
	}
	if _, err := tx.Exec(ctx, "UPDATE "+w.ident(fenceTable)+" SET generation = generation WHERE pipeline = $1", w.cfg.Pipeline); err != nil {
		return classify(fmt.Errorf("redshift: lock fence: %w", err))
	}
	var fence *int64
	if err := tx.QueryRow(ctx, "SELECT MAX(generation) FROM "+w.ident(fenceTable)+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&fence); err != nil {
		return classify(fmt.Errorf("redshift: read fence: %w", err))
	}
	switch {
	case fence == nil:
		return errors.New("redshift: the fence row is missing; Recover must run first")
	case *fence > 0 && uint64(*fence) > w.cfg.Generation:
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, *fence, w.cfg.Generation)
	}
	return nil
}

// fenced runs fn, which may run DDL on other connections, while a
// transaction holds the fence, so that a newer generation's Recover waits
// for it and a fenced writer can't run it. fn must not touch the fence's
// table.
func (w *writer) fenced(ctx context.Context, fn func() error) error {
	return w.inTx(ctx, func(tx pgx.Tx) error {
		if err := w.fence(ctx, tx); err != nil {
			return err
		}
		return fn()
	})
}

func (w *writer) marked(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, desc staged, stream string) (bool, error) {
	var done int64
	err := q.QueryRow(ctx, "SELECT COUNT(*) FROM "+w.ident(loadsTable)+" WHERE pipeline = $1 AND stream = $2 AND generation = $3 AND epoch = $4",
		w.cfg.Pipeline, stream, desc.Generation, desc.Epoch).Scan(&done)
	if err != nil {
		return false, classify(fmt.Errorf("redshift: read marker for %s: %w", stream, err))
	}
	return done > 0, nil
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, collapsed map[string]uint64) error {
	skipped := map[string]uint64{}
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		if err := w.fence(ctx, tx); err != nil {
			return err
		}
		for _, s := range streams {
			if done, err := w.marked(ctx, tx, desc, s.Name); err != nil || done {
				if err != nil {
					return err
				}
				continue
			}
			n, err := w.applyStream(ctx, tx, s, prep[s.Name])
			if err != nil {
				return classify(fmt.Errorf("redshift: apply %s: %w", s.Name, err))
			}
			// Dedupe skips append and overwrite rows; a merge's apply counts
			// none of the rows it applies.
			if s.Deduped() && s.Mode != modeMerge && s.Rows > n {
				skipped[s.Name] = s.Rows - n
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(loadsTable)+" (pipeline, stream, generation, epoch, loaded_at) VALUES ($1, $2, $3, $4, GETDATE())",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
				return classify(fmt.Errorf("redshift: write marker for %s: %w", s.Name, err))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for name, n := range skipped {
		collapsed[name] += n
	}
	return nil
}

// applyStream applies one stream's staging in tx, and returns the rows it
// inserted.
func (w *writer) applyStream(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) (uint64, error) {
	n, err := w.applyRows(ctx, tx, s, p)
	if err != nil {
		return 0, err
	}
	for _, q := range w.patch(s, p) {
		if _, err := tx.Exec(ctx, q); err != nil {
			return 0, fmt.Errorf("patch: %w", err)
		}
	}
	return n, nil
}

func (w *writer) applyRows(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) (uint64, error) {
	if s.Mode == modeMerge {
		if len(s.Stagings) == 0 {
			return 0, nil
		}
		for _, q := range w.merge(s, p) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	var n uint64
	if len(s.Stagings) > 0 {
		tag, err := tx.Exec(ctx, w.insert(s, p))
		if err != nil {
			return 0, err
		}
		n = uint64(max(tag.RowsAffected(), 0))
	}
	if s.Ended {
		return n, w.replace(ctx, tx, s, p.read)
	}
	return n, nil
}

// keyEq matches rows of a and b by key: Redshift compares text without its
// trailing blanks, so text keys must have as many bytes too.
func keyEq(a, b string, key []string, cols []column) string {
	out := make([]string, 0, len(key))
	for _, k := range key {
		c := quoteIdent(k)
		eq := a + "." + c + " = " + b + "." + c
		if isText(typeOf(cols, k)) {
			eq += " AND OCTET_LENGTH(" + a + "." + c + ") = OCTET_LENGTH(" + b + "." + c + ")"
		}
		out = append(out, eq)
	}
	return strings.Join(out, " AND ")
}

// keyParts partitions or groups rel's rows by key, as keyEq matches them.
func keyParts(rel string, key []string, cols []column) string {
	out := make([]string, 0, len(key))
	for _, k := range key {
		c := rel + "." + quoteIdent(k)
		out = append(out, c)
		if isText(typeOf(cols, k)) {
			out = append(out, "OCTET_LENGTH("+c+")")
		}
	}
	return strings.Join(out, ", ")
}

func isText(typ string) bool {
	return strings.HasPrefix(typ, "varchar") || strings.HasPrefix(typ, "char")
}

// latestFirst orders rel's rows from the last staged, by _openetl_seq if the
// staging has it, nulls last.
func latestFirst(rel string, hasSeq bool) string {
	order := rel + "." + partColumn + " DESC, " + rel + "." + quoteIdent(ordColumn) + " DESC"
	if hasSeq {
		seq := rel + "." + quoteIdent(seqColumn)
		order = "CASE WHEN " + seq + " IS NULL THEN 1 ELSE 0 END, " + seq + " DESC, " + order
	}
	return order
}

// listHas is whether the JSON array expr, as nameList spells it, has name;
// false for null.
func (d dialect) listHas(expr, name string) string {
	return "coalesce(STRPOS(" + expr + ", " + d.lit(jsonName(name)) + ") > 0, false)"
}

// heldHas is whether the object expr, as heldObject spells it, holds name.
func (d dialect) heldHas(expr, name string) string {
	return "coalesce(STRPOS(" + expr + ", " + d.lit(jsonName(name)+":") + ") > 0, false)"
}

// heldID is the version id the object expr holds name at, where it does.
func (d dialect) heldID(expr, name string) string {
	key := jsonName(name) + `:"`
	return "SUBSTRING(" + expr + ", STRPOS(" + expr + ", " + d.lit(key) + ") + " + strconv.Itoa(len(key)) + ", 36)"
}

// member is an object member's text, name to the id v; null if v is.
func (d dialect) member(name, v string) string {
	return d.lit(jsonName(name)+`:"`) + " || " + v + ` || '"'`
}

// jsonOf builds a JSON array or object, as text, from parts that are each an
// element's or a member's text, or null to leave it out; null if all are.
func jsonOf(open, close string, parts []string) string {
	if len(parts) == 0 {
		return "CAST(NULL AS " + varcharMax + ")"
	}
	cat := make([]string, len(parts))
	for i, p := range parts {
		cat[i] = "coalesce(',' || " + p + ", '')"
	}
	return "NULLIF('" + open + "' || LTRIM(" + strings.Join(cat, " || ") + ", ',') || '" + close + "', '" + open + close + "')"
}

func selectList(rel string, cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = rel + "." + quoteIdent(c)
	}
	return strings.Join(out, ", ")
}

// insert copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, the first staged, and with
// Dedupe only IDs the target doesn't have.
func (w *writer) insert(s stagedStream, p prepared) string {
	cols := names(p.cols)
	into := "INSERT INTO " + w.ident(s.Into()) + " (" + quoteAll(cols) + ") "
	if !s.Deduped() {
		return into + "SELECT " + selectList("s", cols) + " FROM " + p.rows + " s"
	}
	id := quoteIdent(idColumn)
	q := into + "SELECT " + selectList("s", cols) + " FROM (SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x." + id + " ORDER BY x." + partColumn + ", x." + quoteIdent(ordColumn) +
		") AS " + rnColumn + " FROM " + p.rows + " x) s WHERE s." + rnColumn + " = 1"
	if s.Dedupe {
		q += " AND NOT EXISTS (SELECT 1 FROM " + w.ident(s.Into()) + " t WHERE t." + id + " = s." + id + ")"
	}
	return q
}

// patch fills held cells from the epoch's patches: each column where the
// target still holds the version the patch comes from, matched by key for
// merge and by _openetl_id for append. Of a row's patches from one version,
// the last staged fills it.
func (w *writer) patch(s stagedStream, p prepared) []string {
	if len(p.patch) == 0 {
		return nil
	}
	q := quoteIdent
	cols := names(p.patch)
	match := []string{idColumn}
	if s.Mode == modeMerge {
		match = s.Key
	}
	parts := keyParts("y", match, p.patch)
	if !slices.Contains(match, idColumn) {
		parts += ", y." + q(idColumn)
	}
	t := w.ident(s.Table)
	heldOf := func(c string) string {
		if f, ok := s.Fills[c]; ok {
			return f
		}
		return c
	}
	// The last column that fills each held column, whose UPDATE takes it out
	// of the target's _openetl_held: an UPDATE before it needs it there.
	last := map[string]string{}
	for _, c := range cols {
		if !slices.Contains(metadataColumns, c) && !slices.Contains(s.Key, c) {
			last[heldOf(c)] = c
		}
	}
	var out []string
	for _, c := range cols {
		if slices.Contains(metadataColumns, c) || slices.Contains(s.Key, c) {
			continue
		}
		held := heldOf(c)
		// The member the patch fills, which its guard pins to its version.
		m := w.lit(jsonName(held)+`:"`) + " || p." + q(idColumn) + ` || '"'`
		h := t + "." + q(heldColumn)
		set := q(c) + " = p." + q(c)
		if last[held] == c {
			set += ", " + q(heldColumn) + " = NULLIF(REPLACE(REPLACE(REPLACE(" + h + ", ',' || " + m + ", ''), " + m + " || ',', ''), " + m + ", ''), '{}')"
		}
		if slices.Contains(cols, syncedColumn) {
			set += ", " + q(syncedColumn) + " = p." + q(syncedColumn)
		}
		from := "(SELECT y.*, ROW_NUMBER() OVER (PARTITION BY " + parts + " ORDER BY y." + partColumn + " DESC, y." + q(ordColumn) + " DESC) AS " + rnColumn +
			" FROM " + p.patches + " y WHERE " + w.heldHas("y."+q(heldColumn), held) + ")"
		out = append(out, "UPDATE "+t+" SET "+set+" FROM "+from+" p WHERE p."+rnColumn+" = 1 AND "+keyEq(t, "p", match, p.patch)+
			" AND "+w.heldHas(h, held)+" AND "+w.heldID(h, held)+" = p."+q(idColumn))
	}
	return out
}

// Of a merge: each key's versions, its picks, and the rows it works out.
const (
	mergeVersions = "_openetl_versions"
	mergePicks    = "_openetl_picks"
	mergeRows     = "_openetl_merge"
)

func pick(i int) string { return "_openetl_pick_" + strconv.Itoa(i) }

// merge upserts each key's latest staged version, by _openetl_seq and then
// staging order, unless the target's is newer. A delete keeps the row's
// other columns as they were, with its metadata columns updated, or with
// hard deletes removes the row. A column in unchanged that the latest
// version didn't send takes its value from the key's latest staged version
// that did, unless the target's row is newer than that version, or else
// keeps the target's.
//
// Each row's values are worked out against the target's row, joined as cur,
// into a temporary table that the update, insert and delete then take them
// from.
func (w *writer) merge(s stagedStream, p prepared) []string {
	cols := names(p.cols)
	q := quoteIdent
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	stmts := []string{
		"CREATE TEMP TABLE " + q(mergeVersions) + " AS SELECT x.*, ROW_NUMBER() OVER (PARTITION BY " + keyParts("x", s.Key, p.cols) +
			" ORDER BY " + latestFirst("x", hasSeq) + ") AS " + rnColumn + " FROM " + p.rows + " x",
	}
	latest := w.latest(s, p, &stmts)

	isDelete := "false"
	if hasOp {
		isDelete = "coalesce(s." + q(opColumn) + " = 'delete', false)"
	}
	// Unless the target's version is newer; a missing sequence doesn't
	// stop an update.
	newer := "true"
	if hasSeq {
		newer = "NOT coalesce(cur." + q(seqColumn) + " > s." + q(seqColumn) + ", false)"
	}
	found := "cur." + q(s.Key[0]) + " IS NOT NULL"
	exprs := make([]string, len(cols))
	var sets []string
	for i, c := range cols {
		v := "s." + q(c)
		if slices.Contains(s.Key, c) {
			exprs[i] = v + " AS " + q(c)
			continue
		}
		sets = append(sets, q(c)+" = m."+q(c))
		switch {
		case c == unchangedColumn:
			// The columns neither ever sent.
			parts := make([]string, len(p.everUnchanged))
			for j, n := range p.everUnchanged {
				parts[j] = "CASE WHEN " + w.listHas("s."+q(c), n) + " AND " + w.listHas("cur."+q(c), n) + " THEN " + w.lit(jsonName(n)) + " END"
			}
			v = "CASE WHEN s." + q(c) + " IS NULL OR cur." + q(c) + " IS NULL THEN NULL ELSE " + jsonOf("[", "]", parts) + " END"
		case c == heldColumn && hasHeld:
			// The row's, and the target's for the columns it didn't send.
			var parts []string
			for _, n := range slices.Compact(slices.Sorted(slices.Values(slices.Concat(p.held, p.everUnchanged)))) {
				part := "CASE WHEN " + w.heldHas("s."+q(c), n) + " THEN " + w.member(n, w.heldID("s."+q(c), n))
				if slices.Contains(cols, unchangedColumn) {
					part += " WHEN " + w.listHas("s."+q(unchangedColumn), n) + " AND " + w.heldHas("cur."+q(c), n) + " THEN " + w.member(n, w.heldID("cur."+q(c), n))
				}
				parts = append(parts, part+" END")
			}
			v = jsonOf("{", "}", parts)
		case slices.Contains(p.unchanged, c):
			v = "CASE WHEN " + w.listHas("s."+q(unchangedColumn), c) + " THEN cur." + q(c) + " ELSE " + v + " END"
		}
		if hasHeld && slices.Contains(s.Last, c) && !slices.Contains(metadataColumns, c) {
			v = "CASE WHEN " + w.heldHas("s."+q(heldColumn), c) + " THEN cur." + q(c) + " ELSE " + v + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metadataColumns, c) || c == unchangedColumn || c == heldColumn) {
			v = "CASE WHEN " + isDelete + " THEN cur." + q(c) + " ELSE " + v + " END"
		}
		exprs[i] = "CASE WHEN " + found + " THEN " + v + " ELSE s." + q(c) + " END AS " + q(c)
	}
	stmts = append(stmts, "CREATE TEMP TABLE "+q(mergeRows)+" AS SELECT "+strings.Join(exprs, ", ")+
		", "+found+" AS _openetl_found, "+newer+" AS _openetl_newer, "+isDelete+" AS _openetl_delete FROM "+latest+
		" s LEFT JOIN "+w.ident(s.Table)+" cur ON "+keyEq("cur", "s", s.Key, p.cols))

	t := w.ident(s.Table)
	match := keyEq(t, "m", s.Key, p.cols)
	update, insert := "m._openetl_found AND m._openetl_newer", "NOT m._openetl_found"
	if hasOp && s.HardDeletes {
		update, insert = update+" AND NOT m._openetl_delete", insert+" AND NOT m._openetl_delete"
	}
	if len(sets) > 0 {
		stmts = append(stmts, "UPDATE "+t+" SET "+strings.Join(sets, ", ")+" FROM "+q(mergeRows)+" m WHERE "+match+" AND "+update)
	}
	stmts = append(stmts, "INSERT INTO "+t+" ("+quoteAll(cols)+") SELECT "+selectList("m", cols)+" FROM "+q(mergeRows)+" m WHERE "+insert)
	if hasOp && s.HardDeletes {
		stmts = append(stmts, "DELETE FROM "+t+" USING "+q(mergeRows)+" m WHERE "+match+" AND m._openetl_found AND m._openetl_newer AND m._openetl_delete")
	}
	for _, tmp := range []string{mergeRows, mergePicks, mergeVersions} {
		stmts = append(stmts, "DROP TABLE IF EXISTS "+q(tmp))
	}
	return stmts
}

// latest is merge's relation of each key's latest staged version, from its
// versions, ranked by latestFirst. With unchanged columns, each comes from
// the key's latest version that sent it and isn't older than the target's
// row, if any, which mergePicks finds; its _openetl_unchanged then leaves it
// out. It adds the statements that
// make mergePicks to stmts.
func (w *writer) latest(s stagedStream, p prepared, stmts *[]string) string {
	cols := names(p.cols)
	q := quoteIdent
	from := q(mergeVersions) + " l"
	if len(p.unchanged) > 0 {
		// A version older than the target's row doesn't give a column: the
		// target's value is newer. A missing seq doesn't stop it, as in merge.
		versions, newer := q(mergeVersions)+" x", ""
		if slices.Contains(cols, seqColumn) {
			versions += " LEFT JOIN " + w.ident(s.Table) + " cur ON " + keyEq("cur", "x", s.Key, p.cols)
			newer = " AND NOT coalesce(cur." + q(seqColumn) + " > x." + q(seqColumn) + ", false)"
		}
		picks := make([]string, len(p.unchanged))
		for i, c := range p.unchanged {
			picks[i] = "MIN(CASE WHEN NOT " + w.listHas("x."+q(unchangedColumn), c) + newer + " THEN x." + rnColumn + " END) AS " + pick(i)
		}
		*stmts = append(*stmts, "CREATE TEMP TABLE "+q(mergePicks)+" AS SELECT "+selectList("x", s.Key)+", "+strings.Join(picks, ", ")+
			" FROM "+versions+" GROUP BY "+keyParts("x", s.Key, p.cols))
		from += " JOIN " + q(mergePicks) + " k ON " + keyEq("k", "l", s.Key, p.cols)
		for i := range p.unchanged {
			e := "e" + strconv.Itoa(i)
			from += " LEFT JOIN " + q(mergeVersions) + " " + e + " ON " + keyEq(e, "l", s.Key, p.cols) + " AND " + e + "." + rnColumn + " = k." + pick(i)
		}
	}
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
		if len(p.unchanged) == 0 {
			continue
		}
		switch j := slices.Index(p.unchanged, c); {
		case j >= 0:
			sel[i] = "CASE WHEN k." + pick(j) + " IS NOT NULL THEN e" + strconv.Itoa(j) + "." + q(c) + " ELSE l." + q(c) + " END AS " + q(c)
		case c == unchangedColumn:
			parts := make([]string, len(p.everUnchanged))
			for k, n := range p.everUnchanged {
				cond := w.listHas("l."+q(c), n)
				if j := slices.Index(p.unchanged, n); j >= 0 {
					cond = "k." + pick(j) + " IS NULL AND " + cond
				}
				parts[k] = "CASE WHEN " + cond + " THEN " + w.lit(jsonName(n)) + " END"
			}
			sel[i] = jsonOf("[", "]", parts) + " AS " + q(c)
		case c == heldColumn && hasHeld:
			// The version a column comes from may hold it.
			var parts []string
			for _, n := range slices.Compact(slices.Sorted(slices.Values(slices.Concat(p.held, p.unchanged)))) {
				own := "CASE WHEN " + w.heldHas("l."+q(c), n) + " THEN " + w.heldID("l."+q(c), n) + " END"
				v := own
				if j := slices.Index(p.unchanged, n); j >= 0 {
					e := "e" + strconv.Itoa(j) + "." + q(c)
					v = "coalesce(CASE WHEN " + w.heldHas(e, n) + " THEN " + w.heldID(e, n) + " END, " + own + ")"
				}
				parts = append(parts, w.member(n, v))
			}
			sel[i] = jsonOf("{", "}", parts) + " AS " + q(c)
		}
	}
	return "(SELECT " + strings.Join(sel, ", ") + " FROM " + from + " WHERE l." + rnColumn + " = 1)"
}

// replace makes the read's rows the target's. Readers see the old rows
// until the transaction commits; apply drops the read's table after it.
// DELETE, as TRUNCATE would commit the transaction.
func (w *writer) replace(ctx context.Context, tx pgx.Tx, s stagedStream, read []string) error {
	switch {
	case s.Table == "" && len(read) > 0:
		return fmt.Errorf("its read has rows in %s, and no target table", s.Read)
	case s.Table == "":
		// The stream never had a schema, so it has no table to empty.
		return nil
	}
	have, err := w.catalogIn(ctx, tx, []string{s.Table})
	if err != nil || have[s.Table] == nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+w.ident(s.Table)); err != nil {
		return err
	}
	if len(read) == 0 {
		return nil
	}
	list := quoteAll(read)
	_, err = tx.Exec(ctx, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+list+" FROM "+w.ident(s.Read))
	return err
}

// prepare makes the tables a stream's apply writes hold the staging's
// columns, and reads the names its held and unchanged columns hold.
func (w *writer) prepare(ctx context.Context, desc staged, s stagedStream) (prepared, error) {
	var p prepared
	var cols []column
	if len(s.Stagings) > 0 {
		var err error
		if cols, p.rows, err = w.gather(ctx, s.Stagings, nil); err != nil {
			return p, err
		}
		if len(cols) == 0 {
			// A newer generation's Recover drops what older ones staged.
			if err := w.fencedOut(ctx); err != nil {
				return p, err
			}
			return p, fmt.Errorf("redshift: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
		}
		p.cols = cols
	}
	if s.Mode == modeOverwrite {
		if cols != nil {
			if err := w.ensureRead(ctx, s, cols); err != nil {
				return p, err
			}
		}
		have, err := w.columnTypes(ctx, s.Read)
		if err != nil {
			return p, err
		}
		p.read = names(have)
		if s.Ended && len(have) > 0 && s.Table != "" {
			if err := w.ensureTable(ctx, s.Name, s.Table, have); err != nil {
				return p, err
			}
		}
	} else if cols != nil {
		if err := w.ensureTable(ctx, s.Name, s.Table, cols); err != nil {
			return p, err
		}
	}
	if cols != nil {
		// At the types of the table they go into, which may be wider.
		into, err := w.columnTypes(ctx, s.Into())
		if err != nil {
			return p, err
		}
		if cols, p.rows, err = w.gather(ctx, s.Stagings, into); err != nil {
			return p, err
		}
		p.cols = cols
	}
	if len(s.Patches) > 0 {
		var err error
		if p.patch, p.patches, err = w.preparePatch(ctx, s); err != nil {
			return p, err
		}
	}
	if s.Mode == modeMerge && cols != nil {
		var err error
		if p.held, p.everUnchanged, err = w.heldNames(ctx, s, p); err != nil {
			return p, err
		}
		p.unchanged = slices.DeleteFunc(slices.Clone(p.everUnchanged), func(c string) bool {
			return typeOf(cols, c) == "" || slices.Contains(s.Key, c) || slices.Contains(metadataColumns, c)
		})
	}
	return p, nil
}

// heldNames returns the names the staging's _openetl_held and
// _openetl_unchanged hold, of the columns the staging and the target have.
func (w *writer) heldNames(ctx context.Context, s stagedStream, p prepared) (held, unchanged []string, err error) {
	hasHeld, hasUnchanged := typeOf(p.cols, heldColumn) != "", typeOf(p.cols, unchangedColumn) != ""
	if !hasHeld && !hasUnchanged {
		return nil, nil, nil
	}
	target, err := w.columnTypes(ctx, s.Table)
	if err != nil {
		return nil, nil, err
	}
	candidates := slices.Compact(slices.Sorted(slices.Values(slices.Concat(names(p.cols), names(target)))))
	candidates = slices.DeleteFunc(candidates, func(c string) bool { return slices.Contains(metadataColumns, c) })
	if len(candidates) == 0 {
		return nil, nil, nil
	}
	var aggs []string
	for _, c := range candidates {
		if hasHeld {
			aggs = append(aggs, "coalesce(MAX(CASE WHEN "+w.heldHas("x."+quoteIdent(heldColumn), c)+" THEN 1 ELSE 0 END), 0)")
		}
		if hasUnchanged {
			aggs = append(aggs, "coalesce(MAX(CASE WHEN "+w.listHas("x."+quoteIdent(unchangedColumn), c)+" THEN 1 ELSE 0 END), 0)")
		}
	}
	found := make([]int32, len(aggs))
	dest := make([]any, len(aggs))
	for i := range found {
		dest[i] = &found[i]
	}
	if err := w.pool.QueryRow(ctx, "SELECT "+strings.Join(aggs, ", ")+" FROM "+p.rows+" x").Scan(dest...); err != nil {
		return nil, nil, classify(fmt.Errorf("redshift: read the held and unchanged columns of %s's staging: %w", s.Name, err))
	}
	i := 0
	for _, c := range candidates {
		if hasHeld {
			if found[i] == 1 {
				held = append(held, c)
			}
			i++
		}
		if hasUnchanged {
			if found[i] == 1 {
				unchanged = append(unchanged, c)
			}
			i++
		}
	}
	return held, unchanged, nil
}

// gather returns the columns of tables, each a staging of one stream, at
// each column's widest type, or into's where that's wider, and a relation of
// every one's rows, in order, each with its table's place in tables as
// _openetl_part. No columns if one of them doesn't exist.
func (w *writer) gather(ctx context.Context, tables []string, into []column) ([]column, string, error) {
	have, err := w.catalog(ctx, tables)
	if err != nil {
		return nil, "", err
	}
	each := make([][]column, len(tables))
	var cols []column
	for i, t := range tables {
		if have[t] == nil {
			return nil, "", nil
		}
		for _, tc := range have[t].cols {
			if tc.name == ordColumn {
				continue
			}
			c := tc.column
			each[i] = append(each[i], c)
			switch j := slices.IndexFunc(cols, func(x column) bool { return x.name == c.name }); {
			case j < 0:
				cols = append(cols, c)
			case widens(c.typ, cols[j].typ):
			case widens(cols[j].typ, c.typ):
				cols[j] = c
			default:
				return nil, "", fmt.Errorf("redshift: column %q is %s in one staging table, and %s in another", c.name, cols[j].typ, c.typ)
			}
		}
	}
	for j, c := range cols {
		if to := typeOf(into, c.name); to != "" && widens(c.typ, to) {
			cols[j].typ = to
		}
	}
	selects := make([]string, len(tables))
	for i, t := range tables {
		sel := make([]string, 0, len(cols)+2)
		for _, c := range cols {
			as := " AS " + quoteIdent(c.name)
			switch from := typeOf(each[i], c.name); {
			case from == "" && c.name == deletedColumn:
				sel = append(sel, "false"+as)
			case c.name == deletedColumn:
				// The target's is NOT NULL.
				sel = append(sel, "coalesce("+quoteIdent(c.name)+", false)"+as)
			case from == "":
				sel = append(sel, "CAST(NULL AS "+c.typ+")"+as)
			default:
				sel = append(sel, castTo(quoteIdent(c.name), from, c.typ)+as)
			}
		}
		sel = append(sel, strconv.Itoa(i)+" AS "+partColumn, quoteIdent(ordColumn))
		selects[i] = "SELECT " + strings.Join(sel, ", ") + " FROM " + w.ident(t)
	}
	return cols, "(" + strings.Join(selects, " UNION ALL ") + ")", nil
}

// preparePatch returns the columns of the stream's patches, none if its
// target doesn't exist, and _openetl_synced_at only if the target has it.
func (w *writer) preparePatch(ctx context.Context, s stagedStream) ([]column, string, error) {
	target, err := w.columnTypes(ctx, s.Table)
	if err != nil {
		return nil, "", err
	}
	cols, rows, err := w.gather(ctx, s.Patches, target)
	if err != nil {
		return nil, rows, err
	}
	if len(cols) == 0 {
		return nil, rows, fmt.Errorf("redshift: patches for %s are missing", s.Name)
	}
	if len(target) == 0 {
		return nil, rows, nil
	}
	if typeOf(target, syncedColumn) == "" {
		cols = slices.DeleteFunc(cols, func(c column) bool { return c.name == syncedColumn })
	}
	return cols, rows, nil
}

// createLabelled creates a table with its label as its comment, in one
// transaction.
func (w *writer) createLabelled(ctx context.Context, name string, label any, cols []column) error {
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(name)+" ("+columnDefs(cols)+")"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "COMMENT ON TABLE "+w.ident(name)+" IS "+w.lit(string(b)))
		return err
	})
}

// ensureRead creates the read's table, labelled, with the target's types
// where it has the column, or adds its columns to hold cols. A column it has
// narrower than the target's takes the target's type: only a person widens a
// target, after ApplySchema refused to and said how, and the read's table
// replaces the target's rows.
func (w *writer) ensureRead(ctx context.Context, s stagedStream, cols []column) error {
	tables := []string{s.Read}
	if s.Table != "" {
		tables = append(tables, s.Table)
	}
	info, err := w.catalog(ctx, tables)
	if err != nil {
		return err
	}
	var target []column
	if t := info[s.Table]; t != nil && s.Table != "" {
		for _, c := range t.cols {
			target = append(target, c.column)
		}
	}
	targetType := func(c column) column {
		if to := typeOf(target, c.name); to != "" && widens(c.typ, to) {
			c.typ = to
		}
		return c
	}
	have := info[s.Read]
	if have == nil {
		want := make([]column, len(cols))
		for i, c := range cols {
			want[i] = targetType(c)
		}
		if err := w.createLabelled(ctx, s.Read, readLabel{Pipeline: w.cfg.Pipeline, Stream: s.Name, Table: s.Table}, want); err != nil {
			return fmt.Errorf("redshift: create the read table for %s: %w", s.Name, err)
		}
		return nil
	}
	// A copy: retype moves the column it changes to the end of have.cols.
	for _, c := range slices.Clone(have.cols) {
		if t := targetType(c.column); t.typ != c.typ {
			if err := w.retype(ctx, s.Read, have, c, t.typ); err != nil {
				return fmt.Errorf("redshift: column %q of the read table for %s, as its target has it: %w", c.name, s.Name, err)
			}
		}
	}
	return w.ensureTable(ctx, s.Name, s.Read, cols)
}

// ApplySchema creates each stream's table and adds its new columns. A
// column it can't add, or whose type changed, is refused, and the rest
// applied. It reports what each table can't take.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var out destination.SchemaApplied
	if err := w.ensureSchema(ctx); err != nil {
		return out, err
	}
	tables := make([]string, len(streams))
	want := make([][]column, len(streams))
	for i, s := range streams {
		cols, err := columnsOf(s.Schema)
		if err != nil {
			return out, err
		}
		tables[i], want[i] = targetTable(s.Schema, s.Name), cols
	}
	have, err := w.catalog(ctx, tables)
	if err != nil {
		return out, err
	}
	out.Limits = make(map[string]destination.TableLimits, len(streams))
	for i, s := range streams {
		t := have[tables[i]]
		refused, err := w.alter(ctx, tables[i], want[i], t, w.streams[s.Name].AllowTypeChanges)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		for _, r := range refused {
			r.Stream = s.Name
			out.Refused = append(out.Refused, r)
		}
		if t == nil {
			t = &tableInfo{}
			for _, c := range want[i] {
				t.cols = append(t.cols, tableColumn{column: c})
			}
		}
		out.Limits[s.Name] = t.limits()
		w.SetTable(s.Name, tables[i])
	}
	return out, nil
}

// ensureTable creates stream's table with cols, or adds its columns to hold
// them. A column already wider is left alone.
func (w *writer) ensureTable(ctx context.Context, stream, table string, cols []column) error {
	have, err := w.catalog(ctx, []string{table})
	if err != nil {
		return err
	}
	refused, err := w.alter(ctx, table, cols, have[table], w.streams[stream].AllowTypeChanges)
	if err != nil {
		return err
	}
	if len(refused) > 0 {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf("redshift: column %q of %s: %s", refused[0].Column, table, refused[0].Reason))
	}
	return nil
}

// Redshift's limit on a table's columns.
const maxColumns = 1600

type tableColumn struct {
	column
	notNull bool
	// The table's distribution key, or one of its sort keys, whose type
	// can't change: such a column can't be dropped.
	key bool
}

// tableInfo is a table as the catalog has it.
type tableInfo struct {
	cols []tableColumn
}

func (t *tableInfo) limits() destination.TableLimits {
	l := destination.TableLimits{MaxColumns: maxColumns}
	if t == nil {
		return l
	}
	l.Columns = len(t.cols)
	for _, c := range t.cols {
		if slices.Contains(metadataColumns, c.name) {
			continue
		}
		if c.notNull {
			l.NotNull = append(l.NotNull, c.name)
		}
		if c.key {
			l.Fixed = append(l.Fixed, c.name)
		}
	}
	return l
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (w *writer) catalog(ctx context.Context, tables []string) (map[string]*tableInfo, error) {
	return w.catalogIn(ctx, w.pool, tables)
}

// catalogIn reads the tables' columns in one query: their types, which can't
// take a null, and which are keys. A table that doesn't exist is missing.
func (w *writer) catalogIn(ctx context.Context, db queryer, tables []string) (map[string]*tableInfo, error) {
	out := map[string]*tableInfo{}
	if len(tables) == 0 {
		return out, nil
	}
	args := []any{w.schema}
	marks := make([]string, len(tables))
	for i, t := range tables {
		args = append(args, t)
		marks[i] = "$" + strconv.Itoa(i+2)
	}
	rows, err := db.Query(ctx, `SELECT c.relname::varchar, a.attname::varchar, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attisdistkey OR a.attsortkeyord <> 0
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname IN (`+strings.Join(marks, ", ")+`) AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`, args...)
	if err != nil {
		return nil, classify(fmt.Errorf("redshift: read the columns of %s: %w", strings.Join(tables, ", "), err))
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		var c tableColumn
		if err := rows.Scan(&table, &c.name, &c.typ, &c.notNull, &c.key); err != nil {
			return nil, err
		}
		c.typ = normType(c.typ)
		t := out[table]
		if t == nil {
			t = &tableInfo{}
			out[table] = t
		}
		t.cols = append(t.cols, c)
	}
	return out, classify(rows.Err())
}

// columnTypes returns a table's columns in order but _openetl_ord, or none
// if it doesn't exist.
func (w *writer) columnTypes(ctx context.Context, table string) ([]column, error) {
	if table == "" {
		return nil, nil
	}
	have, err := w.catalog(ctx, []string{table})
	if err != nil || have[table] == nil {
		return nil, err
	}
	var cols []column
	for _, c := range have[table].cols {
		if c.name != ordColumn {
			cols = append(cols, c.column)
		}
	}
	return cols, nil
}

// retypeName is the column retype copies a column's values into.
func retypeName(column string) string { return hashName("_openetl_retype_", column) }

// retypeSQL changes a column's type, which Redshift's ALTER COLUMN does only
// for a VARCHAR's length and outside a transaction: it adds a column of the
// new type, copies the values into it, drops the old one and renames the new
// one to its name, which moves it to the end of the table.
func (w *writer) retypeSQL(table, col, from, to string) []string {
	t, c, tmp := w.ident(table), quoteIdent(col), quoteIdent(retypeName(col))
	return []string{
		"ALTER TABLE " + t + " ADD COLUMN " + tmp + " " + to,
		"UPDATE " + t + " SET " + tmp + " = " + castTo(c, from, to),
		"ALTER TABLE " + t + " DROP COLUMN " + c,
		"ALTER TABLE " + t + " RENAME COLUMN " + tmp + " TO " + c,
	}
}

// retype runs retypeSQL in one transaction, after finishing or undoing an
// earlier one that didn't end; have is updated to match.
func (w *writer) retype(ctx context.Context, table string, have *tableInfo, c tableColumn, to string) error {
	tmp := retypeName(c.name)
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		if slices.ContainsFunc(have.cols, func(x tableColumn) bool { return x.name == tmp }) {
			if _, err := tx.Exec(ctx, "ALTER TABLE "+w.ident(table)+" DROP COLUMN "+quoteIdent(tmp)); err != nil {
				return err
			}
		}
		for _, q := range w.retypeSQL(table, c.name, c.typ, to) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	have.cols = slices.DeleteFunc(have.cols, func(x tableColumn) bool { return x.name == c.name || x.name == tmp })
	c.typ = to
	have.cols = append(have.cols, c)
	return nil
}

// alter creates the table with cols if it doesn't exist, or adds its new
// columns, one statement each, and returns the columns it refused, with
// why. It changes a column's type only if the stream allows type changes,
// as that rewrites the column. have is updated to match.
func (w *writer) alter(ctx context.Context, table string, cols []column, have *tableInfo, allow bool) ([]destination.RefusedColumn, error) {
	if have == nil {
		if err := w.exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(table)+" ("+columnDefs(cols)+")"); err != nil {
			return nil, fmt.Errorf("redshift: create %s: %w", table, err)
		}
		return nil, nil
	}
	var refused []destination.RefusedColumn
	full := func(c column, why string) destination.RefusedColumn {
		return destination.RefusedColumn{Column: c.name, Reason: "it takes no more columns: " + why, ToType: c.typ}
	}
	var add []column
	type change struct {
		have tableColumn
		to   string
	}
	var widen []change
	n := len(have.cols)
	for _, c := range cols {
		i := slices.IndexFunc(have.cols, func(x tableColumn) bool { return x.name == c.name })
		switch {
		case i < 0 && n >= maxColumns:
			refused = append(refused, full(c, fmt.Sprintf("the table has the %d columns Redshift takes", maxColumns)))
		case i < 0:
			add = append(add, c)
			n++
		case have.cols[i].typ == c.typ || widens(c.typ, have.cols[i].typ):
		case !widens(have.cols[i].typ, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it can't change a column from %s to %s", have.cols[i].typ, c.typ), FromType: have.cols[i].typ, ToType: c.typ,
			})
		case have.cols[i].key || have.cols[i].notNull:
			why := "it's the table's distribution key or one of its sort keys, which Redshift can't drop to change its type"
			fix := "ALTER TABLE " + w.ident(table) + " ALTER DISTSTYLE EVEN; ALTER TABLE " + w.ident(table) + " ALTER SORTKEY NONE; "
			if have.cols[i].notNull {
				why, fix = "it's NOT NULL, which a column Redshift adds to change its type can't be without a default", ""
			}
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: why, FromType: have.cols[i].typ, ToType: c.typ, Fix: fix + strings.Join(w.retypeSQL(table, c.name, have.cols[i].typ, c.typ), "; ") + ";",
			})
		case allow:
			widen = append(widen, change{have.cols[i], c.typ})
		default:
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it, as Redshift rewrites the column, which moves to the table's end",
				FromType: have.cols[i].typ, ToType: c.typ, Fix: strings.Join(w.retypeSQL(table, c.name, have.cols[i].typ, c.typ), "; ") + ";",
			})
		}
	}
	for _, ch := range widen {
		if err := w.retype(ctx, table, have, ch.have, ch.to); err != nil {
			return nil, fmt.Errorf("redshift: change the type of column %q of %s to %s: %w", ch.have.name, table, ch.to, err)
		}
	}
	for _, c := range add {
		err := w.exec(ctx, "ALTER TABLE "+w.ident(table)+" ADD COLUMN "+columnDef(c))
		if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "54011" {
			refused = append(refused, full(c, pe.Message))
			continue
		} else if err != nil {
			return nil, fmt.Errorf("redshift: add column %q to %s: %w", c.name, table, err)
		}
		have.cols = append(have.cols, tableColumn{column: c})
	}
	return refused, nil
}

func (w *writer) unapplied(ctx context.Context, desc staged, streams []stagedStream) ([]stagedStream, error) {
	var todo []stagedStream
	for _, s := range streams {
		done, err := w.marked(ctx, w.pool, desc, s.Name)
		if err != nil {
			return nil, err
		}
		if !done {
			todo = append(todo, s)
		}
	}
	return todo, nil
}

// dropStaging is best effort: staging left behind is never applied twice,
// because its marker exists.
func (w *writer) dropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		for _, t := range slices.Concat(s.Stagings, s.Patches) {
			_ = w.exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t))
		}
	}
}

// ensureSchema creates the schema if it's missing.
func (w *writer) ensureSchema(ctx context.Context) error {
	var n int64
	if err := w.pool.QueryRow(ctx, "SELECT COUNT(*) FROM pg_namespace WHERE nspname = $1", w.schema).Scan(&n); err != nil {
		return classify(fmt.Errorf("redshift: schema %s: %w", w.schema, err))
	}
	if n > 0 {
		return nil
	}
	if err := w.exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(w.schema)); err != nil {
		return fmt.Errorf("redshift: create schema %s: %w", w.schema, err)
	}
	return nil
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	if err := w.ensureSchema(ctx); err != nil {
		return 0, err
	}
	for _, q := range []string{
		"CREATE TABLE IF NOT EXISTS " + w.ident(fenceTable) + " (pipeline varchar(1024) NOT NULL, generation bigint NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + w.ident(loadsTable) + ` (pipeline varchar(1024) NOT NULL, stream varchar(1024) NOT NULL,
			generation bigint NOT NULL, epoch bigint NOT NULL, loaded_at timestamp NOT NULL)`,
	} {
		if err := w.exec(ctx, q); err != nil {
			return 0, fmt.Errorf("redshift: prepare %s: %w", w.schema, err)
		}
	}
	if err := w.raise(ctx, gen); err != nil {
		return 0, err
	}
	if e.IsZero() {
		w.dropRemovedReads(ctx)
		return destination.Applied, nil
	}

	desc, err := w.MergeLanes("redshift", committables)
	if err != nil {
		return 0, err
	}
	missing, err := w.unapplied(ctx, desc, desc.Streams)
	if err != nil {
		return 0, err
	}
	// An overwrite read that ended without rows in the epoch needs no
	// staging.
	var needs []string
	for _, m := range missing {
		needs = append(needs, slices.Concat(m.Stagings, m.Patches)...)
	}
	present, err := w.catalog(ctx, needs)
	if err != nil {
		return 0, err
	}
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		w.dropStaging(ctx, desc.Streams)
		r = destination.Applied
	case len(present) == len(slices.Compact(slices.Sorted(slices.Values(needs)))):
		if err := w.apply(ctx, desc, missing, map[string]uint64{}); err != nil {
			return 0, err
		}
		r = destination.Redone
	case len(missing) == len(desc.Streams):
		// Nothing of it is visible, and some lane's staging is gone, unless
		// a newer generation has raised the fence since, applied the epoch
		// and dropped its staging.
		if err := w.raise(ctx, gen); err != nil {
			return 0, err
		}
		r = destination.Lost
	default:
		return 0, fmt.Errorf("redshift: epoch %d-g%d is partly applied and the staging for the rest is gone", desc.Epoch, desc.Generation)
	}
	if r != destination.Lost {
		w.trimMarkers(ctx, desc.Epoch)
	}
	w.dropStale(ctx, gen)
	w.dropRemovedReads(ctx)
	return r, nil
}

// raise raises the pipeline's fence to gen, once applies that hold it are
// done, and fails with ErrFenced if a newer generation has raised it. Run
// again with the same gen, it changes nothing.
func (w *writer) raise(ctx context.Context, gen uint64) error {
	var raised *int64
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "LOCK "+w.ident(fenceTable)); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, "UPDATE "+w.ident(fenceTable)+" SET generation = $2 WHERE pipeline = $1 AND generation <= $2", w.cfg.Pipeline, gen)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var n int64
			if err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM "+w.ident(fenceTable)+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&n); err != nil {
				return err
			}
			if n == 0 {
				if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(fenceTable)+" (pipeline, generation) VALUES ($1, $2)", w.cfg.Pipeline, gen); err != nil {
					return err
				}
			}
		}
		return tx.QueryRow(ctx, "SELECT MAX(generation) FROM "+w.ident(fenceTable)+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&raised)
	})
	if err != nil {
		return fmt.Errorf("redshift: raise fence: %w", err)
	}
	if raised == nil || *raised < 0 || uint64(*raised) != gen {
		return fmt.Errorf("%w: recovering as generation %d, and the fence is %v", destination.ErrFenced, gen, raised)
	}
	return nil
}

// trimMarkers deletes the pipeline's markers below an applied epoch. Recovery
// checks only HEAD's, and HEAD never moves back past an applied epoch; a lost
// one moves it back to the previous record, whose markers it then checks.
// Best effort, like dropStaging.
func (w *writer) trimMarkers(ctx context.Context, applied uint64) {
	_ = w.exec(ctx, "DELETE FROM "+w.ident(loadsTable)+" WHERE pipeline = $1 AND epoch < $2", w.cfg.Pipeline, applied)
}

// labelled returns the schema's tables whose names start with prefix and
// that have a comment, with it.
func (w *writer) labelled(ctx context.Context, prefix string) ([][2]string, error) {
	rows, err := w.pool.Query(ctx, `SELECT c.relname::varchar, d.description FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0 WHERE n.nspname = $1 AND c.relkind = 'r'`, w.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var t [2]string
		if err := rows.Scan(&t[0], &t[1]); err != nil {
			return nil, err
		}
		if strings.HasPrefix(t[0], prefix) {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// dropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) dropStale(ctx context.Context, gen uint64) {
	labelled, err := w.labelled(ctx, "_openetl_stage_")
	if err != nil {
		return
	}
	for _, t := range labelled {
		var l stagingLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && l.Generation < gen {
			_ = w.exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0]))
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned. Callers hold the fence, so a zombie can't drop a read that
// continues.
func (w *writer) dropAbandonedReads(ctx context.Context, drop func(l readLabel, table string) bool) error {
	labelled, err := w.labelled(ctx, "_openetl_read_")
	if err != nil {
		return classify(fmt.Errorf("redshift: find abandoned reads: %w", err))
	}
	for _, t := range labelled {
		var l readLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && drop(l, t[0]) {
			if err := w.exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0])); err != nil {
				return fmt.Errorf("redshift: drop an abandoned read of %s: %w", l.Stream, err)
			}
		}
	}
	return nil
}

// dropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence. Best effort, like
// dropStaging.
func (w *writer) dropRemovedReads(ctx context.Context) {
	_ = w.dropAbandonedReads(ctx, func(l readLabel, _ string) bool {
		return w.streams[l.Stream].Mode != destination.Overwrite
	})
}

func (w *writer) Close(context.Context) error {
	for _, tables := range slices.Concat(slices.Collect(maps.Values(w.Staging)), slices.Collect(maps.Values(w.Patches))) {
		for _, st := range tables {
			for _, p := range st.parts {
				p.discard()
			}
		}
	}
	w.pool.Close()
	return os.RemoveAll(w.dir)
}

// classify gives the server's refusals their taxonomy codes, and failures
// to reach it, or to serialize with another transaction, TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
		}
		return err
	}
	switch {
	case pe.Code == "28P01" || pe.Code == "28000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case pe.Code == "3D000":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case pe.Code == "42501" || strings.Contains(pe.Message, "Access Denied") || strings.Contains(pe.Message, "is not authorized to assume"):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case pe.Code == "53100" || strings.Contains(pe.Message, "Disk Full"):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DESTINATION_QUOTA, err)
	case strings.HasPrefix(pe.Code, "08") || pe.Code == "40001" || pe.Code == "53300" || pe.Code == "57P01" || pe.Code == "57P03" ||
		strings.Contains(pe.Message, "Serializable isolation violation"):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
