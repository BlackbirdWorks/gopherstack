package gcpspanner

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxPersistRows   = 200_000
	maxPersistBytes  = 64 << 20
	maxPersistTables = 1000
	dumpBudget       = 20 * time.Second
	recvSlack        = 1 << 20
	commitCells      = 20_000
	commitBytes      = 8 << 20
	maxDepth         = 32
)

const (
	pgTablesSQL = "SELECT table_name, parent_table_name FROM information_schema.tables " +
		"WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name"
	pgColumnsSQL = "SELECT table_name, column_name FROM information_schema.columns " +
		"WHERE table_schema = 'public' AND is_generated = 'NEVER' ORDER BY table_name, ordinal_position"
	tablesSQL = "SELECT TABLE_NAME, PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
		"WHERE TABLE_SCHEMA = '' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
	columnsSQL = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
		"WHERE TABLE_SCHEMA = '' AND IS_GENERATED = 'NEVER' ORDER BY TABLE_NAME, ORDINAL_POSITION"
)

type tableSnap struct {
	Name    string           `json:"name"`
	Parent  string           `json:"parent,omitempty"`
	Columns []string         `json:"columns"`
	Rows    []jsontext.Value `json:"rows,omitempty"`
}

type dbDataSnap struct {
	Tables []tableSnap `json:"tables"`
}

type dumpBudgetState struct{ rows, bytes int }

func (d *dumpBudgetState) take(rows, bytes int) bool {
	if d.rows+rows > maxPersistRows || d.bytes+bytes > maxPersistBytes {
		return false
	}

	d.rows, d.bytes = d.rows+rows, d.bytes+bytes

	return true
}

// collectData reads the table data of every GoogleSQL database from the running emulator. Without one it carries
// forward the data restored from the last snapshot that no emulator has loaded yet. Called without the lock.
func (b *InMemoryBackend) collectData(ctx context.Context) map[string]dbDataSnap {
	cc := b.emu.current()
	if cc == nil {
		b.mu.RLock("collectData")
		defer b.mu.RUnlock()

		return b.pendingData
	}

	dialects := map[string]bool{}

	b.mu.RLock("collectData")
	for _, r := range b.instances {
		for id, d := range r.dbs {
			dialects[databaseName(r.pb.GetName(), id)] = databaseIsPostgres(d.pb)
		}
	}
	b.mu.RUnlock()

	names := make([]string, 0, len(dialects))
	for n := range dialects {
		names = append(names, n)
	}

	slices.Sort(names)

	ctx, cancel := context.WithTimeout(ctx, dumpBudget)
	defer cancel()

	out := map[string]dbDataSnap{}
	budget := &dumpBudgetState{}
	log := logger.Load(ctx)

	for _, n := range names {
		attempt := *budget

		d, err := dumpDatabase(ctx, spannerpb.NewSpannerClient(cc), n, dialects[n], &attempt)
		if err != nil {
			log.WarnContext(ctx, "gcpspanner: table data not persisted, database kept schema-only",
				"database", n, "error", err)

			continue
		}

		*budget = attempt

		if len(d.Tables) > 0 {
			out[n] = d
		}
	}

	return out
}

func dumpDatabase(
	ctx context.Context, c spannerpb.SpannerClient, db string, postgres bool, budget *dumpBudgetState,
) (dbDataSnap, error) {
	sess, err := c.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: db})
	if err != nil {
		return dbDataSnap{}, fmt.Errorf("create session: %w", err)
	}

	defer func() {
		_, _ = c.DeleteSession(context.WithoutCancel(ctx), &spannerpb.DeleteSessionRequest{Name: sess.GetName()})
	}()

	tx, err := c.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
		Session: sess.GetName(),
		Options: &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_ReadOnly_{
			ReadOnly: &spannerpb.TransactionOptions_ReadOnly{
				TimestampBound: &spannerpb.TransactionOptions_ReadOnly_Strong{Strong: true},
			},
		}},
	})
	if err != nil {
		return dbDataSnap{}, fmt.Errorf("begin read-only transaction: %w", err)
	}

	sel := &spannerpb.TransactionSelector{Selector: &spannerpb.TransactionSelector_Id{Id: tx.GetId()}}

	tables, err := listTables(ctx, c, sess.GetName(), sel, postgres)
	if err != nil {
		return dbDataSnap{}, err
	}

	out := tables[:0]

	for i := range tables {
		rows, rerr := c.Read(ctx, &spannerpb.ReadRequest{
			Session: sess.GetName(), Transaction: sel, Table: tables[i].Name, Columns: tables[i].Columns,
			KeySet: &spannerpb.KeySet{All: true}, Limit: int64(maxPersistRows - budget.rows + 1),
		}, grpc.MaxCallRecvMsgSize(maxPersistBytes-budget.bytes+recvSlack))
		if status.Code(rerr) == codes.ResourceExhausted {
			return dbDataSnap{}, fmt.Errorf("%w: %s", errPersistLimit, tables[i].Name)
		}

		if rerr != nil {
			return dbDataSnap{}, fmt.Errorf("read %s: %w", tables[i].Name, rerr)
		}

		raw := make([]jsontext.Value, 0, len(rows.GetRows()))
		size := 0

		for _, r := range rows.GetRows() {
			v, merr := protojson.Marshal(r)
			if merr != nil {
				return dbDataSnap{}, fmt.Errorf("encode row of %s: %w", tables[i].Name, merr)
			}

			size += len(v)
			raw = append(raw, v)
		}

		if !budget.take(len(raw), size) {
			return dbDataSnap{}, fmt.Errorf("%w: %s", errPersistLimit, tables[i].Name)
		}

		if len(raw) == 0 {
			continue
		}

		tables[i].Rows = raw
		out = append(out, tables[i])
	}

	return dbDataSnap{Tables: out}, nil
}

var errRowShape = errors.New("stored row does not match its columns")

var errPersistLimit = fmt.Errorf(
	"table data exceeds the persistence limit of %d rows or %d bytes",
	maxPersistRows,
	maxPersistBytes,
)

func queryStrings(
	ctx context.Context,
	c spannerpb.SpannerClient,
	sess string,
	sel *spannerpb.TransactionSelector,
	sql string,
) ([][]string, error) {
	rs, err := c.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{Session: sess, Transaction: sel, Sql: sql})
	if err != nil {
		return nil, fmt.Errorf("query schema: %w", err)
	}

	out := make([][]string, 0, len(rs.GetRows()))

	for _, r := range rs.GetRows() {
		row := make([]string, len(r.GetValues()))
		for i, v := range r.GetValues() {
			row[i] = v.GetStringValue()
		}

		out = append(out, row)
	}

	return out, nil
}

func listTables(
	ctx context.Context, c spannerpb.SpannerClient, sess string, sel *spannerpb.TransactionSelector, postgres bool,
) ([]tableSnap, error) {
	tSQL, cSQL := tablesSQL, columnsSQL
	if postgres {
		tSQL, cSQL = pgTablesSQL, pgColumnsSQL
	}

	trows, err := queryStrings(ctx, c, sess, sel, tSQL)
	if err != nil {
		return nil, err
	}

	if len(trows) > maxPersistTables {
		return nil, fmt.Errorf("%w: %d tables", errPersistLimit, len(trows))
	}

	crows, err := queryStrings(ctx, c, sess, sel, cSQL)
	if err != nil {
		return nil, err
	}

	cols := map[string][]string{}

	for _, r := range crows {
		if len(r) == 2 { //nolint:mnd // table, column
			cols[r[0]] = append(cols[r[0]], r[1])
		}
	}

	var out []tableSnap

	for _, r := range trows {
		if len(r) == 2 && len(cols[r[0]]) > 0 {
			out = append(out, tableSnap{Name: r[0], Parent: r[1], Columns: cols[r[0]]})
		}
	}

	return out, nil
}

// parentFirst orders tables so every interleaved child follows its parent; a parent cycle is cut at maxDepth.
func parentFirst(tables []tableSnap) []tableSnap {
	idx := map[string]int{}
	for i, t := range tables {
		idx[t.Name] = i
	}

	depth := func(t tableSnap) int {
		d := 0

		for p := t.Parent; p != "" && d < maxDepth; d++ {
			i, ok := idx[p]
			if !ok {
				break
			}

			p = tables[i].Parent
		}

		return d
	}

	out := slices.Clone(tables)
	slices.SortStableFunc(out, func(a, b tableSnap) int { return depth(a) - depth(b) })

	return out
}

// replayData writes the restored table data into a freshly started emulator. A failure is logged and the data
// dropped: the data plane stays usable.
func (b *InMemoryBackend) replayData(ctx context.Context, cc grpc.ClientConnInterface) {
	b.mu.Lock("replayData")
	pending := b.pendingData
	b.pendingData = nil
	b.mu.Unlock()

	names := make([]string, 0, len(pending))
	for n := range pending {
		names = append(names, n)
	}

	slices.Sort(names)

	log := logger.Load(ctx)

	for _, n := range names {
		if err := loadDatabase(ctx, spannerpb.NewSpannerClient(cc), n, pending[n]); err != nil {
			log.WarnContext(ctx, "gcpspanner: table data not restored", "database", n, "error", err)
		}
	}
}

func loadDatabase(ctx context.Context, c spannerpb.SpannerClient, db string, d dbDataSnap) error {
	sess, err := c.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: db})
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	name := sess.GetName()

	defer func() {
		_, _ = c.DeleteSession(context.WithoutCancel(ctx), &spannerpb.DeleteSessionRequest{Name: name})
	}()

	if err = loadTables(ctx, c, name, parentFirst(d.Tables)); err != nil {
		if cerr := clearTables(context.WithoutCancel(ctx), c, name, d.Tables); cerr != nil {
			err = errors.Join(err, fmt.Errorf("clear partial restore: %w", cerr))
		}

		return err
	}

	return nil
}

// loadTables writes tables in order and retries those rejected by a foreign key until a pass makes no progress.
func loadTables(ctx context.Context, c spannerpb.SpannerClient, sess string, tables []tableSnap) error {
	for len(tables) > 0 {
		var deferred []tableSnap

		var last error

		for _, t := range tables {
			err := loadTable(ctx, c, sess, t)
			if status.Code(err) == codes.FailedPrecondition {
				deferred, last = append(deferred, t), err

				continue
			}

			if err != nil {
				return err
			}
		}

		if len(deferred) == len(tables) {
			return last
		}

		tables = deferred
	}

	return nil
}

// clearTables deletes every row of tables in one commit so a failed replay leaves the database empty.
func clearTables(ctx context.Context, c spannerpb.SpannerClient, sess string, tables []tableSnap) error {
	muts := make([]*spannerpb.Mutation, 0, len(tables))

	for _, t := range tables {
		muts = append(muts, &spannerpb.Mutation{Operation: &spannerpb.Mutation_Delete_{
			Delete: &spannerpb.Mutation_Delete{Table: t.Name, KeySet: &spannerpb.KeySet{All: true}},
		}})
	}

	_, err := c.Commit(ctx, commitRequest(sess, muts))

	return err //nolint:wrapcheck // wrapped by the caller
}

func commitRequest(sess string, muts []*spannerpb.Mutation) *spannerpb.CommitRequest {
	return &spannerpb.CommitRequest{
		Session: sess,
		Transaction: &spannerpb.CommitRequest_SingleUseTransaction{
			SingleUseTransaction: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		},
		Mutations: muts,
	}
}

func loadTable(ctx context.Context, c spannerpb.SpannerClient, sess string, t tableSnap) error {
	per := max(commitCells/max(len(t.Columns), 1), 1)

	for from := 0; from < len(t.Rows); {
		to, size := from, 0
		for to < len(t.Rows) && to-from < per && (to == from || size+len(t.Rows[to]) <= commitBytes) {
			size += len(t.Rows[to])
			to++
		}

		vals := make([]*structpb.ListValue, 0, to-from)

		for _, raw := range t.Rows[from:to] {
			lv := &structpb.ListValue{}
			if err := (protojson.UnmarshalOptions{}).Unmarshal(raw, lv); err != nil {
				return fmt.Errorf("decode row of %s: %w", t.Name, err)
			}

			if len(lv.GetValues()) != len(t.Columns) {
				return fmt.Errorf(
					"%w: %s has %d values for %d columns",
					errRowShape,
					t.Name,
					len(lv.GetValues()),
					len(t.Columns),
				)
			}

			vals = append(vals, lv)
		}

		_, err := c.Commit(
			ctx,
			commitRequest(sess, []*spannerpb.Mutation{{Operation: &spannerpb.Mutation_InsertOrUpdate{
				InsertOrUpdate: &spannerpb.Mutation_Write{Table: t.Name, Columns: t.Columns, Values: vals},
			}}}),
		)
		if err != nil {
			return fmt.Errorf("restore %s: %w", t.Name, err)
		}

		from = to
	}

	return nil
}

// dropPendingData forgets restored table data under prefix so a re-created database does not inherit it.
func (b *InMemoryBackend) dropPendingData(prefix string) {
	for n := range b.pendingData {
		if n == prefix || strings.HasPrefix(n, prefix+"/") {
			delete(b.pendingData, n)
		}
	}
}
