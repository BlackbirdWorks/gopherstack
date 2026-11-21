package gcpspanner_test

import (
	"context"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// fakeEmulator records what the backend forwards, standing in for the Cloud Spanner emulator.
type fakeEmulator struct {
	instances map[string]bool
	ddl       map[string][]string
	tables    map[string][]fakeTable
	commits   []fakeCommit
	requires  map[string]string
	failing   map[string]codes.Code
	deleted   []string
	sqls      []string
	headers   []string
	dropped   []string
	mu        sync.Mutex
}

func newFakeEmulator(t *testing.T) (*fakeEmulator, string) {
	t.Helper()

	f := &fakeEmulator{
		instances: map[string]bool{},
		ddl:       map[string][]string{},
	}
	srv := grpc.NewServer()
	instancepb.RegisterInstanceAdminServer(srv, fakeInst{f: f})
	databasepb.RegisterDatabaseAdminServer(srv, fakeDB{f: f})
	spannerpb.RegisterSpannerServer(srv, fakeSpanner{f: f})
	longrunningpb.RegisterOperationsServer(srv, longrunningpb.UnimplementedOperationsServer{})

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Serve(l)
	}()

	t.Cleanup(func() {
		srv.Stop()
		<-done
	})

	return f, l.Addr().String()
}

type (
	fakeInst struct {
		instancepb.UnimplementedInstanceAdminServer
		f *fakeEmulator
	}
	fakeDB struct {
		databasepb.UnimplementedDatabaseAdminServer
		f *fakeEmulator
	}
	fakeSpanner struct {
		spannerpb.UnimplementedSpannerServer
		f *fakeEmulator
	}
)

func doneOp(name string) *longrunningpb.Operation {
	return &longrunningpb.Operation{Name: name, Done: true}
}

func (s fakeInst) CreateInstance(
	_ context.Context, in *instancepb.CreateInstanceRequest,
) (*longrunningpb.Operation, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	name := in.GetParent() + "/instances/" + in.GetInstanceId()
	if s.f.instances[name] {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}

	s.f.instances[name] = true

	return doneOp(name + "/operations/op"), nil
}

func (s fakeInst) DeleteInstance(_ context.Context, in *instancepb.DeleteInstanceRequest) (*emptypb.Empty, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	delete(s.f.instances, in.GetName())

	return &emptypb.Empty{}, nil
}

func dbID(stmt string) string {
	return strings.Trim(strings.TrimSpace(strings.TrimPrefix(stmt, "CREATE DATABASE")), "`\"")
}

func (s fakeDB) CreateDatabase(
	_ context.Context, in *databasepb.CreateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	if !s.f.instances[in.GetParent()] {
		return nil, status.Error(codes.NotFound, "instance missing on emulator")
	}

	name := in.GetParent() + "/databases/" + dbID(in.GetCreateStatement())
	s.f.ddl[name] = append([]string(nil), in.GetExtraStatements()...)

	return doneOp(name + "/operations/create"), nil
}

func (s fakeDB) GetDatabaseDdl(
	_ context.Context, in *databasepb.GetDatabaseDdlRequest,
) (*databasepb.GetDatabaseDdlResponse, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	return &databasepb.GetDatabaseDdlResponse{Statements: s.f.ddl[in.GetDatabase()]}, nil
}

func (s fakeDB) UpdateDatabaseDdl(
	_ context.Context, in *databasepb.UpdateDatabaseDdlRequest,
) (*longrunningpb.Operation, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	op := doneOp(in.GetDatabase() + "/operations/ddl")

	for _, stmt := range in.GetStatements() {
		if strings.Contains(stmt, "BAD") {
			op.Result = &longrunningpb.Operation_Error{Error: status.New(codes.InvalidArgument, "bad ddl").Proto()}

			return op, nil
		}

		s.f.ddl[in.GetDatabase()] = append(s.f.ddl[in.GetDatabase()], stmt)
	}

	return op, nil
}

func (s fakeDB) DropDatabase(_ context.Context, in *databasepb.DropDatabaseRequest) (*emptypb.Empty, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	delete(s.f.ddl, in.GetDatabase())
	s.f.dropped = append(s.f.dropped, in.GetDatabase())

	return &emptypb.Empty{}, nil
}

func (s fakeSpanner) CreateSession(
	ctx context.Context, in *spannerpb.CreateSessionRequest,
) (*spannerpb.Session, error) {
	s.f.recordHeader(ctx)

	return &spannerpb.Session{Name: in.GetDatabase() + "/sessions/s1"}, nil
}

func (f *fakeEmulator) recordHeader(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)

	f.mu.Lock()
	f.headers = append(f.headers, md.Get("x-goog-request-params")...)
	f.mu.Unlock()
}

func (s fakeSpanner) ExecuteStreamingSql( //nolint:revive,staticcheck // gRPC method name
	in *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer,
) error {
	for _, v := range []string{in.GetSql(), "second"} {
		if err := stream.Send(
			&spannerpb.PartialResultSet{Values: []*structpb.Value{structpb.NewStringValue(v)}},
		); err != nil {
			return err
		}
	}

	return nil
}

func (f *fakeEmulator) snapshot() (map[string]bool, map[string][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	inst := maps.Clone(f.instances)

	ddl := map[string][]string{}
	for k, v := range f.ddl {
		ddl[k] = append([]string(nil), v...)
	}

	return inst, ddl
}

func (s fakeDB) GetDatabase(_ context.Context, in *databasepb.GetDatabaseRequest) (*databasepb.Database, error) {
	return &databasepb.Database{
		Name:            in.GetName(),
		DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
	}, nil
}

type fakeTable struct {
	name, parent string
	columns      []string
	rows         [][]*structpb.Value
}

type fakeCommit struct {
	db, table string
	columns   []string
	rows      [][]string
}

func (f *fakeEmulator) seed(db string, tables ...fakeTable) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.tables == nil {
		f.tables = map[string][]fakeTable{}
	}

	f.tables[db] = tables
}

func (f *fakeEmulator) committed() []fakeCommit {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]fakeCommit(nil), f.commits...)
}

func dbOfSession(s string) string {
	db, _, _ := strings.Cut(s, "/sessions/")

	return db
}

func strRow(vals ...string) *structpb.ListValue {
	lv := &structpb.ListValue{}
	for _, v := range vals {
		lv.Values = append(lv.Values, structpb.NewStringValue(v))
	}

	return lv
}

func (s fakeSpanner) BeginTransaction(
	_ context.Context,
	_ *spannerpb.BeginTransactionRequest,
) (*spannerpb.Transaction, error) {
	return &spannerpb.Transaction{Id: []byte("tx")}, nil
}

func (s fakeSpanner) DeleteSession(_ context.Context, _ *spannerpb.DeleteSessionRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s fakeSpanner) ExecuteSql( //nolint:revive,staticcheck // gRPC method name
	_ context.Context, in *spannerpb.ExecuteSqlRequest) (*spannerpb.ResultSet, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	rs := &spannerpb.ResultSet{}
	sql := strings.ToUpper(in.GetSql())
	s.f.sqls = append(s.f.sqls, in.GetSql())

	for _, t := range s.f.tables[dbOfSession(in.GetSession())] {
		switch {
		case strings.Contains(sql, "INFORMATION_SCHEMA.TABLES"):
			rs.Rows = append(rs.Rows, strRow(t.name, t.parent))
		case strings.Contains(sql, "INFORMATION_SCHEMA.COLUMNS"):
			for _, c := range t.columns {
				rs.Rows = append(rs.Rows, strRow(t.name, c))
			}
		}
	}

	return rs, nil
}

func (s fakeSpanner) Read(_ context.Context, in *spannerpb.ReadRequest) (*spannerpb.ResultSet, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	rs := &spannerpb.ResultSet{}

	for _, t := range s.f.tables[dbOfSession(in.GetSession())] {
		if t.name == in.GetTable() {
			for _, r := range t.rows {
				rs.Rows = append(rs.Rows, &structpb.ListValue{Values: r})
			}
		}
	}

	return rs, nil
}

func (s fakeSpanner) Commit(_ context.Context, in *spannerpb.CommitRequest) (*spannerpb.CommitResponse, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	for _, m := range in.GetMutations() {
		if d := m.GetDelete(); d != nil {
			s.f.deleted = append(s.f.deleted, d.GetTable())

			continue
		}

		w := m.GetInsertOrUpdate()
		if code, bad := s.f.failing[w.GetTable()]; bad {
			return nil, status.Error(code, "injected")
		}

		if need, ok := s.f.requires[w.GetTable()]; ok && !s.f.committedLocked(need) {
			return nil, status.Error(codes.FailedPrecondition, "referenced key missing in "+need)
		}

		c := fakeCommit{db: dbOfSession(in.GetSession()), table: w.GetTable(), columns: w.GetColumns()}

		for _, r := range w.GetValues() {
			row := make([]string, len(r.GetValues()))
			for i, v := range r.GetValues() {
				row[i] = v.GetStringValue()
			}

			c.rows = append(c.rows, row)
		}

		s.f.commits = append(s.f.commits, c)
	}

	return &spannerpb.CommitResponse{}, nil
}

func (f *fakeEmulator) queries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.sqls...)
}

func (f *fakeEmulator) committedLocked(table string) bool {
	for _, c := range f.commits {
		if c.table == table {
			return true
		}
	}

	return false
}

func (f *fakeEmulator) require(table, other string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.requires == nil {
		f.requires = map[string]string{}
	}

	f.requires[table] = other
}

func (f *fakeEmulator) failCommits(table string, code codes.Code) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failing == nil {
		f.failing = map[string]codes.Code{}
	}

	f.failing[table] = code
}

func (f *fakeEmulator) deletedTables() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.deleted...)
}
