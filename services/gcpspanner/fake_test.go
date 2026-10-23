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
	headers   []string
	dropped   []string
	mu        sync.Mutex
}

func newFakeEmulator(t *testing.T) (*fakeEmulator, string) {
	t.Helper()

	f := &fakeEmulator{instances: map[string]bool{}, ddl: map[string][]string{}}
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
