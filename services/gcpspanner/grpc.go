package gcpspanner

import (
	"context"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type instanceServer struct {
	instancepb.UnimplementedInstanceAdminServer
	b *InMemoryBackend
}

func (s *instanceServer) ListInstanceConfigs(
	_ context.Context, in *instancepb.ListInstanceConfigsRequest,
) (*instancepb.ListInstanceConfigsResponse, error) {
	return s.b.ListInstanceConfigs(in)
}

func (s *instanceServer) GetInstanceConfig(
	_ context.Context, in *instancepb.GetInstanceConfigRequest,
) (*instancepb.InstanceConfig, error) {
	return s.b.GetInstanceConfig(in.GetName())
}

func (s *instanceServer) ListInstances(
	_ context.Context, in *instancepb.ListInstancesRequest,
) (*instancepb.ListInstancesResponse, error) {
	return s.b.ListInstances(in)
}

func (s *instanceServer) GetInstance(
	_ context.Context, in *instancepb.GetInstanceRequest,
) (*instancepb.Instance, error) {
	return s.b.GetInstance(in.GetName())
}

func (s *instanceServer) CreateInstance(
	_ context.Context, in *instancepb.CreateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateInstance(in)
}

func (s *instanceServer) UpdateInstance(
	_ context.Context, in *instancepb.UpdateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateInstance(in)
}

func (s *instanceServer) DeleteInstance(
	_ context.Context, in *instancepb.DeleteInstanceRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteInstance(in.GetName())
}

func (s *instanceServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion())
}

func (s *instanceServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *instanceServer) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

// databaseServer answers database metadata and DDL itself and proxies the rest to the emulator.
type databaseServer struct {
	databasepb.UnimplementedDatabaseAdminServer
	b *InMemoryBackend
}

func (s *databaseServer) ListDatabases(
	_ context.Context, in *databasepb.ListDatabasesRequest,
) (*databasepb.ListDatabasesResponse, error) {
	return s.b.ListDatabases(in)
}

func (s *databaseServer) CreateDatabase(
	ctx context.Context, in *databasepb.CreateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateDatabase(ctx, in)
}

func (s *databaseServer) GetDatabase(
	_ context.Context, in *databasepb.GetDatabaseRequest,
) (*databasepb.Database, error) {
	return s.b.GetDatabase(in.GetName())
}

func (s *databaseServer) UpdateDatabase(
	_ context.Context, in *databasepb.UpdateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateDatabase(in)
}

func (s *databaseServer) UpdateDatabaseDdl(
	ctx context.Context, in *databasepb.UpdateDatabaseDdlRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateDatabaseDdl(ctx, in)
}

func (s *databaseServer) DropDatabase(ctx context.Context, in *databasepb.DropDatabaseRequest) (*emptypb.Empty, error) {
	return s.b.DropDatabase(ctx, in.GetDatabase())
}

func (s *databaseServer) GetDatabaseDdl(
	_ context.Context, in *databasepb.GetDatabaseDdlRequest,
) (*databasepb.GetDatabaseDdlResponse, error) {
	return s.b.GetDatabaseDdl(in.GetDatabase())
}

// localDatabaseMethods are answered by databaseServer; RestoreDatabase and ListDatabaseOperations stay
// unimplemented because the emulator's databases and operations are not the backend's.
var localDatabaseMethods = map[string]bool{ //nolint:gochecknoglobals // immutable catalog
	"ListDatabases": true, "CreateDatabase": true, "GetDatabase": true, "UpdateDatabase": true,
	"UpdateDatabaseDdl": true, "DropDatabase": true, "GetDatabaseDdl": true,
	"RestoreDatabase": true, "ListDatabaseOperations": true,
}
