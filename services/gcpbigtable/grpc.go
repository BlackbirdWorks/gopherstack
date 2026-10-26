package gcpbigtable

import (
	"context"
	"encoding/base64"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type instanceAdminServer struct {
	adminpb.UnimplementedBigtableInstanceAdminServer
	b *InMemoryBackend
}

func (s *instanceAdminServer) CreateInstance(
	_ context.Context, in *adminpb.CreateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateInstance(in)
}

func (s *instanceAdminServer) GetInstance(
	_ context.Context,
	in *adminpb.GetInstanceRequest,
) (*adminpb.Instance, error) {
	return s.b.GetInstance(in.GetName())
}

func (s *instanceAdminServer) ListInstances(
	_ context.Context, in *adminpb.ListInstancesRequest,
) (*adminpb.ListInstancesResponse, error) {
	return s.b.ListInstances(in)
}

func (s *instanceAdminServer) UpdateInstance(_ context.Context, in *adminpb.Instance) (*adminpb.Instance, error) {
	return s.b.UpdateInstance(in)
}

func (s *instanceAdminServer) PartialUpdateInstance(
	_ context.Context, in *adminpb.PartialUpdateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.PartialUpdateInstance(in)
}

func (s *instanceAdminServer) DeleteInstance(
	_ context.Context, in *adminpb.DeleteInstanceRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteInstance(in.GetName())
}

func (s *instanceAdminServer) CreateCluster(
	_ context.Context, in *adminpb.CreateClusterRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateCluster(in)
}

func (s *instanceAdminServer) GetCluster(_ context.Context, in *adminpb.GetClusterRequest) (*adminpb.Cluster, error) {
	return s.b.GetCluster(in.GetName())
}

func (s *instanceAdminServer) ListClusters(
	_ context.Context, in *adminpb.ListClustersRequest,
) (*adminpb.ListClustersResponse, error) {
	return s.b.ListClusters(in)
}

func (s *instanceAdminServer) UpdateCluster(_ context.Context, in *adminpb.Cluster) (*longrunningpb.Operation, error) {
	return s.b.UpdateCluster(in)
}

func (s *instanceAdminServer) PartialUpdateCluster(
	_ context.Context, in *adminpb.PartialUpdateClusterRequest,
) (*longrunningpb.Operation, error) {
	return s.b.PartialUpdateCluster(in)
}

func (s *instanceAdminServer) DeleteCluster(
	_ context.Context, in *adminpb.DeleteClusterRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteCluster(in.GetName())
}

func (s *instanceAdminServer) CreateAppProfile(
	_ context.Context, in *adminpb.CreateAppProfileRequest,
) (*adminpb.AppProfile, error) {
	return s.b.CreateAppProfile(in)
}

func (s *instanceAdminServer) GetAppProfile(
	_ context.Context, in *adminpb.GetAppProfileRequest,
) (*adminpb.AppProfile, error) {
	return s.b.GetAppProfile(in.GetName())
}

func (s *instanceAdminServer) ListAppProfiles(
	_ context.Context, in *adminpb.ListAppProfilesRequest,
) (*adminpb.ListAppProfilesResponse, error) {
	return s.b.ListAppProfiles(in)
}

func (s *instanceAdminServer) UpdateAppProfile(
	_ context.Context, in *adminpb.UpdateAppProfileRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateAppProfile(in)
}

func (s *instanceAdminServer) DeleteAppProfile(
	_ context.Context, in *adminpb.DeleteAppProfileRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteAppProfile(in)
}

func (s *instanceAdminServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion())
}

func (s *instanceAdminServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *instanceAdminServer) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

type tableAdminServer struct {
	adminpb.UnimplementedBigtableTableAdminServer
	b *InMemoryBackend
}

func (s *tableAdminServer) CreateTable(_ context.Context, in *adminpb.CreateTableRequest) (*adminpb.Table, error) {
	return s.b.CreateTable(in)
}

func (s *tableAdminServer) ListTables(
	_ context.Context, in *adminpb.ListTablesRequest,
) (*adminpb.ListTablesResponse, error) {
	return s.b.ListTables(in)
}

func (s *tableAdminServer) GetTable(_ context.Context, in *adminpb.GetTableRequest) (*adminpb.Table, error) {
	return s.b.GetTable(in)
}

func (s *tableAdminServer) UpdateTable(
	_ context.Context, in *adminpb.UpdateTableRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateTable(in)
}

func (s *tableAdminServer) DeleteTable(_ context.Context, in *adminpb.DeleteTableRequest) (*emptypb.Empty, error) {
	return s.b.DeleteTable(in.GetName())
}

func (s *tableAdminServer) ModifyColumnFamilies(
	_ context.Context, in *adminpb.ModifyColumnFamiliesRequest,
) (*adminpb.Table, error) {
	return s.b.ModifyColumnFamilies(in)
}

func (s *tableAdminServer) DropRowRange(_ context.Context, in *adminpb.DropRowRangeRequest) (*emptypb.Empty, error) {
	return s.b.DropRowRange(in)
}

func (s *tableAdminServer) GenerateConsistencyToken(
	_ context.Context, in *adminpb.GenerateConsistencyTokenRequest,
) (*adminpb.GenerateConsistencyTokenResponse, error) {
	return s.b.GenerateConsistencyToken(in)
}

func (s *tableAdminServer) CheckConsistency(
	_ context.Context, in *adminpb.CheckConsistencyRequest,
) (*adminpb.CheckConsistencyResponse, error) {
	return s.b.CheckConsistency(in)
}

func (s *tableAdminServer) CreateBackup(
	_ context.Context, in *adminpb.CreateBackupRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateBackup(in)
}

func (s *tableAdminServer) GetBackup(_ context.Context, in *adminpb.GetBackupRequest) (*adminpb.Backup, error) {
	return s.b.GetBackup(in.GetName())
}

func (s *tableAdminServer) UpdateBackup(_ context.Context, in *adminpb.UpdateBackupRequest) (*adminpb.Backup, error) {
	return s.b.UpdateBackup(in)
}

func (s *tableAdminServer) DeleteBackup(_ context.Context, in *adminpb.DeleteBackupRequest) (*emptypb.Empty, error) {
	return s.b.DeleteBackup(in.GetName())
}

func (s *tableAdminServer) ListBackups(
	_ context.Context, in *adminpb.ListBackupsRequest,
) (*adminpb.ListBackupsResponse, error) {
	return s.b.ListBackups(in)
}

func (s *tableAdminServer) RestoreTable(
	_ context.Context, in *adminpb.RestoreTableRequest,
) (*longrunningpb.Operation, error) {
	return s.b.RestoreTable(in)
}

func (s *tableAdminServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion())
}

func (s *tableAdminServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *tableAdminServer) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

type dataServer struct {
	btpb.UnimplementedBigtableServer
	b *InMemoryBackend
}

func featureFlags(ctx context.Context) *btpb.FeatureFlags {
	ff := &btpb.FeatureFlags{}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ff
	}

	for _, v := range md.Get("bigtable-features") {
		if raw, err := base64.StdEncoding.DecodeString(v); err == nil {
			_ = proto.Unmarshal(raw, ff)
		}
	}

	return ff
}

func (s *dataServer) MutateRow(_ context.Context, in *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	return s.b.MutateRow(in)
}

func (s *dataServer) MutateRows(in *btpb.MutateRowsRequest, stream btpb.Bigtable_MutateRowsServer) error {
	entries, err := s.b.MutateRows(in)
	if err != nil {
		return err
	}

	return stream.Send(&btpb.MutateRowsResponse{Entries: entries})
}

func (s *dataServer) CheckAndMutateRow(
	_ context.Context, in *btpb.CheckAndMutateRowRequest,
) (*btpb.CheckAndMutateRowResponse, error) {
	return s.b.CheckAndMutateRow(in)
}

func (s *dataServer) ReadModifyWriteRow(
	_ context.Context, in *btpb.ReadModifyWriteRowRequest,
) (*btpb.ReadModifyWriteRowResponse, error) {
	return s.b.ReadModifyWriteRow(in)
}

func (s *dataServer) PingAndWarm(_ context.Context, in *btpb.PingAndWarmRequest) (*btpb.PingAndWarmResponse, error) {
	if err := s.b.PingAndWarm(in); err != nil {
		return nil, err
	}

	return &btpb.PingAndWarmResponse{}, nil
}

func (s *dataServer) SampleRowKeys(in *btpb.SampleRowKeysRequest, stream btpb.Bigtable_SampleRowKeysServer) error {
	samples, err := s.b.SampleRowKeys(in)
	if err != nil {
		return err
	}

	for _, sm := range samples {
		if err = stream.Send(sm); err != nil {
			return err //nolint:wrapcheck // stream errors pass through to the transport
		}
	}

	return nil
}

func (s *dataServer) ReadRows(in *btpb.ReadRowsRequest, stream btpb.Bigtable_ReadRowsServer) error {
	res, err := s.b.ReadRows(in)
	if err != nil {
		return err
	}

	flags := featureFlags(stream.Context())

	for _, row := range res.Rows {
		var resp *btpb.ReadRowsResponse

		switch {
		case len(row.Cells) > 0:
			resp = &btpb.ReadRowsResponse{Chunks: rowChunks(row)}
		case flags.GetLastScannedRowResponses():
			resp = &btpb.ReadRowsResponse{LastScannedRowKey: []byte(row.Key)}
		default:
			continue
		}

		if err = stream.Send(resp); err != nil {
			return err //nolint:wrapcheck // stream errors pass through to the transport
		}
	}

	if in.GetRequestStatsView() == btpb.ReadRowsRequest_REQUEST_STATS_FULL {
		return stream.Send(&btpb.ReadRowsResponse{RequestStats: &btpb.RequestStats{
			StatsView: &btpb.RequestStats_FullReadStatsView{FullReadStatsView: &btpb.FullReadStatsView{
				ReadIterationStats:  res.Stats,
				RequestLatencyStats: &btpb.RequestLatencyStats{FrontendServerLatency: durationpb.New(0)},
			}},
		}})
	}

	return nil
}

// rowChunks encodes a row as cell chunks, naming the row, family and qualifier only when they change.
func rowChunks(row *ResultRow) []*btpb.ReadRowsResponse_CellChunk {
	chunks := make([]*btpb.ReadRowsResponse_CellChunk, 0, len(row.Cells))

	for i, c := range row.Cells {
		ch := &btpb.ReadRowsResponse_CellChunk{TimestampMicros: c.ts, Value: c.val}
		if c.label != "" {
			ch.Labels = []string{c.label}
		}

		newFam := i == 0 || c.fam != row.Cells[i-1].fam

		if i == 0 {
			ch.RowKey = []byte(row.Key)
		}

		if newFam {
			ch.FamilyName = wrapperspb.String(c.fam)
		}

		if newFam || c.qual != row.Cells[i-1].qual {
			ch.Qualifier = wrapperspb.Bytes([]byte(c.qual))
		}

		chunks = append(chunks, ch)
	}

	chunks[len(chunks)-1].RowStatus = &btpb.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true}

	return chunks
}
