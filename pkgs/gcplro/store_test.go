package gcplro_test

import (
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const parent = "projects/p/locations/l"

func seeded(t *testing.T, ids ...string) *gcplro.Store {
	t.Helper()

	s := gcplro.New("test")

	for _, id := range ids {
		op, err := gcplro.Done(parent+"/operations/"+id, &emptypb.Empty{}, &emptypb.Empty{})
		require.NoError(t, err)
		s.Put(op)
	}

	return s
}

func TestListOperations(t *testing.T) {
	t.Parallel()

	s := seeded(t, "c", "a", "b")
	other, err := gcplro.Done("projects/p/locations/x/operations/z", &emptypb.Empty{}, &emptypb.Empty{})
	require.NoError(t, err)
	s.Put(other)

	tests := []struct {
		req  *longrunningpb.ListOperationsRequest
		name string
		want int
		code codes.Code
		next bool
	}{
		{name: "all under parent", req: &longrunningpb.ListOperationsRequest{Name: parent}, want: 3},
		{name: "paged", req: &longrunningpb.ListOperationsRequest{Name: parent, PageSize: 2}, want: 2, next: true},
		{name: "other parent", req: &longrunningpb.ListOperationsRequest{Name: "projects/p/locations/x"}, want: 1},
		{
			name: "filter",
			req:  &longrunningpb.ListOperationsRequest{Name: parent, Filter: "done=true"},
			code: codes.InvalidArgument,
		},
		{
			name: "negative size",
			req:  &longrunningpb.ListOperationsRequest{Name: parent, PageSize: -1},
			code: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, lerr := s.ListOperations(t.Context(), tt.req)
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(lerr))

				return
			}

			require.NoError(t, lerr)
			assert.Len(t, resp.GetOperations(), tt.want)
			assert.Equal(t, tt.next, resp.GetNextPageToken() != "")
		})
	}
}

func TestSnapshotRestoreAndReset(t *testing.T) {
	t.Parallel()

	s := seeded(t, "a")
	dst := gcplro.New("dst")
	require.NoError(t, dst.Restore(s.Snapshot()))

	got, err := dst.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: parent + "/operations/a"})
	require.NoError(t, err)
	assert.True(t, got.GetDone())

	dst.Reset()

	_, ok := dst.Lookup(parent + "/operations/a")
	assert.False(t, ok)
}
