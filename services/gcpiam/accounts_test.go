package gcpiam_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCreateServiceAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		parent    string
		id        string
		display   string
		wantEmail string
		wantCode  codes.Code
	}{
		{"valid", project, "my-account", "Mine", "my-account@test-project.iam.gserviceaccount.com", codes.OK},
		{"too short", project, "abc", "", "", codes.InvalidArgument},
		{"too long", project, strings.Repeat("a", 31), "", "", codes.InvalidArgument},
		{"uppercase", project, "My-Account", "", "", codes.InvalidArgument},
		{"trailing dash", project, "my-account-", "", "", codes.InvalidArgument},
		{"bad parent", "folders/1", "my-account", "", "", codes.InvalidArgument},
		{"long display name", project, "my-account", strings.Repeat("x", 101), "", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sa, err := e.client.CreateServiceAccount(t.Context(), &adminpb.CreateServiceAccountRequest{
				Name: tt.parent, AccountId: tt.id, ServiceAccount: &adminpb.ServiceAccount{DisplayName: tt.display},
			})

			if tt.wantCode != codes.OK {
				requireCode(t, tt.wantCode, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEmail, sa.GetEmail())
			assert.Equal(t, "projects/test-project/serviceAccounts/"+tt.wantEmail, sa.GetName())
			assert.Regexp(t, `^[1-9][0-9]{20}$`, sa.GetUniqueId())
			assert.Equal(t, sa.GetUniqueId(), sa.GetOauth2ClientId())
			assert.Equal(t, "test-project", sa.GetProjectId())
			assert.Equal(t, tt.display, sa.GetDisplayName())
		})
	}
}

func TestServiceAccountLookupAndDuplicates(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "lookup-sa")

	_, dupErr := e.client.CreateServiceAccount(
		t.Context(),
		&adminpb.CreateServiceAccountRequest{Name: project, AccountId: "lookup-sa"},
	)
	requireCode(t, codes.AlreadyExists, dupErr)

	tests := []struct {
		name     string
		resource string
		wantCode codes.Code
	}{
		{"by email", "projects/test-project/serviceAccounts/" + sa.GetEmail(), codes.OK},
		{"by unique id", "projects/test-project/serviceAccounts/" + sa.GetUniqueId(), codes.OK},
		{"wildcard project", "projects/-/serviceAccounts/" + sa.GetEmail(), codes.OK},
		{"wrong project", "projects/other/serviceAccounts/" + sa.GetEmail(), codes.NotFound},
		{"missing", "projects/test-project/serviceAccounts/nope@test-project.iam.gserviceaccount.com", codes.NotFound},
		{"malformed", "serviceAccounts/x", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := e.client.GetServiceAccount(t.Context(), &adminpb.GetServiceAccountRequest{Name: tt.resource})
			if tt.wantCode != codes.OK {
				requireCode(t, tt.wantCode, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, sa.GetUniqueId(), got.GetUniqueId())
		})
	}
}

func TestListServiceAccountsPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for i := range 5 {
		e.account(t, fmt.Sprintf("acct-%d-list", i))
	}

	it := e.client.ListServiceAccounts(t.Context(), &adminpb.ListServiceAccountsRequest{Name: project, PageSize: 2})

	var names []string

	for {
		sa, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		names = append(names, sa.GetEmail())
	}

	require.Len(t, names, 5)
	assert.IsIncreasing(t, names)

	other := e.client.ListServiceAccounts(t.Context(), &adminpb.ListServiceAccountsRequest{Name: "projects/another"})
	_, err := other.Next()
	require.ErrorIs(t, err, iterator.Done)
}

func TestUpdateAndPatchServiceAccount(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "patch-sa")

	patched, err := e.client.PatchServiceAccount(t.Context(), &adminpb.PatchServiceAccountRequest{
		ServiceAccount: &adminpb.ServiceAccount{Name: sa.GetName(), DisplayName: "New"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "New", patched.GetDisplayName())
	assert.Equal(t, "desc", patched.GetDescription())

	_, err = e.client.PatchServiceAccount(t.Context(), &adminpb.PatchServiceAccountRequest{
		ServiceAccount: &adminpb.ServiceAccount{Name: sa.GetName()},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"email"}},
	})
	requireCode(t, codes.InvalidArgument, err)

	updated, err := e.client.UpdateServiceAccount(
		t.Context(),
		&adminpb.ServiceAccount{Name: sa.GetName(), DisplayName: "Put"},
	)
	require.NoError(t, err)
	assert.Equal(t, "Put", updated.GetDisplayName())
	assert.Empty(t, updated.GetDescription())
	assert.Equal(t, sa.GetEmail(), updated.GetEmail())
}

func TestDisableEnableDeleteUndelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "life-sa")
	ctx := t.Context()

	require.NoError(t, e.client.DisableServiceAccount(ctx, &adminpb.DisableServiceAccountRequest{Name: sa.GetName()}))

	got, err := e.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	require.NoError(t, err)
	assert.True(t, got.GetDisabled())

	require.NoError(t, e.client.EnableServiceAccount(ctx, &adminpb.EnableServiceAccountRequest{Name: sa.GetName()}))

	got, err = e.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	require.NoError(t, err)
	assert.False(t, got.GetDisabled())

	_, err = e.client.UndeleteServiceAccount(
		ctx,
		&adminpb.UndeleteServiceAccountRequest{Name: project + "/serviceAccounts/" + sa.GetUniqueId()},
	)
	requireCode(t, codes.FailedPrecondition, err)

	require.NoError(t, e.client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: sa.GetName()}))

	_, err = e.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	requireCode(t, codes.NotFound, err)

	resp, err := e.client.UndeleteServiceAccount(ctx, &adminpb.UndeleteServiceAccountRequest{
		Name: "projects/-/serviceAccounts/" + sa.GetUniqueId(),
	})
	require.NoError(t, err)
	assert.Equal(t, sa.GetUniqueId(), resp.GetRestoredAccount().GetUniqueId())

	_, err = e.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	require.NoError(t, err)
}

func TestUndeleteServiceAccountEdges(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	first := e.account(t, "edge-sa")

	require.NoError(t, e.client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: first.GetName()}))

	second := e.account(t, "edge-sa")
	assert.NotEqual(t, first.GetUniqueId(), second.GetUniqueId())

	_, err := e.client.UndeleteServiceAccount(
		ctx,
		&adminpb.UndeleteServiceAccountRequest{Name: project + "/serviceAccounts/" + first.GetUniqueId()},
	)
	requireCode(t, codes.AlreadyExists, err)

	require.NoError(t, e.client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: second.GetName()}))
	e.clk.advance(31 * 24 * time.Hour)

	_, err = e.client.UndeleteServiceAccount(
		ctx,
		&adminpb.UndeleteServiceAccountRequest{Name: project + "/serviceAccounts/" + second.GetUniqueId()},
	)
	requireCode(t, codes.NotFound, err)
}
