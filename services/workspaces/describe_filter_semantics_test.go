package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeWorkspaceBundles_OwnerSemantics pins AMAZON for AWS bundles, no Owner for the
// account's own, and Owner excluding BundleIds (api_op_DescribeWorkspaceBundles.go).
func TestDescribeWorkspaceBundles_OwnerSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		owner     *string
		bundleIDs []string
		wantErr   bool
		wantAny   bool
	}{
		{name: "amazon_owner", owner: aws.String("AMAZON"), wantAny: true},
		{name: "no_owner_is_account_bundles", wantAny: false},
		{name: "bundle_id_amazon_bundle", bundleIDs: []string{"wsb-gm4d5tx2v"}, wantAny: true},
		{
			name:      "owner_with_bundle_ids",
			owner:     aws.String("AMAZON"),
			bundleIDs: []string{"wsb-gm4d5tx2v"},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			out, err := client.DescribeWorkspaceBundles(t.Context(), &wssdk.DescribeWorkspaceBundlesInput{
				Owner: tc.owner, BundleIds: tc.bundleIDs,
			})

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterValuesException")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantAny, len(out.Bundles) > 0)

			for _, b := range out.Bundles {
				assert.Equal(t, "AMAZON", aws.ToString(b.Owner))
			}
		})
	}
}

// TestDescribeWorkspaces_FilterExclusions pins the documented exclusions on
// DescribeWorkspacesInput (api_op_DescribeWorkspaces.go).
func TestDescribeWorkspaces_FilterExclusions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   wssdk.DescribeWorkspacesInput
		wantErr bool
	}{
		{name: "none", input: wssdk.DescribeWorkspacesInput{}},
		{
			name:  "directory_with_user",
			input: wssdk.DescribeWorkspacesInput{DirectoryId: aws.String("d-1"), UserName: aws.String("u")},
		},
		{name: "directory_only", input: wssdk.DescribeWorkspacesInput{DirectoryId: aws.String("d-1")}},
		{name: "bundle_only", input: wssdk.DescribeWorkspacesInput{BundleId: aws.String("wsb-1")}},
		{name: "ids_only", input: wssdk.DescribeWorkspacesInput{WorkspaceIds: []string{"ws-1"}}},
		{
			name:    "user_without_directory",
			input:   wssdk.DescribeWorkspacesInput{UserName: aws.String("u")},
			wantErr: true,
		},
		{
			name:    "ids_with_directory",
			input:   wssdk.DescribeWorkspacesInput{WorkspaceIds: []string{"ws-1"}, DirectoryId: aws.String("d-1")},
			wantErr: true,
		},
		{
			name:    "bundle_with_directory",
			input:   wssdk.DescribeWorkspacesInput{BundleId: aws.String("wsb-1"), DirectoryId: aws.String("d-1")},
			wantErr: true,
		},
		{
			name:    "ids_with_bundle",
			input:   wssdk.DescribeWorkspacesInput{WorkspaceIds: []string{"ws-1"}, BundleId: aws.String("wsb-1")},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.DescribeWorkspaces(t.Context(), &tc.input)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterValuesException")

				return
			}

			require.NoError(t, err)
		})
	}
}
