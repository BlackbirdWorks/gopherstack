package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestDescribeAddonVersions_Filters pins DescribeAddonVersionsInput.AddonName,
// KubernetesVersion ("versions that you can use the add-on with") and Types.
func TestDescribeAddonVersions_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantVersions map[string][]string
		name         string
		wantAddons   []string
		input        ekssdk.DescribeAddonVersionsInput
	}{
		{
			name:       "by_name",
			input:      ekssdk.DescribeAddonVersionsInput{AddonName: aws.String("coredns")},
			wantAddons: []string{"coredns"},
		},
		{name: "unknown_name", input: ekssdk.DescribeAddonVersionsInput{AddonName: aws.String("nope")}},
		{
			name:       "by_type",
			input:      ekssdk.DescribeAddonVersionsInput{Types: []string{"storage"}},
			wantAddons: []string{"aws-ebs-csi-driver", "aws-efs-csi-driver"},
		},
		{
			name: "k8s_version_narrows_versions",
			input: ekssdk.DescribeAddonVersionsInput{
				AddonName:         aws.String("kube-proxy"),
				KubernetesVersion: aws.String("1.32"),
			},
			wantAddons:   []string{"kube-proxy"},
			wantVersions: map[string][]string{"kube-proxy": {"v1.32.0-eksbuild.1"}},
		},
		{
			name:       "k8s_version_without_match",
			input:      ekssdk.DescribeAddonVersionsInput{KubernetesVersion: aws.String("1.10")},
			wantAddons: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(
				t,
				eks.NewHandler(eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")),
			)
			out, err := client.DescribeAddonVersions(t.Context(), &tc.input)
			require.NoError(t, err)

			var got []string
			for _, a := range out.Addons {
				got = append(got, aws.ToString(a.AddonName))

				if want, ok := tc.wantVersions[aws.ToString(a.AddonName)]; ok {
					var versions []string
					for _, v := range a.AddonVersions {
						versions = append(versions, aws.ToString(v.AddonVersion))
					}

					assert.Equal(t, want, versions)
				}
			}

			assert.ElementsMatch(t, tc.wantAddons, got)
		})
	}
}
