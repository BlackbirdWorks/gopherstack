package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListDiscoveredResources_ResourceIDs_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{name: "no filter", want: []string{"b1", "b2", "b3"}},
		{name: "single", ids: []string{"b2"}, want: []string{"b2"}},
		{name: "multiple or", ids: []string{"b1", "b3"}, want: []string{"b1", "b3"}},
		{name: "unknown", ids: []string{"zzz"}, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newBackend(t)

			for _, id := range []string{"b1", "b2", "b3"} {
				require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", id, "{}"))
			}

			out, err := client.ListDiscoveredResources(t.Context(), &configservicesdk.ListDiscoveredResourcesInput{
				ResourceType: types.ResourceTypeBucket,
				ResourceIds:  tt.ids,
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, r := range out.ResourceIdentifiers {
				got = append(got, aws.ToString(r.ResourceId))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestSelectResourceConfig_UnsupportedGrammar_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expr     string
		wantCode string
		wantIDs  []string
	}{
		{
			name:    "supported and",
			expr:    "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket' AND resourceId = 'b1'",
			wantIDs: []string{"b1"},
		},
		{
			name: "or rejected", wantCode: "InvalidExpressionException",
			expr: "SELECT resourceId WHERE resourceId = 'b1' OR resourceId = 'b2'",
		},
		{
			name: "not equal rejected", wantCode: "InvalidExpressionException",
			expr: "SELECT resourceId WHERE resourceId != 'b1'",
		},
		{name: "garbage rejected", expr: "DROP TABLE x", wantCode: "InvalidExpressionException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newBackend(t)
			require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b1", "{}"))
			require.NoError(t, backend.PutResourceConfig("AWS::S3::Bucket", "b2", "{}"))

			out, err := client.SelectResourceConfig(
				t.Context(),
				&configservicesdk.SelectResourceConfigInput{Expression: aws.String(tt.expr)},
			)
			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Results, len(tt.wantIDs))
		})
	}
}
