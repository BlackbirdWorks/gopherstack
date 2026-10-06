package ssm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

const docContent = `{"schemaVersion":"2.2","mainSteps":[]}`

func seedWindowsAndDocs(t *testing.T, c *ssmsdk.Client) {
	t.Helper()

	for _, n := range []string{"mw-a", "mw-b", "mw-c"} {
		_, err := c.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
			Name: aws.String(n), Schedule: aws.String("cron(0 0 ? * * *)"), Duration: aws.Int32(2), Cutoff: 1,
		})
		require.NoError(t, err)
		_, err = c.PutParameter(t.Context(), &ssmsdk.PutParameterInput{
			Name: aws.String("/" + n), Value: aws.String("v"), Type: ssmtypes.ParameterTypeString,
		})
		require.NoError(t, err)
		_, err = c.CreateDocument(
			t.Context(),
			&ssmsdk.CreateDocumentInput{Name: aws.String(n), Content: aws.String(docContent)},
		)
		require.NoError(t, err)
	}
}

// TestListOps_FilterNarrowsResults covers Filters / DocumentFilterList wire members (ssm@v1.77.0).
func TestListOps_FilterNarrowsResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(ctx context.Context, c *ssmsdk.Client) ([]string, error)
		name string
		want []string
	}{
		{
			name: "maintenance_windows_name",
			want: []string{"mw-b"},
			list: func(ctx context.Context, c *ssmsdk.Client) ([]string, error) {
				out, err := c.DescribeMaintenanceWindows(ctx, &ssmsdk.DescribeMaintenanceWindowsInput{
					Filters: []ssmtypes.MaintenanceWindowFilter{{Key: aws.String("Name"), Values: []string{"mw-b"}}},
				})
				if err != nil {
					return nil, err
				}

				names := make([]string, 0)
				for _, w := range out.WindowIdentities {
					names = append(names, aws.ToString(w.Name))
				}

				return names, nil
			},
		},
		{
			name: "maintenance_windows_enabled_false",
			want: []string{},
			list: func(ctx context.Context, c *ssmsdk.Client) ([]string, error) {
				out, err := c.DescribeMaintenanceWindows(ctx, &ssmsdk.DescribeMaintenanceWindowsInput{
					Filters: []ssmtypes.MaintenanceWindowFilter{
						{Key: aws.String("Enabled"), Values: []string{"false"}},
					},
				})
				if err != nil {
					return nil, err
				}

				names := make([]string, 0)
				for _, w := range out.WindowIdentities {
					names = append(names, aws.ToString(w.Name))
				}

				return names, nil
			},
		},
		{
			name: "parameters_legacy_filters_name",
			want: []string{"/mw-c"},
			list: func(ctx context.Context, c *ssmsdk.Client) ([]string, error) {
				out, err := c.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{
					Filters: []ssmtypes.ParametersFilter{
						{Key: ssmtypes.ParametersFilterKeyName, Values: []string{"/mw-c"}},
					},
				})
				if err != nil {
					return nil, err
				}

				names := make([]string, 0)
				for _, p := range out.Parameters {
					names = append(names, aws.ToString(p.Name))
				}

				return names, nil
			},
		},
		{
			name: "documents_document_filter_list_name",
			want: []string{"mw-a"},
			list: func(ctx context.Context, c *ssmsdk.Client) ([]string, error) {
				out, err := c.ListDocuments(ctx, &ssmsdk.ListDocumentsInput{
					DocumentFilterList: []ssmtypes.DocumentFilter{
						{Key: ssmtypes.DocumentFilterKeyName, Value: aws.String("mw-a")},
					},
				})
				if err != nil {
					return nil, err
				}

				names := make([]string, 0)
				for _, d := range out.DocumentIdentifiers {
					names = append(names, aws.ToString(d.Name))
				}

				return names, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			seedWindowsAndDocs(t, c)

			got, err := tt.list(t.Context(), c)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestListOps_PageAndRejectBadTokens covers MaxResults/NextToken and the declared InvalidNextToken.
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list    func(ctx context.Context, c *ssmsdk.Client, size *int32, tok *string) (int, *string, error)
		name    string
		wantErr string
	}{
		{
			name:    "inventory_schema",
			wantErr: "InvalidNextToken",
			list: func(ctx context.Context, c *ssmsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.GetInventorySchema(ctx, &ssmsdk.GetInventorySchemaInput{MaxResults: sz, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Schemas), out.NextToken, nil
			},
		},
		{
			name:    "documents",
			wantErr: "InvalidNextToken",
			list: func(ctx context.Context, c *ssmsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.ListDocuments(ctx, &ssmsdk.ListDocumentsInput{MaxResults: sz, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.DocumentIdentifiers), out.NextToken, nil
			},
		},
		{
			name:    "parameters",
			wantErr: "InvalidNextToken",
			list: func(ctx context.Context, c *ssmsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{MaxResults: sz, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Parameters), out.NextToken, nil
			},
		},
		{
			name:    "maintenance_windows",
			wantErr: "ValidationException",
			list: func(ctx context.Context, c *ssmsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.DescribeMaintenanceWindows(
					ctx,
					&ssmsdk.DescribeMaintenanceWindowsInput{MaxResults: sz, NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.WindowIdentities), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			seedWindowsAndDocs(t, c)

			total, next, err := tt.list(t.Context(), c, nil, nil)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, total, 3)
			assert.Nil(t, next)

			n, next, err := tt.list(t.Context(), c, aws.Int32(2), nil)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
			require.NotNil(t, next)

			rest, _, err := tt.list(t.Context(), c, aws.Int32(2), next)
			require.NoError(t, err)
			assert.Equal(t, min(2, total-2), rest)

			_, _, err = tt.list(t.Context(), c, nil, aws.String("bogus"))
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
		})
	}
}

// TestAssociationExecutions_Filters covers the Filters members of the two association execution describes.
func TestAssociationExecutions_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		execKey string
		execVal string
		tgtKey  string
		tgtVal  string
		want    int
	}{
		{
			name:    "status_match",
			execKey: "Status",
			execVal: "Success",
			tgtKey:  "ResourceId",
			tgtVal:  "i-filter",
			want:    1,
		},
		{name: "status_miss", execKey: "Status", execVal: "Failed", tgtKey: "ResourceId", tgtVal: "i-other", want: 0},
		{
			name:    "unknown_execution_id",
			execKey: "ExecutionId",
			execVal: "nope",
			tgtKey:  "ResourceType",
			tgtVal:  "Nope",
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			_, err := c.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
				Name: aws.String("assoc-doc"), Content: aws.String(docContent),
			})
			require.NoError(t, err)

			created, err := c.CreateAssociation(t.Context(), &ssmsdk.CreateAssociationInput{
				Name: aws.String("assoc-doc"), InstanceId: aws.String("i-filter"),
			})
			require.NoError(t, err)
			id := created.AssociationDescription.AssociationId

			execs, err := c.DescribeAssociationExecutions(t.Context(), &ssmsdk.DescribeAssociationExecutionsInput{
				AssociationId: id,
				Filters: []ssmtypes.AssociationExecutionFilter{{
					Key:   ssmtypes.AssociationExecutionFilterKey(tt.execKey),
					Value: aws.String(tt.execVal),
					Type:  ssmtypes.AssociationFilterOperatorTypeEqual,
				}},
			})
			require.NoError(t, err)
			assert.Len(t, execs.AssociationExecutions, tt.want)

			targets, err := c.DescribeAssociationExecutionTargets(
				t.Context(),
				&ssmsdk.DescribeAssociationExecutionTargetsInput{
					AssociationId: id,
					ExecutionId:   aws.String("exec-1"),
					Filters: []ssmtypes.AssociationExecutionTargetsFilter{{
						Key:   ssmtypes.AssociationExecutionTargetsFilterKey(tt.tgtKey),
						Value: aws.String(tt.tgtVal),
					}},
				},
			)
			require.NoError(t, err)
			assert.Len(t, targets.AssociationExecutionTargets, tt.want)
		})
	}
}
