package opensearch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

type osPageFn func(ctx context.Context, c *opensearchsdk.Client, size *int32, token *string) (int, *string, error)

// TestListOps_PageAndRejectBadTokens covers maxResults/nextToken (serializers.go@v1.75.4 query members).
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	const domain = "paged-domain"

	tests := []struct {
		list    osPageFn
		seed    func(t *testing.T, c *opensearchsdk.Client, b *opensearch.InMemoryBackend)
		name    string
		wantErr string
		total   int
		noSize  bool
	}{
		{
			name:  "reserved_instance_offerings",
			total: 3,
			list: func(ctx context.Context, c *opensearchsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.DescribeReservedInstanceOfferings(
					ctx,
					&opensearchsdk.DescribeReservedInstanceOfferingsInput{
						MaxResults: aws.ToInt32(sz), NextToken: tok,
					},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.ReservedInstanceOfferings), out.NextToken, nil
			},
		},
		{
			name:  "reserved_instances",
			total: 3,
			seed: func(t *testing.T, _ *opensearchsdk.Client, b *opensearch.InMemoryBackend) {
				t.Helper()

				for _, id := range []string{"ri-offering-1", "ri-offering-2", "ri-offering-3"} {
					_, err := b.PurchaseReservedInstanceOffering(id, "res-"+id, 1)
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.DescribeReservedInstances(ctx, &opensearchsdk.DescribeReservedInstancesInput{
					MaxResults: aws.ToInt32(sz), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ReservedInstances), out.NextToken, nil
			},
		},
		{
			name:  "instance_type_details",
			total: -1,
			list: func(ctx context.Context, c *opensearchsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.ListInstanceTypeDetails(ctx, &opensearchsdk.ListInstanceTypeDetailsInput{
					EngineVersion: aws.String("OpenSearch_2.11"), MaxResults: aws.ToInt32(sz), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.InstanceTypeDetails), out.NextToken, nil
			},
		},
		{
			name:   "vpc_endpoints_no_maxresults_member",
			total:  3,
			noSize: true,
			seed: func(t *testing.T, _ *opensearchsdk.Client, b *opensearch.InMemoryBackend) {
				t.Helper()

				d, err := b.CreateDomain(opensearch.CreateDomainInput{Name: domain})
				require.NoError(t, err)

				for range 3 {
					_, err = b.CreateVpcEndpoint(d.ARN, map[string]any{"SubnetIds": []any{"subnet-1"}})
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, _ *int32, tok *string) (int, *string, error) {
				out, err := c.ListVpcEndpoints(ctx, &opensearchsdk.ListVpcEndpointsInput{NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.VpcEndpointSummaryList), out.NextToken, nil
			},
		},
		{
			name:    "scheduled_actions_error_code",
			total:   0,
			wantErr: "InvalidPaginationTokenException",
			seed: func(t *testing.T, _ *opensearchsdk.Client, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: domain})
				require.NoError(t, err)
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz *int32, tok *string) (int, *string, error) {
				out, err := c.ListScheduledActions(ctx, &opensearchsdk.ListScheduledActionsInput{
					DomainName: aws.String(domain), MaxResults: aws.ToInt32(sz), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ScheduledActions), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
			c := newTestOpenSearchClient(t, opensearch.NewHandler(b))

			if tt.seed != nil {
				tt.seed(t, c, b)
			}

			wantErr := tt.wantErr
			if wantErr == "" {
				wantErr = "ValidationException"
			}

			_, _, err := tt.list(t.Context(), c, nil, aws.String("%%bogus%%"))
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, wantErr, apiErr.ErrorCode())

			if tt.total == 0 {
				return
			}

			total, next, err := tt.list(t.Context(), c, nil, nil)
			require.NoError(t, err)
			assert.Empty(t, aws.ToString(next))

			if tt.total > 0 {
				assert.Equal(t, tt.total, total)
			}

			if tt.noSize {
				return
			}

			n, next, err := tt.list(t.Context(), c, aws.Int32(2), nil)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
			require.NotNil(t, next)

			rest, _, err := tt.list(t.Context(), c, aws.Int32(2), next)
			require.NoError(t, err)
			assert.Equal(t, min(2, total-2), rest)
		})
	}
}
