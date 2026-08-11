package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	networkmonitorsvc "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	vpclatticesvc "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticeTypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NetworkMonitor
// ---------------------------------------------------------------------------

// TestTerraform_NetworkMonitor provisions a NetworkMonitor monitor via
// Terraform and verifies it appears in ListMonitors.
func TestTerraform_NetworkMonitor(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "networkmonitor/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"MonitorName": "tf-nm-" + uuid.NewString()[:8],
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createNetworkMonitorClient(t)
				monitorName := vars["MonitorName"].(string)

				out, err := client.ListMonitors(ctx, &networkmonitorsvc.ListMonitorsInput{})
				require.NoError(t, err, "ListMonitors should succeed after terraform apply")

				found := false
				for _, m := range out.Monitors {
					if aws.ToString(m.MonitorName) == monitorName {
						found = true

						break
					}
				}
				assert.True(t, found, "monitor %q should appear in ListMonitors", monitorName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// TestTerraformImport_NetworkMonitor verifies that a NetworkMonitor monitor
// created via the SDK can be imported into Terraform state without drift.
func TestTerraformImport_NetworkMonitor(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "monitor",
			fixture:         "networkmonitor/import",
			resourceAddress: "aws_networkmonitor_monitor.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createNetworkMonitorClient(t)
				monitorName := "tf-nm-import-" + uuid.NewString()[:8]

				_, err := client.CreateMonitor(ctx, &networkmonitorsvc.CreateMonitorInput{
					MonitorName:       aws.String(monitorName),
					AggregationPeriod: aws.Int64(30),
				})
				require.NoError(t, err, "CreateMonitor should succeed")

				t.Cleanup(func() {
					cleanupCtx, cancel := cleanupContext(t)
					defer cancel()

					_, _ = client.DeleteMonitor(cleanupCtx, &networkmonitorsvc.DeleteMonitorInput{
						MonitorName: aws.String(monitorName),
					})
				})

				return map[string]any{"MonitorName": monitorName}, monitorName
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformDrift_NetworkMonitor verifies that Terraform detects and
// corrects drift when a monitor's aggregation period is changed via the SDK.
func TestTerraformDrift_NetworkMonitor(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "aggregation_period_changed",
			fixture: "networkmonitor/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"MonitorName":       "tf-nm-drift-" + uuid.NewString()[:8],
					"AggregationPeriod": 30,
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createNetworkMonitorClient(t)
				monitorName := vars["MonitorName"].(string)

				_, err := client.UpdateMonitor(ctx, &networkmonitorsvc.UpdateMonitorInput{
					MonitorName:       aws.String(monitorName),
					AggregationPeriod: aws.Int64(60),
				})
				require.NoError(t, err, "UpdateMonitor should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createNetworkMonitorClient(t)
				monitorName := vars["MonitorName"].(string)

				out, err := client.GetMonitor(ctx, &networkmonitorsvc.GetMonitorInput{
					MonitorName: aws.String(monitorName),
				})
				require.NoError(t, err)
				assert.Equal(t, int64(30), aws.ToInt64(out.AggregationPeriod),
					"aggregation period should be restored to 30 after drift correction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// VPCLattice (import + drift only — success test exists in terraform_test.go)
// ---------------------------------------------------------------------------

// TestTerraformImport_VPCLattice verifies that a VPC Lattice service network
// created via the SDK can be imported into Terraform state without drift.
func TestTerraformImport_VPCLattice(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "service_network",
			fixture:         "vpclattice/import",
			resourceAddress: "aws_vpclattice_service_network.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createVPCLatticeClient(t)
				networkName := "tf-vl-import-" + uuid.NewString()[:8]

				out, err := client.CreateServiceNetwork(ctx, &vpclatticesvc.CreateServiceNetworkInput{
					Name:     aws.String(networkName),
					AuthType: vpclatticeTypes.AuthTypeNone,
				})
				require.NoError(t, err, "CreateServiceNetwork should succeed")

				networkID := aws.ToString(out.Id)
				t.Cleanup(func() {
					cleanupCtx, cancel := cleanupContext(t)
					defer cancel()

					_, _ = client.DeleteServiceNetwork(cleanupCtx, &vpclatticesvc.DeleteServiceNetworkInput{
						ServiceNetworkIdentifier: aws.String(networkID),
					})
				})

				return map[string]any{"NetworkName": networkName}, networkID
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runImportTest(t, tc)
		})
	}
}

// TestTerraformDrift_VPCLattice verifies that Terraform detects and corrects
// drift when a VPC Lattice service network's tags are changed via the SDK.
func TestTerraformDrift_VPCLattice(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "tags_changed",
			fixture: "vpclattice/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"NetworkName": "tf-vl-drift-" + uuid.NewString()[:8],
					"Tag":         "test",
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createVPCLatticeClient(t)
				networkName := vars["NetworkName"].(string)

				listOut, err := client.ListServiceNetworks(ctx, &vpclatticesvc.ListServiceNetworksInput{})
				require.NoError(t, err)

				var networkARN string
				for _, sn := range listOut.Items {
					if aws.ToString(sn.Name) == networkName {
						networkARN = aws.ToString(sn.Arn)

						break
					}
				}
				require.NotEmpty(t, networkARN, "service network %q must exist to mutate", networkName)

				_, err = client.TagResource(ctx, &vpclatticesvc.TagResourceInput{
					ResourceArn: aws.String(networkARN),
					Tags:        map[string]string{"Env": "drifted"},
				})
				require.NoError(t, err, "TagResource should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createVPCLatticeClient(t)
				networkName := vars["NetworkName"].(string)

				listOut, err := client.ListServiceNetworks(ctx, &vpclatticesvc.ListServiceNetworksInput{})
				require.NoError(t, err)

				found := false
				for _, sn := range listOut.Items {
					if aws.ToString(sn.Name) == networkName {
						found = true

						break
					}
				}
				assert.True(t, found, "service network %q should still exist after drift correction", networkName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDriftTest(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-service e2e: DynamoDB Streams -> Lambda (wiring receipt)
// ---------------------------------------------------------------------------

// TestTerraform_DDBStreams_Lambda_WiringReceipt verifies that a DynamoDB table
// with streams enabled can be wired to a Lambda function via an event source
// mapping, and the mapping is retrievable (cross-service routing receipt).
func TestTerraform_DDBStreams_Lambda_WiringReceipt(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "ddb_streams_to_lambda",
			fixture: "dynamodbstreams/esm",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]
				src := "def handler(event, context):\n    return {'statusCode': 200}\n"
				zipPath := makePyZip(t, dir, src)

				return map[string]any{
					"TableName": "tf-ddb-esm-" + id,
					"FuncName":  "tf-ddb-esm-fn-" + id,
					"RoleName":  "tf-ddb-esm-role-" + id,
					"ZipPath":   zipPath,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				ddbClient := createDynamoDBClient(t)
				lambdaClient := createLambdaClient(t)
				tableName := vars["TableName"].(string)
				funcName := vars["FuncName"].(string)

				// Verify stream is enabled on the table.
				descOut, err := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(tableName),
				})
				require.NoError(t, err, "DescribeTable should succeed")
				streamARN := aws.ToString(descOut.Table.LatestStreamArn)
				require.NotEmpty(t, streamARN, "table should have a stream ARN")

				// Verify the ESM is wired up.
				listOut, err := lambdaClient.ListEventSourceMappings(ctx, &lambdasvc.ListEventSourceMappingsInput{
					FunctionName: aws.String(funcName),
				})
				require.NoError(t, err, "ListEventSourceMappings should succeed")

				found := false
				for _, m := range listOut.EventSourceMappings {
					if strings.Contains(aws.ToString(m.EventSourceArn), tableName) {
						found = true

						break
					}
				}
				assert.True(t, found, "DDB stream ESM for table %q should be wired to function %q", tableName, funcName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
