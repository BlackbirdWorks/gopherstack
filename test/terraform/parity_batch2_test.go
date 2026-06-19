package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsvc "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	cleanroomssvc "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	cleanroomstypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dlmsvc "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	networkmonitorsvc "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	omicssvc "github.com/aws/aws-sdk-go-v2/service/omics"
	vpclatticesvc "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticeTypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// BedrockAgent
// ---------------------------------------------------------------------------

// TestTerraform_BedrockAgent provisions a Bedrock Agent via Terraform and
// verifies it appears in ListAgents.
func TestTerraform_BedrockAgent(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "bedrockagent/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"AgentName": "tf-ba-" + uuid.NewString()[:8],
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createBedrockAgentClient(t)
				agentName := vars["AgentName"].(string)

				out, err := client.ListAgents(ctx, &bedrockagentsvc.ListAgentsInput{})
				require.NoError(t, err, "ListAgents should succeed after terraform apply")

				found := false
				for _, a := range out.AgentSummaries {
					if aws.ToString(a.AgentName) == agentName {
						found = true

						break
					}
				}
				assert.True(t, found, "agent %q should appear in ListAgents", agentName)
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

// TestTerraformImport_BedrockAgent verifies that a Bedrock Agent created via
// the SDK can be imported into Terraform state without drift.
func TestTerraformImport_BedrockAgent(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "agent",
			fixture:         "bedrockagent/import",
			resourceAddress: "aws_bedrockagent_agent.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createBedrockAgentClient(t)
				agentName := "tf-ba-import-" + uuid.NewString()[:8]

				out, err := client.CreateAgent(ctx, &bedrockagentsvc.CreateAgentInput{
					AgentName:            aws.String(agentName),
					AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/bedrock-agent-role"),
					FoundationModel:      aws.String("anthropic.claude-3-sonnet-20240229-v1:0"),
					Instruction:          aws.String("You are a helpful assistant."),
				})
				require.NoError(t, err, "CreateAgent should succeed")

				agentID := aws.ToString(out.Agent.AgentId)
				t.Cleanup(func() {
					_, _ = client.DeleteAgent(ctx, &bedrockagentsvc.DeleteAgentInput{
						AgentId: aws.String(agentID),
					})
				})

				return map[string]any{"AgentName": agentName}, agentID
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

// TestTerraformDrift_BedrockAgent verifies that Terraform detects and corrects
// drift when a Bedrock Agent's instruction is changed directly via the SDK.
func TestTerraformDrift_BedrockAgent(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "instruction_changed",
			fixture: "bedrockagent/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"AgentName":   "tf-ba-drift-" + uuid.NewString()[:8],
					"Instruction": "You are a helpful assistant.",
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createBedrockAgentClient(t)

				// Find the agent by name.
				listOut, err := client.ListAgents(ctx, &bedrockagentsvc.ListAgentsInput{})
				require.NoError(t, err, "ListAgents should succeed")

				agentName := vars["AgentName"].(string)
				var agentID string
				for _, a := range listOut.AgentSummaries {
					if aws.ToString(a.AgentName) == agentName {
						agentID = aws.ToString(a.AgentId)

						break
					}
				}
				require.NotEmpty(t, agentID, "agent %q must exist to mutate", agentName)

				_, err = client.UpdateAgent(ctx, &bedrockagentsvc.UpdateAgentInput{
					AgentId:              aws.String(agentID),
					AgentName:            aws.String(agentName),
					AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/bedrock-agent-role"),
					FoundationModel:      aws.String("anthropic.claude-3-sonnet-20240229-v1:0"),
					Instruction:          aws.String("You are a different assistant."),
				})
				require.NoError(t, err, "UpdateAgent should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createBedrockAgentClient(t)
				agentName := vars["AgentName"].(string)

				listOut, err := client.ListAgents(ctx, &bedrockagentsvc.ListAgentsInput{})
				require.NoError(t, err)

				found := false
				for _, a := range listOut.AgentSummaries {
					if aws.ToString(a.AgentName) == agentName {
						found = true

						break
					}
				}
				assert.True(t, found, "agent %q should still exist after drift correction", agentName)
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
// CleanRooms
// ---------------------------------------------------------------------------

// TestTerraform_CleanRooms provisions a CleanRooms collaboration via Terraform
// and verifies it appears in ListCollaborations.
func TestTerraform_CleanRooms(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cleanrooms/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"CollabName":  "tf-cr-" + id,
					"Description": "tf-test-desc-" + id,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCleanRoomsClient(t)
				collabName := vars["CollabName"].(string)

				out, err := client.ListCollaborations(ctx, &cleanroomssvc.ListCollaborationsInput{})
				require.NoError(t, err, "ListCollaborations should succeed after terraform apply")

				found := false
				for _, c := range out.CollaborationList {
					if aws.ToString(c.Name) == collabName {
						found = true

						break
					}
				}
				assert.True(t, found, "collaboration %q should appear in ListCollaborations", collabName)
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

// TestTerraformImport_CleanRooms verifies that a CleanRooms collaboration
// created via the SDK can be imported into Terraform state without drift.
func TestTerraformImport_CleanRooms(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "collaboration",
			fixture:         "cleanrooms/import",
			resourceAddress: "aws_cleanrooms_collaboration.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createCleanRoomsClient(t)
				id := uuid.NewString()[:8]
				collabName := "tf-cr-import-" + id
				description := "tf-test-import-" + id

				out, err := client.CreateCollaboration(ctx, &cleanroomssvc.CreateCollaborationInput{
					Name:                   aws.String(collabName),
					Description:            aws.String(description),
					CreatorDisplayName:     aws.String("tf-test-creator"),
					CreatorMemberAbilities: []cleanroomstypes.MemberAbility{cleanroomstypes.MemberAbilityCanQuery, cleanroomstypes.MemberAbilityCanReceiveResults},
					QueryLogStatus:         cleanroomstypes.CollaborationQueryLogStatusEnabled,
					Members:                []cleanroomstypes.MemberSpecification{},
				})
				require.NoError(t, err, "CreateCollaboration should succeed")

				collabID := aws.ToString(out.Collaboration.Id)
				t.Cleanup(func() {
					_, _ = client.DeleteCollaboration(ctx, &cleanroomssvc.DeleteCollaborationInput{
						CollaborationIdentifier: aws.String(collabID),
					})
				})

				return map[string]any{
					"CollabName":  collabName,
					"Description": description,
				}, collabID
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

// TestTerraformDrift_CleanRooms verifies that Terraform detects and corrects
// drift when a CleanRooms collaboration's description is changed via the SDK.
func TestTerraformDrift_CleanRooms(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "description_changed",
			fixture: "cleanrooms/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"CollabName":  "tf-cr-drift-" + id,
					"Description": "original-desc-" + id,
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCleanRoomsClient(t)
				collabName := vars["CollabName"].(string)

				listOut, err := client.ListCollaborations(ctx, &cleanroomssvc.ListCollaborationsInput{})
				require.NoError(t, err)

				var collabID string
				for _, c := range listOut.CollaborationList {
					if aws.ToString(c.Name) == collabName {
						collabID = aws.ToString(c.Id)

						break
					}
				}
				require.NotEmpty(t, collabID, "collaboration %q must exist to mutate", collabName)

				_, err = client.UpdateCollaboration(ctx, &cleanroomssvc.UpdateCollaborationInput{
					CollaborationIdentifier: aws.String(collabID),
					Description:             aws.String("drifted-desc"),
				})
				require.NoError(t, err, "UpdateCollaboration should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createCleanRoomsClient(t)
				collabName := vars["CollabName"].(string)
				originalDesc := vars["Description"].(string)

				listOut, err := client.ListCollaborations(ctx, &cleanroomssvc.ListCollaborationsInput{})
				require.NoError(t, err)

				var collabID string
				for _, c := range listOut.CollaborationList {
					if aws.ToString(c.Name) == collabName {
						collabID = aws.ToString(c.Id)

						break
					}
				}
				require.NotEmpty(t, collabID)

				getOut, err := client.GetCollaboration(ctx, &cleanroomssvc.GetCollaborationInput{
					CollaborationIdentifier: aws.String(collabID),
				})
				require.NoError(t, err)
				assert.Equal(t, originalDesc, aws.ToString(getOut.Collaboration.Description),
					"description should be restored to Terraform-defined value after drift correction")
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
// DLM (import + drift only — success test exists in services_parity_test.go)
// ---------------------------------------------------------------------------

// TestTerraformImport_DLM verifies that a DLM lifecycle policy created via
// the SDK can be imported into Terraform state without drift.
func TestTerraformImport_DLM(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "lifecycle_policy",
			fixture:         "dlm/import",
			resourceAddress: "aws_dlm_lifecycle_policy.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createDLMClient(t)
				id := uuid.NewString()[:8]
				policyName := "tf-dlm-import-" + id
				roleName := "tf-dlm-import-role-" + id

				out, err := client.CreateLifecyclePolicy(ctx, &dlmsvc.CreateLifecyclePolicyInput{
					Description:      aws.String(policyName),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/" + roleName),
					State:            dlmtypes.SettablePolicyStateValuesEnabled,
					PolicyDetails: &dlmtypes.PolicyDetails{
						ResourceTypes: []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesVolume},
						Schedules: []dlmtypes.Schedule{
							{
								Name: aws.String("daily"),
								CreateRule: &dlmtypes.CreateRule{
									Interval:     aws.Int32(24),
									IntervalUnit: dlmtypes.IntervalUnitValuesHours,
									Times:        []string{"23:45"},
								},
								RetainRule: &dlmtypes.RetainRule{Count: aws.Int32(1)},
							},
						},
						TargetTags: []dlmtypes.Tag{{Key: aws.String("Snapshot"), Value: aws.String("true")}},
					},
				})
				require.NoError(t, err, "CreateLifecyclePolicy should succeed")

				policyID := aws.ToString(out.PolicyId)
				t.Cleanup(func() {
					_, _ = client.DeleteLifecyclePolicy(ctx, &dlmsvc.DeleteLifecyclePolicyInput{
						PolicyId: aws.String(policyID),
					})
				})

				// IAM role must exist for the fixture to be valid.
				iamClient := createIAMClient(t)
				_, _ = iamClient.CreateRole(ctx, &iamsvc.CreateRoleInput{
					RoleName: aws.String(roleName),
					AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"dlm.amazonaws.com"},"Action":"sts:AssumeRole"}]}`),
				})

				return map[string]any{
					"PolicyName": policyName,
					"RoleName":   roleName,
				}, policyID
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

// TestTerraformDrift_DLM verifies that Terraform detects and corrects drift
// when a DLM lifecycle policy's state is changed via the SDK.
func TestTerraformDrift_DLM(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "state_changed",
			fixture: "dlm/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()
				id := uuid.NewString()[:8]

				return map[string]any{
					"PolicyName": "tf-dlm-drift-" + id,
					"RoleName":   "tf-dlm-drift-role-" + id,
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createDLMClient(t)
				policyName := vars["PolicyName"].(string)

				listOut, err := client.GetLifecyclePolicies(ctx, &dlmsvc.GetLifecyclePoliciesInput{})
				require.NoError(t, err)

				var policyID string
				for _, p := range listOut.Policies {
					if aws.ToString(p.Description) == policyName {
						policyID = aws.ToString(p.PolicyId)

						break
					}
				}
				require.NotEmpty(t, policyID, "policy %q must exist to mutate", policyName)

				_, err = client.UpdateLifecyclePolicy(ctx, &dlmsvc.UpdateLifecyclePolicyInput{
					PolicyId:         aws.String(policyID),
					Description:      aws.String(policyName),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/" + vars["RoleName"].(string)),
					State:            dlmtypes.SettablePolicyStateValuesDisabled,
				})
				require.NoError(t, err, "UpdateLifecyclePolicy should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createDLMClient(t)
				policyName := vars["PolicyName"].(string)

				listOut, err := client.GetLifecyclePolicies(ctx, &dlmsvc.GetLifecyclePoliciesInput{})
				require.NoError(t, err)

				for _, p := range listOut.Policies {
					if aws.ToString(p.Description) == policyName {
						assert.Equal(t, dlmtypes.GettablePolicyStateValuesEnabled, p.State,
							"policy state should be restored to ENABLED after drift correction")

						return
					}
				}
				t.Errorf("policy %q not found after drift correction", policyName)
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
// Omics
// ---------------------------------------------------------------------------

// TestTerraform_Omics provisions an Omics reference store via Terraform and
// verifies it appears in ListReferenceStores.
func TestTerraform_Omics(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "omics/success",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"StoreName": "tf-omics-" + uuid.NewString()[:8],
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createOmicsClient(t)
				storeName := vars["StoreName"].(string)

				out, err := client.ListReferenceStores(ctx, &omicssvc.ListReferenceStoresInput{})
				require.NoError(t, err, "ListReferenceStores should succeed after terraform apply")

				found := false
				for _, s := range out.ReferenceStores {
					if aws.ToString(s.Name) == storeName {
						found = true

						break
					}
				}
				assert.True(t, found, "reference store %q should appear in ListReferenceStores", storeName)
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

// TestTerraformImport_Omics verifies that an Omics reference store created via
// the SDK can be imported into Terraform state without drift.
func TestTerraformImport_Omics(t *testing.T) {
	t.Parallel()

	tests := []importTestCase{
		{
			name:            "reference_store",
			fixture:         "omics/import",
			resourceAddress: "aws_omics_reference_store.this",
			createResource: func(t *testing.T, ctx context.Context, _ string) (map[string]any, string) {
				t.Helper()

				client := createOmicsClient(t)
				storeName := "tf-omics-import-" + uuid.NewString()[:8]

				out, err := client.CreateReferenceStore(ctx, &omicssvc.CreateReferenceStoreInput{
					Name: aws.String(storeName),
				})
				require.NoError(t, err, "CreateReferenceStore should succeed")

				storeID := aws.ToString(out.Id)
				t.Cleanup(func() {
					_, _ = client.DeleteReferenceStore(ctx, &omicssvc.DeleteReferenceStoreInput{
						Id: aws.String(storeID),
					})
				})

				return map[string]any{"StoreName": storeName}, storeID
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

// TestTerraformDrift_Omics verifies that Terraform detects and corrects drift
// for an Omics reference store when its tags are changed via the SDK.
func TestTerraformDrift_Omics(t *testing.T) {
	t.Parallel()

	tests := []driftTestCase{
		{
			name:    "tags_changed",
			fixture: "omics/drift",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"StoreName": "tf-omics-drift-" + uuid.NewString()[:8],
				}
			},
			mutate: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createOmicsClient(t)
				storeName := vars["StoreName"].(string)

				listOut, err := client.ListReferenceStores(ctx, &omicssvc.ListReferenceStoresInput{})
				require.NoError(t, err)

				var storeID string
				for _, s := range listOut.ReferenceStores {
					if aws.ToString(s.Name) == storeName {
						storeID = aws.ToString(s.Id)

						break
					}
				}
				require.NotEmpty(t, storeID, "store %q must exist to mutate", storeName)

				storeARN := "arn:aws:omics:us-east-1:000000000000:referenceStore/" + storeID
				_, err = client.TagResource(ctx, &omicssvc.TagResourceInput{
					ResourceArn: aws.String(storeARN),
					Tags:        map[string]string{"Env": "drifted"},
				})
				require.NoError(t, err, "TagResource should succeed to introduce drift")
			},
			verifyAfter: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createOmicsClient(t)
				storeName := vars["StoreName"].(string)

				listOut, err := client.ListReferenceStores(ctx, &omicssvc.ListReferenceStoresInput{})
				require.NoError(t, err)

				var storeID string
				for _, s := range listOut.ReferenceStores {
					if aws.ToString(s.Name) == storeName {
						storeID = aws.ToString(s.Id)

						break
					}
				}
				require.NotEmpty(t, storeID, "store %q must still exist after drift correction", storeName)
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
					_, _ = client.DeleteMonitor(ctx, &networkmonitorsvc.DeleteMonitorInput{
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
					_, _ = client.DeleteServiceNetwork(ctx, &vpclatticesvc.DeleteServiceNetworkInput{
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
