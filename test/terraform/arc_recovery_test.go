package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	arczonalshiftsvc "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	arczstypes "github.com/aws/aws-sdk-go-v2/service/arczonalshift/types"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	rccsvc "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	rcctypes "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	rrsvc "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_ARCRecoveryControlConfig provisions an ARC cluster, control panel, routing controls,
// assertion and gating safety rules, and a RECOVERY_CONTROL health check.
func TestTerraform_ARCRecoveryControlConfig(t *testing.T) {
	t.Parallel()

	suffix := uuid.NewString()[:8]

	verifiers := []struct {
		verify func(ctx context.Context, t *testing.T, suffix string)
		name   string
	}{
		{name: "control_config", verify: verifyARCControlConfig},
		{name: "health_check", verify: verifyARCRecoveryControlHealthCheck},
	}

	runTFTest(t, tfTestCase{
		name:    "recovery_control_config",
		fixture: "arc-recovery-control-config",
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			return map[string]any{"Suffix": suffix}
		},
		verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
			t.Helper()

			for _, tt := range verifiers {
				t.Run(tt.name, func(t *testing.T) {
					tt.verify(ctx, t, suffix)
				})
			}
		},
	})
}

// TestTerraform_ARCReadiness provisions the readiness cell, recovery group, resource set and
// readiness check resources.
func TestTerraform_ARCReadiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify  func(ctx context.Context, t *testing.T, suffix string)
		name    string
		fixture string
		skip    string
	}{
		{
			name:    "cells_and_recovery_group",
			fixture: "arc-readiness-cells-and-recovery-group",
			verify:  verifyARCReadinessCellsAndGroup,
		},
		{
			name:    "resource_set_and_readiness_check",
			fixture: "arc-readiness-resource-set-and-check",
			verify:  verifyARCReadinessResourceSetAndCheck,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.skip != "" {
				t.Skip(tc.skip)
			}

			suffix := uuid.NewString()[:8]

			runTFTest(t, tfTestCase{
				name:    tc.name,
				fixture: tc.fixture,
				setup: func(t *testing.T, _ string) map[string]any {
					t.Helper()

					return map[string]any{"Suffix": suffix}
				},
				verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
					t.Helper()
					tc.verify(ctx, t, suffix)
				},
			})
		})
	}
}

func verifyARCControlConfig(ctx context.Context, t *testing.T, suffix string) {
	t.Helper()

	client := rccsvc.NewFromConfig(megaConfig(t), func(o *rccsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	clusters, err := client.ListClusters(ctx, &rccsvc.ListClustersInput{})
	require.NoError(t, err)

	var clusterARN string

	for _, c := range clusters.Clusters {
		if aws.ToString(c.Name) == "arc-cluster-"+suffix {
			clusterARN = aws.ToString(c.ClusterArn)

			assert.Equal(t, rcctypes.StatusDeployed, c.Status)
		}
	}

	require.NotEmpty(t, clusterARN, "cluster should be listed")

	panels, err := client.ListControlPanels(ctx, &rccsvc.ListControlPanelsInput{})
	require.NoError(t, err)

	var panelARN string

	for _, p := range panels.ControlPanels {
		if aws.ToString(p.Name) == "arc-panel-"+suffix {
			panelARN = aws.ToString(p.ControlPanelArn)

			assert.Equal(t, clusterARN, aws.ToString(p.ClusterArn))
			assert.Equal(t, int32(2), aws.ToInt32(p.RoutingControlCount))
		}
	}

	require.NotEmpty(t, panelARN, "control panel should be listed")

	controls, err := client.ListRoutingControls(ctx, &rccsvc.ListRoutingControlsInput{
		ControlPanelArn: aws.String(panelARN),
	})
	require.NoError(t, err)

	names := map[string]string{}
	for _, rc := range controls.RoutingControls {
		names[aws.ToString(rc.Name)] = aws.ToString(rc.RoutingControlArn)
	}

	require.Contains(t, names, "arc-primary-"+suffix)
	require.Contains(t, names, "arc-secondary-"+suffix)

	rules, err := client.ListSafetyRules(ctx, &rccsvc.ListSafetyRulesInput{ControlPanelArn: aws.String(panelARN)})
	require.NoError(t, err)

	var sawAssertion, sawGating bool

	for _, r := range rules.SafetyRules {
		if as := r.ASSERTION; as != nil && aws.ToString(as.Name) == "arc-at-least-one-"+suffix {
			sawAssertion = true

			assert.ElementsMatch(t, []string{names["arc-primary-"+suffix], names["arc-secondary-"+suffix]},
				as.AssertedControls)
			assert.Equal(t, int32(5000), aws.ToInt32(as.WaitPeriodMs))
			assert.Equal(t, rcctypes.RuleTypeAtleast, as.RuleConfig.Type)
		}

		if g := r.GATING; g != nil && aws.ToString(g.Name) == "arc-gate-secondary-"+suffix {
			sawGating = true

			assert.Equal(t, []string{names["arc-primary-"+suffix]}, g.GatingControls)
			assert.Equal(t, []string{names["arc-secondary-"+suffix]}, g.TargetControls)
		}
	}

	assert.True(t, sawAssertion, "assertion safety rule should be listed")
	assert.True(t, sawGating, "gating safety rule should be listed")
}

func verifyARCRecoveryControlHealthCheck(ctx context.Context, t *testing.T, suffix string) {
	t.Helper()

	cc := rccsvc.NewFromConfig(megaConfig(t), func(o *rccsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	r53 := route53svc.NewFromConfig(megaConfig(t), func(o *route53svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	panels, err := cc.ListControlPanels(ctx, &rccsvc.ListControlPanelsInput{})
	require.NoError(t, err)

	var primaryARN string

	for _, p := range panels.ControlPanels {
		if aws.ToString(p.Name) != "arc-panel-"+suffix {
			continue
		}

		controls, listErr := cc.ListRoutingControls(ctx, &rccsvc.ListRoutingControlsInput{
			ControlPanelArn: p.ControlPanelArn,
		})
		require.NoError(t, listErr)

		for _, rc := range controls.RoutingControls {
			if aws.ToString(rc.Name) == "arc-primary-"+suffix {
				primaryARN = aws.ToString(rc.RoutingControlArn)
			}
		}
	}

	require.NotEmpty(t, primaryARN)

	checks, err := r53.ListHealthChecks(ctx, &route53svc.ListHealthChecksInput{})
	require.NoError(t, err)

	var found bool

	for _, hc := range checks.HealthChecks {
		if hc.HealthCheckConfig != nil && hc.HealthCheckConfig.Type == r53types.HealthCheckTypeRecoveryControl &&
			aws.ToString(hc.HealthCheckConfig.RoutingControlArn) == primaryARN {
			found = true
		}
	}

	assert.True(t, found, "RECOVERY_CONTROL health check for the primary routing control should be listed")
}

func verifyARCReadinessCellsAndGroup(ctx context.Context, t *testing.T, suffix string) {
	t.Helper()

	client := rrsvc.NewFromConfig(megaConfig(t), func(o *rrsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	west, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_west_" + suffix)})
	require.NoError(t, err)

	east, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_east_" + suffix)})
	require.NoError(t, err)

	group, err := client.GetRecoveryGroup(ctx, &rrsvc.GetRecoveryGroupInput{
		RecoveryGroupName: aws.String("arc_group_" + suffix),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{aws.ToString(west.CellArn), aws.ToString(east.CellArn)}, group.Cells)
}

func verifyARCReadinessResourceSetAndCheck(ctx context.Context, t *testing.T, suffix string) {
	t.Helper()

	client := rrsvc.NewFromConfig(megaConfig(t), func(o *rrsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	west, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_west_" + suffix)})
	require.NoError(t, err)

	set, err := client.GetResourceSet(ctx, &rrsvc.GetResourceSetInput{
		ResourceSetName: aws.String("arc_queues_" + suffix),
	})
	require.NoError(t, err)
	assert.Equal(t, "AWS::SQS::Queue", aws.ToString(set.ResourceSetType))
	require.Len(t, set.Resources, 1)
	assert.Contains(t, aws.ToString(set.Resources[0].ResourceArn), "arc-orders-"+suffix)
	assert.Equal(t, []string{aws.ToString(west.CellArn)}, set.Resources[0].ReadinessScopes)

	check, err := client.GetReadinessCheck(ctx, &rrsvc.GetReadinessCheckInput{
		ReadinessCheckName: aws.String("arc_check_" + suffix),
	})
	require.NoError(t, err)
	assert.Equal(t, "arc_queues_"+suffix, aws.ToString(check.ResourceSet))

	status, err := client.GetReadinessCheckStatus(ctx, &rrsvc.GetReadinessCheckStatusInput{
		ReadinessCheckName: aws.String("arc_check_" + suffix),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, status.Readiness)
}

// TestTerraform_ARCZonalAutoshiftConfiguration provisions a zonal autoshift configuration with
// outcome and blocking alarms on an Application Load Balancer.
func TestTerraform_ARCZonalAutoshiftConfiguration(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "enabled_with_blocking_alarms",
			fixture:    "arc-zonal-autoshift-configuration",
			providerFn: arczonalshiftProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				vars := vpcCIDRVars(t)
				vars["Suffix"] = uuid.NewString()[:8]

				return vars
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := arczonalshiftsvc.NewFromConfig(megaConfig(t), func(o *arczonalshiftsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				resources, err := client.ListManagedResources(ctx, &arczonalshiftsvc.ListManagedResourcesInput{})
				require.NoError(t, err)

				var arn string

				for _, r := range resources.Items {
					if aws.ToString(r.Name) == "arczs-"+vars["Suffix"].(string) {
						arn = aws.ToString(r.Arn)

						assert.Equal(t, arczstypes.ZonalAutoshiftStatusEnabled, r.ZonalAutoshiftStatus)
					}
				}

				require.NotEmpty(t, arn, "load balancer should be a managed resource")

				got, err := client.GetManagedResource(ctx, &arczonalshiftsvc.GetManagedResourceInput{
					ResourceIdentifier: aws.String(arn),
				})
				require.NoError(t, err)
				require.NotNil(t, got.PracticeRunConfiguration)
				assert.Len(t, got.PracticeRunConfiguration.OutcomeAlarms, 1)
				assert.Len(t, got.PracticeRunConfiguration.BlockingAlarms, 1)
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
