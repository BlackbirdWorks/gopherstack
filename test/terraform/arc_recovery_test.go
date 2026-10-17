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

// arcControlConfigUpdate builds the update overlay. Provider v5 re-reads a safety rule after
// UpdateSafetyRule via DescribeControlPanel and fails against real AWS too, so rules only update in v6.
func arcControlConfigUpdate(suffix string, rulesUpdatable bool) map[string]any {
	update := map[string]any{
		"PanelName":     "arc-panel2-" + suffix,
		"SecondaryName": "arc-secondary2-" + suffix,
		"Env":           "prod",
	}

	if rulesUpdatable {
		update["WaitMs"] = 6000
		update["AssertionName"] = "arc-at-least-two-" + suffix
		update["GatingName"] = "arc-gate-secondary2-" + suffix
	}

	return update
}

// arcControlConfigTags returns the tag map for arn from the recovery control config API.
func arcControlConfigTags(ctx context.Context, t *testing.T, arn string) map[string]string {
	t.Helper()

	client := rccsvc.NewFromConfig(megaConfig(t), func(o *rccsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListTagsForResource(ctx, &rccsvc.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	require.NoError(t, err)

	return out.Tags
}

// TestTerraform_ARCRecoveryControlConfig provisions an ARC cluster, control panel, routing controls,
// assertion and gating safety rules, and a RECOVERY_CONTROL health check, then updates in place.
func TestTerraform_ARCRecoveryControlConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		providerFn func(addr string) string
		name       string
		fixture    string
		tagged     bool // v6 only: tags, network type and updatable safety rules
	}{
		{name: "provider_v5", fixture: "arc-recovery-control-config"},
		{
			name:       "provider_v6_tags",
			fixture:    "arc-recovery-control-config-v6",
			providerFn: arczonalshiftProviderBlock,
			tagged:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			suffix := uuid.NewString()[:8]

			runTFTest(t, tfTestCase{
				name:       tc.name,
				fixture:    tc.fixture,
				providerFn: tc.providerFn,
				noDrift:    true,
				setup: func(t *testing.T, _ string) map[string]any {
					t.Helper()

					return map[string]any{
						"Suffix":        suffix,
						"PanelName":     "arc-panel-" + suffix,
						"SecondaryName": "arc-secondary-" + suffix,
						"WaitMs":        5000,
						"Threshold":     1,
						"AssertionName": "arc-at-least-one-" + suffix,
						"GatingName":    "arc-gate-secondary-" + suffix,
						"Env":           "dev",
					}
				},
				update: arcControlConfigUpdate(suffix, tc.tagged),
				verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
					t.Helper()

					verifyARCControlConfig(ctx, t, vars, tc.tagged)
					verifyARCRecoveryControlHealthCheck(ctx, t, suffix)
				},
				verifyUpdate: func(t *testing.T, ctx context.Context, vars map[string]any) {
					t.Helper()

					verifyARCControlConfig(ctx, t, vars, tc.tagged)
				},
			})
		})
	}
}

// TestTerraform_ARCReadiness provisions the readiness cell, recovery group, resource set and
// readiness check resources, then updates their tags.
func TestTerraform_ARCReadiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify  func(ctx context.Context, t *testing.T, vars map[string]any)
		name    string
		fixture string
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
		{
			name:    "dns_target_resource_set",
			fixture: "arc-readiness-dns-target-resource",
			verify:  verifyARCReadinessDNSTarget,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runTFTest(t, tfTestCase{
				name:    tc.name,
				fixture: tc.fixture,
				noDrift: true,
				setup: func(t *testing.T, _ string) map[string]any {
					t.Helper()

					return map[string]any{"Suffix": uuid.NewString()[:8], "Env": "dev"}
				},
				update: map[string]any{"Env": "prod"},
				verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
					t.Helper()
					tc.verify(ctx, t, vars)
				},
				verifyUpdate: func(t *testing.T, ctx context.Context, vars map[string]any) {
					t.Helper()
					tc.verify(ctx, t, vars)
				},
			})
		})
	}
}

// TestTerraform_ARCAutoshiftObserverNotification toggles the account-level observer notification status.
func TestTerraform_ARCAutoshiftObserverNotification(t *testing.T) {
	t.Parallel()

	verify := func(want arczstypes.AutoshiftObserverNotificationStatus) func(*testing.T, context.Context, map[string]any) {
		return func(t *testing.T, ctx context.Context, _ map[string]any) {
			t.Helper()

			client := arczonalshiftsvc.NewFromConfig(megaConfig(t), func(o *arczonalshiftsvc.Options) {
				o.BaseEndpoint = aws.String(endpoint)
			})

			got, err := client.GetAutoshiftObserverNotificationStatus(ctx,
				&arczonalshiftsvc.GetAutoshiftObserverNotificationStatusInput{})
			require.NoError(t, err)
			assert.Equal(t, want, got.Status)
		}
	}

	runTFTest(t, tfTestCase{
		name:         "enabled_then_disabled",
		fixture:      "arc-autoshift-observer-notification",
		providerFn:   arczonalshiftProviderBlock,
		noDrift:      true,
		setup:        func(*testing.T, string) map[string]any { return map[string]any{"Status": "ENABLED"} },
		update:       map[string]any{"Status": "DISABLED"},
		verify:       verify(arczstypes.AutoshiftObserverNotificationStatusEnabled),
		verifyUpdate: verify(arczstypes.AutoshiftObserverNotificationStatusDisabled),
	})
}

func verifyARCControlConfig(ctx context.Context, t *testing.T, vars map[string]any, tagged bool) {
	t.Helper()

	suffix := vars["Suffix"].(string)
	panelName := vars["PanelName"].(string)
	secondaryName := vars["SecondaryName"].(string)
	waitMs := int32(vars["WaitMs"].(int))
	threshold := int32(vars["Threshold"].(int))
	wantTags := map[string]string{"env": vars["Env"].(string)}

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

			if tagged {
				assert.Equal(t, rcctypes.NetworkTypeIpv4, c.NetworkType)
			}
		}
	}

	require.NotEmpty(t, clusterARN, "cluster should be listed")

	if tagged {
		assert.Equal(t, wantTags, arcControlConfigTags(ctx, t, clusterARN))
	}

	panels, err := client.ListControlPanels(ctx, &rccsvc.ListControlPanelsInput{})
	require.NoError(t, err)

	var panelARN string

	for _, p := range panels.ControlPanels {
		if aws.ToString(p.Name) == panelName {
			panelARN = aws.ToString(p.ControlPanelArn)

			assert.Equal(t, clusterARN, aws.ToString(p.ClusterArn))
			assert.Equal(t, int32(2), aws.ToInt32(p.RoutingControlCount))
		}
	}

	require.NotEmpty(t, panelARN, "control panel should be listed")

	if tagged {
		assert.Equal(t, wantTags, arcControlConfigTags(ctx, t, panelARN))
	}

	controls, err := client.ListRoutingControls(ctx, &rccsvc.ListRoutingControlsInput{
		ControlPanelArn: aws.String(panelARN),
	})
	require.NoError(t, err)

	names := map[string]string{}
	for _, rc := range controls.RoutingControls {
		names[aws.ToString(rc.Name)] = aws.ToString(rc.RoutingControlArn)
	}

	require.Contains(t, names, "arc-primary-"+suffix)
	require.Contains(t, names, secondaryName)

	rules, err := client.ListSafetyRules(ctx, &rccsvc.ListSafetyRulesInput{ControlPanelArn: aws.String(panelARN)})
	require.NoError(t, err)

	var sawAssertion, sawGating bool

	for _, r := range rules.SafetyRules {
		if as := r.ASSERTION; as != nil && aws.ToString(as.Name) == vars["AssertionName"].(string) {
			sawAssertion = true

			assert.ElementsMatch(t, []string{names["arc-primary-"+suffix], names[secondaryName]}, as.AssertedControls)
			assert.Equal(t, waitMs, aws.ToInt32(as.WaitPeriodMs))
			assert.Equal(t, rcctypes.RuleTypeAtleast, as.RuleConfig.Type)
			assert.Equal(t, threshold, aws.ToInt32(as.RuleConfig.Threshold))
			assert.False(t, aws.ToBool(as.RuleConfig.Inverted))

			if tagged {
				assert.Equal(t, wantTags, arcControlConfigTags(ctx, t, aws.ToString(as.SafetyRuleArn)))
			}
		}

		if g := r.GATING; g != nil && aws.ToString(g.Name) == vars["GatingName"].(string) {
			sawGating = true

			assert.Equal(t, []string{names["arc-primary-"+suffix]}, g.GatingControls)
			assert.Equal(t, []string{names[secondaryName]}, g.TargetControls)
			assert.Equal(t, waitMs, aws.ToInt32(g.WaitPeriodMs))
			assert.Equal(t, rcctypes.RuleTypeOr, g.RuleConfig.Type)
			assert.True(t, aws.ToBool(g.RuleConfig.Inverted))
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

func verifyARCReadinessCellsAndGroup(ctx context.Context, t *testing.T, vars map[string]any) {
	t.Helper()

	suffix := vars["Suffix"].(string)
	wantTags := map[string]string{"env": vars["Env"].(string)}

	client := rrsvc.NewFromConfig(megaConfig(t), func(o *rrsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	west, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_west_" + suffix)})
	require.NoError(t, err)

	nested, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_west_az_" + suffix)})
	require.NoError(t, err)
	assert.Equal(t, []string{aws.ToString(nested.CellArn)}, west.Cells)
	assert.Equal(t, []string{aws.ToString(west.CellArn)}, nested.ParentReadinessScopes)
	assert.Equal(t, wantTags, west.Tags)

	east, err := client.GetCell(ctx, &rrsvc.GetCellInput{CellName: aws.String("arc_east_" + suffix)})
	require.NoError(t, err)
	assert.Equal(t, wantTags, east.Tags)

	group, err := client.GetRecoveryGroup(ctx, &rrsvc.GetRecoveryGroupInput{
		RecoveryGroupName: aws.String("arc_group_" + suffix),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{aws.ToString(west.CellArn), aws.ToString(east.CellArn)}, group.Cells)
	assert.Equal(t, wantTags, group.Tags)
}

func verifyARCReadinessResourceSetAndCheck(ctx context.Context, t *testing.T, vars map[string]any) {
	t.Helper()

	suffix := vars["Suffix"].(string)
	wantTags := map[string]string{"env": vars["Env"].(string)}

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
	assert.Equal(t, wantTags, set.Tags)
	require.Len(t, set.Resources, 1)
	assert.Contains(t, aws.ToString(set.Resources[0].ResourceArn), "arc-orders-"+suffix)
	assert.Equal(t, []string{aws.ToString(west.CellArn)}, set.Resources[0].ReadinessScopes)

	check, err := client.GetReadinessCheck(ctx, &rrsvc.GetReadinessCheckInput{
		ReadinessCheckName: aws.String("arc_check_" + suffix),
	})
	require.NoError(t, err)
	assert.Equal(t, "arc_queues_"+suffix, aws.ToString(check.ResourceSet))
	assert.Equal(t, wantTags, check.Tags)

	status, err := client.GetReadinessCheckStatus(ctx, &rrsvc.GetReadinessCheckStatusInput{
		ReadinessCheckName: aws.String("arc_check_" + suffix),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, status.Readiness)
}

func verifyARCReadinessDNSTarget(ctx context.Context, t *testing.T, vars map[string]any) {
	t.Helper()

	suffix := vars["Suffix"].(string)

	client := rrsvc.NewFromConfig(megaConfig(t), func(o *rrsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	set, err := client.GetResourceSet(ctx, &rrsvc.GetResourceSetInput{ResourceSetName: aws.String("arc_dns_" + suffix)})
	require.NoError(t, err)
	assert.Equal(t, "AWS::Route53RecoveryReadiness::DNSTargetResource", aws.ToString(set.ResourceSetType))
	assert.Equal(t, map[string]string{"env": vars["Env"].(string)}, set.Tags)
	require.Len(t, set.Resources, 1)

	dns := set.Resources[0].DnsTargetResource
	require.NotNil(t, dns)
	assert.Equal(t, "app.arc-"+suffix+".example.com", aws.ToString(dns.DomainName))
	assert.Equal(t, "A", aws.ToString(dns.RecordType))
	assert.Equal(t, "primary", aws.ToString(dns.RecordSetId))
	require.NotNil(t, dns.TargetResource)
	require.NotNil(t, dns.TargetResource.R53Resource)
	assert.Equal(t, "secondary", aws.ToString(dns.TargetResource.R53Resource.RecordSetId))
}

// TestTerraform_ARCZonalAutoshiftConfiguration provisions a zonal autoshift configuration with
// outcome and blocking alarms and maintenance windows on an Application Load Balancer, then updates it.
func TestTerraform_ARCZonalAutoshiftConfiguration(t *testing.T) {
	t.Parallel()

	type verifyFn = func(*testing.T, context.Context, map[string]any)

	verify := func(status arczstypes.ZonalAutoshiftStatus, blockedWindow string) verifyFn {
		return func(t *testing.T, ctx context.Context, vars map[string]any) {
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

					assert.Equal(t, status, r.ZonalAutoshiftStatus)
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
			assert.Equal(t, []string{blockedWindow}, got.PracticeRunConfiguration.BlockedWindows)
			assert.Equal(t, []string{"2030-12-25"}, got.PracticeRunConfiguration.BlockedDates)
		}
	}

	runTFTest(t, tfTestCase{
		name:       "enabled_with_blocking_alarms",
		fixture:    "arc-zonal-autoshift-configuration",
		providerFn: arczonalshiftProviderBlock,
		noDrift:    true,
		setup: func(t *testing.T, _ string) map[string]any {
			t.Helper()

			vars := vpcCIDRVars(t)
			vars["Suffix"] = uuid.NewString()[:8]
			vars["Status"] = "ENABLED"
			vars["BlockedWindow"] = "Tue:00:00-Tue:04:00"

			return vars
		},
		update:       map[string]any{"Status": "DISABLED", "BlockedWindow": "Wed:00:00-Wed:04:00"},
		verify:       verify(arczstypes.ZonalAutoshiftStatusEnabled, "Tue:00:00-Tue:04:00"),
		verifyUpdate: verify(arczstypes.ZonalAutoshiftStatusDisabled, "Wed:00:00-Wed:04:00"),
	})
}
