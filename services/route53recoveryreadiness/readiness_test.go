package route53recoveryreadiness_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	r53 "github.com/blackbirdworks/gopherstack/services/route53"
	sqsbk "github.com/blackbirdworks/gopherstack/services/sqs"
)

// checkOf creates a resource set of one type, a readiness check over it and returns the check name.
func (f *fixture) checkOf(t *testing.T, typ string, resources ...types.Resource) string {
	t.Helper()

	ctx := t.Context()
	_, err := f.client.CreateResourceSet(ctx, &sdk.CreateResourceSetInput{
		ResourceSetName: aws.String("rs"), ResourceSetType: aws.String(typ), Resources: resources,
	})
	require.NoError(t, err)
	_, err = f.client.CreateReadinessCheck(
		ctx,
		&sdk.CreateReadinessCheckInput{ReadinessCheckName: aws.String("rc"), ResourceSetName: aws.String("rs")},
	)
	require.NoError(t, err)

	return "rc"
}

func (f *fixture) readiness(t *testing.T, check string) types.Readiness {
	t.Helper()

	out, err := f.client.GetReadinessCheckStatus(
		t.Context(),
		&sdk.GetReadinessCheckStatusInput{ReadinessCheckName: aws.String(check)},
	)
	require.NoError(t, err)

	return out.Readiness
}

func (f *fixture) rules(t *testing.T, check, resource string) map[string]types.Readiness {
	t.Helper()

	out, err := f.client.GetReadinessCheckResourceStatus(t.Context(), &sdk.GetReadinessCheckResourceStatusInput{
		ReadinessCheckName: aws.String(check), ResourceIdentifier: aws.String(resource),
	})
	require.NoError(t, err)

	got := map[string]types.Readiness{}
	for _, r := range out.Rules {
		got[aws.ToString(r.RuleId)] = r.Readiness
	}

	return got
}

func TestReadinessFollowsSiblingState(t *testing.T) {
	t.Parallel()

	t.Run("sqs queue", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		q := queueARN("orders")
		rc := f.checkOf(t, "AWS::SQS::Queue", types.Resource{ResourceArn: aws.String(q)})

		assert.Equal(t, types.ReadinessNotReady, f.readiness(t, rc), "the queue does not exist yet")
		assert.Equal(t, map[string]types.Readiness{"SqsQueueExists": types.ReadinessNotReady}, f.rules(t, rc, q))

		_, err := f.sqs.CreateQueue(&sqsbk.CreateQueueInput{QueueName: "orders"})
		require.NoError(t, err)

		assert.Equal(t, types.ReadinessReady, f.readiness(t, rc))
		assert.Equal(t, map[string]types.Readiness{"SqsQueueExists": types.ReadinessReady}, f.rules(t, rc, q))
	})

	t.Run("route53 health check", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		hc, err := f.r53.CreateHealthCheck(
			"ref",
			r53.HealthCheckConfig{Type: r53.HealthCheckTypeTCP, IPAddress: "192.0.2.1", Port: 80},
		)
		require.NoError(t, err)

		arn := "arn:aws:route53:::healthcheck/" + hc.ID
		rc := f.checkOf(t, "AWS::Route53::HealthCheck", types.Resource{ResourceArn: aws.String(arn)})

		assert.Equal(t, types.ReadinessReady, f.readiness(t, rc))

		require.NoError(t, f.r53.SetHealthCheckStatus(hc.ID, "Unhealthy"))
		assert.Equal(t, types.ReadinessNotReady, f.readiness(t, rc))
		assert.Equal(t, map[string]types.Readiness{
			"Route53HealthCheckExists": types.ReadinessReady, "Route53HealthCheckHealthy": types.ReadinessNotReady,
		}, f.rules(t, rc, arn))
	})

	t.Run("dns target record", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		zone, err := f.r53.CreateHostedZone("example.com.", "zref", "", false, "", "", "")
		require.NoError(t, err)

		dns := &types.DNSTargetResource{
			DomainName: aws.String("app.example.com"), RecordType: aws.String("A"),
			HostedZoneArn: aws.String("arn:aws:route53:::hostedzone/" + zone.ID),
		}
		rc := f.checkOf(
			t,
			"AWS::Route53RecoveryReadiness::DNSTargetResource",
			types.Resource{DnsTargetResource: dns, ComponentId: aws.String("app")},
		)
		assert.Equal(t, types.ReadinessNotReady, f.readiness(t, rc))

		_, err = f.r53.ChangeResourceRecordSets(
			zone.ID,
			[]r53.Change{{Action: r53.ChangeActionCreate, ResourceRecordSet: r53.ResourceRecordSet{
				Name: "app.example.com.", Type: "A", TTL: 60, Records: []r53.ResourceRecord{{Value: "192.0.2.10"}},
			}}},
		)
		require.NoError(t, err)

		assert.Equal(t, types.ReadinessReady, f.readiness(t, rc))
		assert.Equal(
			t,
			map[string]types.Readiness{"DnsTargetRecordExists": types.ReadinessReady},
			f.rules(t, rc, "app"),
		)
	})

	t.Run("unreachable sibling is unknown", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		arn := "arn:aws:kafka:us-east-1:123456789012:cluster/c/1"
		rc := f.checkOf(t, "AWS::MSK::Cluster", types.Resource{ResourceArn: aws.String(arn)})

		out, err := f.client.GetReadinessCheckStatus(
			t.Context(),
			&sdk.GetReadinessCheckStatusInput{ReadinessCheckName: aws.String(rc)},
		)
		require.NoError(t, err)
		assert.Equal(t, types.ReadinessUnknown, out.Readiness)
		assert.Equal(t, map[string]types.Readiness{
			"MskClusterExists": types.ReadinessUnknown, "MskClusterActive": types.ReadinessUnknown,
		}, f.rules(t, rc, arn))
	})

	t.Run("cross account needs authorization", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		arn := "arn:aws:sqs:us-east-1:999999999999:theirs"
		rc := f.checkOf(t, "AWS::SQS::Queue", types.Resource{ResourceArn: aws.String(arn)})

		assert.Equal(t, types.ReadinessNotAuthorized, f.readiness(t, rc))

		_, err := f.client.CreateCrossAccountAuthorization(t.Context(), &sdk.CreateCrossAccountAuthorizationInput{
			CrossAccountAuthorization: aws.String("arn:aws:iam::999999999999:root"),
		})
		require.NoError(t, err)

		assert.Equal(
			t,
			types.ReadinessNotReady,
			f.readiness(t, rc),
			"authorized, but the queue is not in this emulator",
		)
	})

	t.Run("empty resource set is unknown", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		rc := f.checkOf(t, "AWS::SQS::Queue", []types.Resource{}...)
		assert.Equal(t, types.ReadinessUnknown, f.readiness(t, rc))
	})
}

func TestReadinessSummaries(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	c1, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c1")})
	require.NoError(t, err)
	c2, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c2")})
	require.NoError(t, err)
	g, err := f.client.CreateRecoveryGroup(ctx, &sdk.CreateRecoveryGroupInput{
		RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c1.CellArn), aws.ToString(c2.CellArn)},
	})
	require.NoError(t, err)

	for _, n := range []string{"q1", "q2"} {
		scope := c1.CellArn
		if n == "q2" {
			scope = c2.CellArn
		}

		_, err = f.client.CreateResourceSet(ctx, &sdk.CreateResourceSetInput{
			ResourceSetName: aws.String("rs_" + n),
			ResourceSetType: aws.String("AWS::SQS::Queue"),
			Resources: []types.Resource{
				{ResourceArn: aws.String(queueARN(n)), ReadinessScopes: []string{aws.ToString(scope)}},
			},
		})
		require.NoError(t, err)
		_, err = f.client.CreateReadinessCheck(
			ctx,
			&sdk.CreateReadinessCheckInput{
				ReadinessCheckName: aws.String("rc_" + n),
				ResourceSetName:    aws.String("rs_" + n),
			},
		)
		require.NoError(t, err)
	}

	_, err = f.sqs.CreateQueue(&sqsbk.CreateQueueInput{QueueName: "q1"})
	require.NoError(t, err)

	cell1, err := f.client.GetCellReadinessSummary(ctx, &sdk.GetCellReadinessSummaryInput{CellName: aws.String("c1")})
	require.NoError(t, err)
	assert.Equal(t, types.ReadinessReady, cell1.Readiness)
	require.Len(t, cell1.ReadinessChecks, 1)
	assert.Equal(t, "rc_q1", aws.ToString(cell1.ReadinessChecks[0].ReadinessCheckName))

	cell2, err := f.client.GetCellReadinessSummary(ctx, &sdk.GetCellReadinessSummaryInput{CellName: aws.String("c2")})
	require.NoError(t, err)
	assert.Equal(t, types.ReadinessNotReady, cell2.Readiness)

	group, err := f.client.GetRecoveryGroupReadinessSummary(
		ctx,
		&sdk.GetRecoveryGroupReadinessSummaryInput{RecoveryGroupName: g.RecoveryGroupName},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ReadinessNotReady, group.Readiness, "one not-ready check makes the group not ready")
	assert.Len(t, group.ReadinessChecks, 2)

	_, err = f.sqs.CreateQueue(&sqsbk.CreateQueueInput{QueueName: "q2"})
	require.NoError(t, err)

	group, err = f.client.GetRecoveryGroupReadinessSummary(
		ctx,
		&sdk.GetRecoveryGroupReadinessSummaryInput{RecoveryGroupName: g.RecoveryGroupName},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ReadinessReady, group.Readiness)

	empty, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("empty")})
	require.NoError(t, err)
	got, err := f.client.GetCellReadinessSummary(ctx, &sdk.GetCellReadinessSummaryInput{CellName: empty.CellName})
	require.NoError(t, err)
	assert.Equal(t, types.ReadinessUnknown, got.Readiness, "no check covers the cell, so nothing supports READY")

	_, err = f.client.GetCellReadinessSummary(ctx, &sdk.GetCellReadinessSummaryInput{CellName: aws.String("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestArchitectureRecommendations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	c1, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c1")})
	require.NoError(t, err)
	_, err = f.client.CreateRecoveryGroup(
		ctx,
		&sdk.CreateRecoveryGroupInput{RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c1.CellArn)}},
	)
	require.NoError(t, err)

	texts := func() []string {
		out, rErr := f.client.GetArchitectureRecommendations(
			ctx,
			&sdk.GetArchitectureRecommendationsInput{RecoveryGroupName: aws.String("g")},
		)
		require.NoError(t, rErr)
		require.NotNil(t, out.LastAuditTimestamp)

		s := make([]string, 0, len(out.Recommendations))
		for _, r := range out.Recommendations {
			s = append(s, aws.ToString(r.RecommendationText))
		}

		return s
	}

	got := texts()
	require.Len(t, got, 2)
	assert.Contains(t, got[0], "single cell")
	assert.Contains(t, got[1], "No resource set is scoped")

	c2, err := f.client.CreateCell(ctx, &sdk.CreateCellInput{CellName: aws.String("c2")})
	require.NoError(t, err)
	_, err = f.client.UpdateRecoveryGroup(ctx, &sdk.UpdateRecoveryGroupInput{
		RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c1.CellArn), aws.ToString(c2.CellArn)},
	})
	require.NoError(t, err)
	_, err = f.client.CreateResourceSet(ctx, &sdk.CreateResourceSetInput{
		ResourceSetName: aws.String("rs"),
		ResourceSetType: aws.String("AWS::SQS::Queue"),
		Resources: []types.Resource{
			{ResourceArn: aws.String(queueARN("q")), ReadinessScopes: []string{aws.ToString(c1.CellArn)}},
		},
	})
	require.NoError(t, err)

	got = texts()
	require.Len(t, got, 2)
	assert.Contains(t, got[0], "Resource set rs")
	assert.Contains(t, got[0], "no readiness check")
	assert.Contains(t, got[1], "AWS::SQS::Queue")
	assert.Contains(t, got[1], aws.ToString(c2.CellArn))

	_, err = f.client.GetArchitectureRecommendations(
		ctx,
		&sdk.GetArchitectureRecommendationsInput{RecoveryGroupName: aws.String("nope")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestListRules(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	all, err := f.client.ListRules(t.Context(), &sdk.ListRulesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(all.Rules), 10)

	one, err := f.client.ListRules(t.Context(), &sdk.ListRulesInput{ResourceType: aws.String("AWS::SQS::Queue")})
	require.NoError(t, err)
	require.Len(t, one.Rules, 1)
	assert.Equal(t, "SqsQueueExists", aws.ToString(one.Rules[0].RuleId))
	assert.NotEmpty(t, aws.ToString(one.Rules[0].RuleDescription))

	none, err := f.client.ListRules(t.Context(), &sdk.ListRulesInput{ResourceType: aws.String("AWS::Nope::Thing")})
	require.NoError(t, err)
	assert.Empty(t, none.Rules)
}

func TestResourceStatusUnknownIdentifier(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	rc := f.checkOf(t, "AWS::SQS::Queue", types.Resource{ResourceArn: aws.String(queueARN("q"))})

	_, err := f.client.GetReadinessCheckResourceStatus(t.Context(), &sdk.GetReadinessCheckResourceStatusInput{
		ReadinessCheckName: aws.String(rc), ResourceIdentifier: aws.String("arn:aws:sqs:us-east-1:123456789012:other"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.GetReadinessCheckStatus(
		t.Context(),
		&sdk.GetReadinessCheckStatusInput{ReadinessCheckName: aws.String("none")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
