package app

import (
	"github.com/blackbirdworks/gopherstack/internal/routegates"

	"log/slog"
	"net"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster"
	rctypes "github.com/aws/aws-sdk-go-v2/service/route53recoverycluster/types"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	ccTypes "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

// arcStack serves the full production composition root on a real listener, so cluster endpoints resolve back to it.
func arcStack(t *testing.T) aws.Config {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port

	cli := &CLI{AccountID: "000000000000", Region: regionA, Port: ":" + strconv.Itoa(port)}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	registry := service.NewRegistry()
	for _, svc := range services {
		require.NoError(t, registry.Register(svc))
	}

	e := echo.New()
	e.Pre(awsMetaMiddleware(regionA, "000000000000"))
	e.Use(
		service.NewServiceRouter(registry).
			WithTargetGates(routegates.TargetGates()).
			WithCBORGates(routegates.CBORGates()).
			WithPathGates(routegates.PathGates()).
			WithAltGates(routegates.AltGates()).
			RouteHandler(),
	)

	srv := httptest.NewUnstartedServer(e)
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithHTTPClient(testclient.New(t)),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	cfg.BaseEndpoint = aws.String(srv.URL)

	return cfg
}

func TestARCRecoveryControlFullStack(t *testing.T) {
	t.Parallel()

	cfg := arcStack(t)
	ctx := t.Context()
	control := route53recoverycontrolconfig.NewFromConfig(cfg)
	dns := route53.NewFromConfig(cfg)

	cluster, err := control.CreateCluster(
		ctx,
		&route53recoverycontrolconfig.CreateClusterInput{ClusterName: aws.String("prod")},
	)
	require.NoError(t, err)

	clusterArn := cluster.Cluster.ClusterArn
	desc, err := control.DescribeCluster(
		ctx,
		&route53recoverycontrolconfig.DescribeClusterInput{ClusterArn: clusterArn},
	)
	require.NoError(t, err)
	require.Equal(t, ccTypes.StatusDeployed, desc.Cluster.Status)
	require.NotEmpty(t, desc.Cluster.ClusterEndpoints)

	panel, err := control.CreateControlPanel(ctx, &route53recoverycontrolconfig.CreateControlPanelInput{
		ClusterArn: clusterArn, ControlPanelName: aws.String("app"),
	})
	require.NoError(t, err)

	newControl := func(name string) string {
		out, cErr := control.CreateRoutingControl(ctx, &route53recoverycontrolconfig.CreateRoutingControlInput{
			ClusterArn:         clusterArn,
			ControlPanelArn:    panel.ControlPanel.ControlPanelArn,
			RoutingControlName: aws.String(name),
		})
		require.NoError(t, cErr)

		return aws.ToString(out.RoutingControl.RoutingControlArn)
	}

	east, west := newControl("east"), newControl("west")

	rule, err := control.CreateSafetyRule(
		ctx,
		&route53recoverycontrolconfig.CreateSafetyRuleInput{AssertionRule: &ccTypes.NewAssertionRule{
			AssertedControls: []string{
				east,
				west,
			},
			ControlPanelArn: panel.ControlPanel.ControlPanelArn,
			Name:            aws.String("keep-one-on"),
			WaitPeriodMs: aws.Int32(
				0,
			),
			RuleConfig: &ccTypes.RuleConfig{
				Inverted:  aws.Bool(false),
				Threshold: aws.Int32(1),
				Type:      ccTypes.RuleTypeAtleast,
			},
		}},
	)
	require.NoError(t, err)

	endpoint := desc.Cluster.ClusterEndpoints[0]
	data := route53recoverycluster.NewFromConfig(cfg, func(o *route53recoverycluster.Options) {
		o.BaseEndpoint = endpoint.Endpoint
		o.Region = aws.ToString(endpoint.Region)
	})

	set := func(arn string, s rctypes.RoutingControlState) error {
		_, uErr := data.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
			RoutingControlArn: aws.String(arn), RoutingControlState: s,
		})

		return uErr
	}

	zone, err := dns.CreateHostedZone(
		ctx,
		&route53.CreateHostedZoneInput{Name: aws.String("example.com."), CallerReference: aws.String("z")},
	)
	require.NoError(t, err)

	zoneID := aws.ToString(zone.HostedZone.Id)

	hc, err := dns.CreateHealthCheck(
		ctx,
		&route53.CreateHealthCheckInput{
			CallerReference: aws.String("hc"),
			HealthCheckConfig: &r53types.HealthCheckConfig{
				Type: r53types.HealthCheckTypeRecoveryControl, RoutingControlArn: aws.String(east),
			},
		},
	)
	require.NoError(t, err)

	hcID := aws.ToString(hc.HealthCheck.Id)

	record := func(id string, fo r53types.ResourceRecordSetFailover, ip string, check *string) r53types.Change {
		return r53types.Change{Action: r53types.ChangeActionCreate, ResourceRecordSet: &r53types.ResourceRecordSet{
			Name: aws.String(
				"app.example.com.",
			),
			Type:            r53types.RRTypeA,
			TTL:             aws.Int64(60),
			SetIdentifier:   aws.String(id),
			Failover:        fo,
			HealthCheckId:   check,
			ResourceRecords: []r53types.ResourceRecord{{Value: aws.String(ip)}},
		}}
	}

	_, err = dns.ChangeResourceRecordSets(
		ctx,
		&route53.ChangeResourceRecordSetsInput{HostedZoneId: aws.String(zoneID), ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				record("primary", r53types.ResourceRecordSetFailoverPrimary, "192.0.2.1", aws.String(hcID)),
				record("secondary", r53types.ResourceRecordSetFailoverSecondary, "192.0.2.2", nil),
			},
		}},
	)
	require.NoError(t, err)

	healthStatus := func() string {
		out, sErr := dns.GetHealthCheckStatus(ctx, &route53.GetHealthCheckStatusInput{HealthCheckId: aws.String(hcID)})
		require.NoError(t, sErr)
		require.NotEmpty(t, out.HealthCheckObservations)

		return aws.ToString(out.HealthCheckObservations[0].StatusReport.Status)
	}

	answer := func() string {
		out, aErr := dns.TestDNSAnswer(ctx, &route53.TestDNSAnswerInput{
			HostedZoneId: aws.String(zoneID), RecordName: aws.String("app.example.com."), RecordType: r53types.RRTypeA,
		})
		require.NoError(t, aErr)
		require.Len(t, out.RecordData, 1)

		return out.RecordData[0]
	}

	// Initial state: both controls Off, the health check is Unhealthy and DNS serves the secondary.
	assert.Equal(t, "Unhealthy", healthStatus())
	assert.Equal(t, "192.0.2.2", answer())

	require.NoError(t, set(east, rctypes.RoutingControlStateOn))
	require.NoError(t, set(west, rctypes.RoutingControlStateOn))
	assert.Equal(t, "Healthy", healthStatus())
	assert.Equal(t, "192.0.2.1", answer(), "primary serves while the routing control is On")

	{ // both off is refused naming the rule
		_, bErr := data.UpdateRoutingControlStates(ctx, &route53recoverycluster.UpdateRoutingControlStatesInput{
			UpdateRoutingControlStateEntries: []rctypes.UpdateRoutingControlStateEntry{
				{RoutingControlArn: aws.String(east), RoutingControlState: rctypes.RoutingControlStateOff},
				{RoutingControlArn: aws.String(west), RoutingControlState: rctypes.RoutingControlStateOff},
			},
		})

		var conflict *rctypes.ConflictException
		require.ErrorAs(t, bErr, &conflict)
		assert.Contains(t, aws.ToString(conflict.Message), "keep-one-on")
		assert.Contains(t, aws.ToString(conflict.Message), aws.ToString(rule.AssertionRule.SafetyRuleArn))
		assert.Equal(t, "Healthy", healthStatus(), "nothing changed")
	}

	{ // flipping one control changes the health check and DNS
		require.NoError(t, set(east, rctypes.RoutingControlStateOff))
		assert.Equal(t, "Unhealthy", healthStatus())
		assert.Equal(t, "192.0.2.2", answer(), "failover record follows the routing control")

		require.Error(t, set(west, rctypes.RoutingControlStateOff), "west is the last control left On")

		require.NoError(t, set(east, rctypes.RoutingControlStateOn))
		assert.Equal(t, "Healthy", healthStatus())
		assert.Equal(t, "192.0.2.1", answer())
	}

	{ // override lets the last control go Off
		require.NoError(t, set(east, rctypes.RoutingControlStateOff))

		_, uErr := data.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
			RoutingControlArn: aws.String(west), RoutingControlState: rctypes.RoutingControlStateOff,
			SafetyRulesToOverride: []string{aws.ToString(rule.AssertionRule.SafetyRuleArn)},
		})
		require.NoError(t, uErr)
	}

	{ // control plane lists the associated health check
		out, lErr := control.ListAssociatedRoute53HealthChecks(
			ctx,
			&route53recoverycontrolconfig.ListAssociatedRoute53HealthChecksInput{
				RoutingControlArn: aws.String(east),
			},
		)
		require.NoError(t, lErr)
		assert.Equal(t, []string{hcID}, out.HealthCheckIds)

		none, lErr := control.ListAssociatedRoute53HealthChecks(
			ctx,
			&route53recoverycontrolconfig.ListAssociatedRoute53HealthChecksInput{
				RoutingControlArn: aws.String(west),
			},
		)
		require.NoError(t, lErr)
		assert.Empty(t, none.HealthCheckIds)
	}

	{ // the routing controls are listed through the data plane
		list, lErr := data.ListRoutingControls(
			ctx,
			&route53recoverycluster.ListRoutingControlsInput{ControlPanelArn: panel.ControlPanel.ControlPanelArn},
		)
		require.NoError(t, lErr)
		require.Len(t, list.RoutingControls, 2)
		assert.Equal(t, "east", aws.ToString(list.RoutingControls[0].RoutingControlName))
	}
}

// Every recovery service must own the requests signed with its own scope; other path matchers must not swallow them.
func TestSignedRecoveryRequestsRouteToOwner(t *testing.T) {
	t.Parallel()

	owners := map[string]string{
		"route53-recovery-control-config": "Route53RecoveryControlConfig",
		"route53-recovery-readiness":      "Route53RecoveryReadiness",
		"route53-recovery-cluster":        "Route53RecoveryCluster",
		"arc-zonal-shift":                 "ARCZonalShift",
	}

	seen := map[string]int{}

	for _, rc := range loadRoutingCorpus(t) {
		owner, ok := owners[rc.authSvc]
		if !ok || rc.host != "localhost:4566" {
			continue
		}

		seen[owner]++

		assert.Equal(t, rc.wantScan, owner, "%s %s signed as %s", rc.method, rc.uri, rc.authSvc)
		assert.Equal(t, rc.wantLookup, owner, "%s %s signed as %s", rc.method, rc.uri, rc.authSvc)
	}

	for _, owner := range owners {
		assert.NotZero(t, seen[owner], "%s has no signed rows in the routing corpus", owner)
	}
}
