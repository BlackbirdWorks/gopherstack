package app

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
)

const quotaTestRegion = "us-east-1"

const quotaTestTemplate = `{"Resources":{"B":{"Type":"AWS::SNS::Topic"}}}`

type resourceQuotaRig struct {
	byName map[string]service.Registerable
	rds    *rdsbackend.Handler
	cfn    *cfnbackend.InMemoryBackend
	elb    *elbbackend.InMemoryBackend
	elbv2  *elbv2backend.Handler
	ecs    *ecsbackend.InMemoryBackend
}

func newResourceQuotaRig(t *testing.T, enforce bool) *resourceQuotaRig {
	t.Helper()

	sqBack := servicequotasbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion)
	sqBack.SetLifecycleDelay(time.Millisecond)

	rdsBk := rdsbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion)
	t.Cleanup(rdsBk.Close)

	elbv2Bk := elbv2backend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion)
	t.Cleanup(elbv2Bk.Close)

	cfnBk := cfnbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion, nil)
	elbBk := elbbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion)
	ecsBk := ecsbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion, ecsbackend.NewNoopRunner())

	r := &resourceQuotaRig{
		rds: rdsbackend.NewHandler(rdsBk), cfn: cfnBk, elb: elbBk, ecs: ecsBk,
		elbv2: elbv2backend.NewHandler(elbv2Bk),
	}
	r.byName = map[string]service.Registerable{
		"ServiceQuotas":  servicequotasbackend.NewHandler(sqBack),
		"RDS":            r.rds,
		"CloudFormation": cfnbackend.NewHandler(cfnBk),
		"ELB":            elbbackend.NewHandler(elbBk),
		"ELBv2":          r.elbv2,
		"ECS":            ecsbackend.NewHandler(ecsBk),
	}

	wireServiceQuotas(enforce, r.byName)

	return r
}

func TestQuotaUsageCountsResourceServices(t *testing.T) {
	t.Parallel()

	r := newResourceQuotaRig(t, false)
	ctx := t.Context()

	for i := range 2 {
		id := "u" + strconv.Itoa(i)
		_, err := r.rds.Backend.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20,
			rdsbackend.DBInstanceOptions{})
		require.NoError(t, err)
	}

	_, err := r.rds.Backend.CreateDBCluster("uc", "aurora-postgresql", "admin", "", "", 5432, nil,
		rdsbackend.DBClusterOptions{})
	require.NoError(t, err)

	_, err = r.cfn.CreateStack(ctx, "st", quotaTestTemplate, nil, cfnbackend.StackOptions{})
	require.NoError(t, err)

	_, err = r.ecs.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "ec"})
	require.NoError(t, err)

	_, err = r.elb.CreateLoadBalancer(ctx, elbbackend.CreateLoadBalancerInput{
		LoadBalancerName: "cl", AvailabilityZones: []string{"us-east-1a"},
		Listeners: []elbbackend.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 80}},
	})
	require.NoError(t, err)

	for i, tp := range []string{"application", "network"} {
		_, err = r.elbv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
			Name: "v2-" + tp + strconv.Itoa(i), Type: tp,
		})
		require.NoError(t, err)
	}

	adapter := newQuotaUsageAdapter(r.byName)

	tests := []struct {
		name   string
		region string
		svc    string
		code   string
		want   float64
	}{
		{name: "rds instances", region: quotaTestRegion, svc: "rds", code: "L-7B6409FD", want: 2},
		{name: "rds clusters", region: quotaTestRegion, svc: "rds", code: "L-952B80B8", want: 1},
		{name: "cloudformation stacks", region: quotaTestRegion, svc: "cloudformation", code: "L-0485CB21", want: 1},
		{name: "ecs clusters", region: quotaTestRegion, svc: "ecs", code: "L-21C621EB", want: 1},
		{name: "classic elbs", region: quotaTestRegion, svc: "elasticloadbalancing", code: "L-E9E9831D", want: 1},
		{name: "albs", region: quotaTestRegion, svc: "elasticloadbalancing", code: "L-53DA6B97", want: 1},
		{name: "nlbs", region: quotaTestRegion, svc: "elasticloadbalancing", code: "L-69A177A2", want: 1},
		{name: "other region is empty", region: "eu-west-1", svc: "rds", code: "L-7B6409FD", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := adapter.QuotaUsage(quotaTestAccount, tc.region, tc.svc, tc.code)
			assert.True(t, ok)
			assert.InDelta(t, tc.want, got, 0.001)
		})
	}

	_, ok := (&quotaUsageAdapter{}).QuotaUsage(quotaTestAccount, quotaTestRegion, "rds", "L-7B6409FD")
	assert.False(t, ok)
}

func TestQuotaEnforcementResourceServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		create     func(t *testing.T, r *resourceQuotaRig, i int) error
		name       string
		openCapped bool
		limit      int
	}{
		{
			name: "rds instances", limit: 40, wantErr: rdsbackend.ErrInstanceQuotaExceeded,
			create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
				_, err := r.rds.Backend.CreateDBInstance("e"+strconv.Itoa(i), "postgres", "db.t3.micro", "", "admin",
					"", 20, rdsbackend.DBInstanceOptions{})

				return err
			},
		},
		{
			name: "rds clusters", limit: 40, wantErr: rdsbackend.ErrClusterQuotaExceeded,
			create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
				_, err := r.rds.Backend.CreateDBCluster("e"+strconv.Itoa(i), "aurora-postgresql", "admin", "", "",
					5432, nil, rdsbackend.DBClusterOptions{})

				return err
			},
		},
		{
			name: "classic elbs", limit: 20, openCapped: true, wantErr: elbbackend.ErrTooManyLoadBalancers,
			create: func(t *testing.T, r *resourceQuotaRig, i int) error {
				t.Helper()

				_, err := r.elb.CreateLoadBalancer(t.Context(), elbbackend.CreateLoadBalancerInput{
					LoadBalancerName: "e" + strconv.Itoa(i), AvailabilityZones: []string{"us-east-1a"},
					Listeners: []elbbackend.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 80}},
				})

				return err
			},
		},
		{
			name: "network load balancers", limit: 50, wantErr: elbv2backend.ErrTooManyLoadBalancers,
			create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
				_, err := r.elbv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
					Name: "e" + strconv.Itoa(i), Type: "network",
				})

				return err
			},
		},
		{
			name: "application load balancers", limit: 50, wantErr: elbv2backend.ErrTooManyLoadBalancers,
			create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
				_, err := r.elbv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
					Name: "e" + strconv.Itoa(i),
				})

				return err
			},
		},
		{
			name: "cloudformation stacks", limit: 2000, wantErr: cfnbackend.ErrStackLimitExceeded,
			create: func(t *testing.T, r *resourceQuotaRig, i int) error {
				t.Helper()

				_, err := r.cfn.CreateStack(t.Context(), "e"+strconv.Itoa(i), quotaTestTemplate, nil,
					cfnbackend.StackOptions{})

				return err
			},
		},
	}

	for _, tc := range tests {
		for _, enforce := range []bool{true, false} {
			name := tc.name + map[bool]string{true: " enforced", false: " open"}[enforce]

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				if testing.Short() && tc.limit > 100 && !enforce {
					t.Skip("redundant open run of a large quota")
				}

				r := newResourceQuotaRig(t, enforce)

				for i := range tc.limit {
					require.NoError(t, tc.create(t, r, i), i)
				}

				err := tc.create(t, r, tc.limit)
				if !enforce {
					if tc.openCapped {
						require.ErrorIs(t, err, tc.wantErr, "the built-in cap stays when enforcement is off")

						return
					}

					require.NoError(t, err)

					return
				}

				require.ErrorIs(t, err, tc.wantErr)
			})
		}
	}
}

func TestQuotaUtilizationOfResourceServicesInFullApp(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	_, err := cfnH.Backend.CreateStack(t.Context(), "util", quotaTestTemplate, nil, cfnbackend.StackOptions{})
	require.NoError(t, err)

	rdsH, ok := byName["RDS"].(*rdsbackend.Handler)
	require.True(t, ok)

	_, err = rdsH.Backend.CreateDBInstance("util-db", "postgres", "db.t3.micro", "", "admin", "", 20,
		rdsbackend.DBInstanceOptions{})
	require.NoError(t, err)

	sqH, ok := byName["ServiceQuotas"].(*servicequotasbackend.Handler)
	require.True(t, ok)

	sqH.Backend.SetLifecycleDelay(time.Nanosecond)

	sq := servicequotas.NewFromConfig(fx.cfg)

	start, err := sq.StartQuotaUtilizationReport(t.Context(), &servicequotas.StartQuotaUtilizationReportInput{})
	require.NoError(t, err)

	out, err := sq.GetQuotaUtilizationReport(t.Context(), &servicequotas.GetQuotaUtilizationReportInput{
		ReportId: start.ReportId, MaxResults: aws.Int32(100),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Quotas)

	util := map[string]float64{}
	for _, q := range out.Quotas {
		util[aws.ToString(q.ServiceCode)+"/"+aws.ToString(q.QuotaCode)] = aws.ToFloat64(q.Utilization)
	}

	assert.InDelta(t, 0.05, util["cloudformation/L-0485CB21"], 0.001)
	assert.InDelta(t, 2.5, util["rds/L-7B6409FD"], 0.001)
}
