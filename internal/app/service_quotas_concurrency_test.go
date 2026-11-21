package app

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
)

func TestQuotaLimitsHoldUnderConcurrentCreatesAndUsageReads(t *testing.T) {
	t.Parallel()

	const (
		limit   = 5
		workers = 24
	)

	tests := []struct {
		create func(t *testing.T, r *resourceQuotaRig, i int) error
		name   string
	}{
		{name: "rds instances", create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
			_, err := r.rds.Backend.CreateDBInstance("c"+strconv.Itoa(i), "postgres", "db.t3.micro", "", "admin",
				"", 20, rdsbackend.DBInstanceOptions{})

			return err
		}},
		{name: "rds clusters", create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
			_, err := r.rds.Backend.CreateDBCluster("c"+strconv.Itoa(i), "aurora-postgresql", "admin", "", "",
				5432, nil, rdsbackend.DBClusterOptions{})

			return err
		}},
		{name: "application load balancers", create: func(_ *testing.T, r *resourceQuotaRig, i int) error {
			_, err := r.elbv2.Backend.CreateLoadBalancer(
				elbv2backend.CreateLoadBalancerInput{Name: "c" + strconv.Itoa(i)})

			return err
		}},
		{name: "classic load balancers", create: func(t *testing.T, r *resourceQuotaRig, i int) error {
			t.Helper()

			_, err := r.elb.CreateLoadBalancer(t.Context(), elbbackend.CreateLoadBalancerInput{
				LoadBalancerName: "c" + strconv.Itoa(i), AvailabilityZones: []string{"us-east-1a"},
				Listeners: []elbbackend.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 80}},
			})

			return err
		}},
		{name: "cloudformation stacks", create: func(t *testing.T, r *resourceQuotaRig, i int) error {
			t.Helper()

			_, err := r.cfn.CreateStack(t.Context(), "c"+strconv.Itoa(i), quotaTestTemplate, nil,
				cfnbackend.StackOptions{})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := newResourceQuotaRig(t, true)

			sq, ok := r.byName["ServiceQuotas"].(*servicequotasbackend.Handler)
			require.True(t, ok)

			fixed := func(a, rg, svc, code string) (float64, bool) {
				_, _ = sq.Backend.QuotaLimit(a, rg, svc, code)

				return limit, true
			}
			r.rds.SetQuotaLimiter(fixed)
			r.elbv2.SetQuotaLimiter(fixed)
			r.elb.SetQuotaLimiter(fixed)

			cfnH, ok := r.byName["CloudFormation"].(*cfnbackend.Handler)
			require.True(t, ok)
			cfnH.SetQuotaLimiter(fixed)

			ctx := t.Context()

			stop := make(chan struct{})

			var readers sync.WaitGroup

			readers.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						sq.Backend.Tick(ctx)

						if rep, err := sq.Backend.StartReport(ctx); err == nil {
							_, _ = sq.Backend.GetReport(ctx, rep.ID, "", nil)
						}
					}
				}
			})

			var (
				created atomic.Int32
				writers sync.WaitGroup
			)

			for i := range workers {
				writers.Go(func() {
					if err := tc.create(t, r, i); err == nil {
						created.Add(1)
					}
				})
			}

			writers.Wait()
			close(stop)
			readers.Wait()

			assert.Equal(t, int32(limit), created.Load())
		})
	}
}
