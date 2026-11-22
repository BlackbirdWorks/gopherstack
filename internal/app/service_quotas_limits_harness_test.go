package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var errNoQuotaTarget = errors.New("no resource to act on")

// limitEnv is what a limit case's create and remove functions get to call the emulated services with.
type limitEnv struct {
	cfg aws.Config
	url string
}

// endpoint points one SDK client's options at the emulator; it works for every service's Options.BaseEndpoint.
func (e limitEnv) endpoint() *string { return aws.String(e.url) }

type limitCase struct {
	handler func(t *testing.T) service.Registerable
	create  func(ctx context.Context, e limitEnv, i int) error
	remove  func(ctx context.Context, e limitEnv, i int) error
	prepare func(ctx context.Context, e limitEnv) error
	name    string
	svc     string
	quota   string
	code    string
	limit   int
	// existing counts resources that exist before the case creates any.
	existing int
}

func (tc limitCase) fixedLimit() func(account, region, svc, code string) (float64, bool) {
	return func(_, _, svc, code string) (float64, bool) {
		return float64(tc.limit), svc == tc.svc && code == tc.quota
	}
}

func newLimitEnv(t *testing.T, tc limitCase, enforce bool) (limitEnv, *quotaUsageAdapter) {
	t.Helper()

	h := tc.handler(t)
	byName := map[string]service.Registerable{h.Name(): h}
	adapter := newQuotaUsageAdapter(byName)

	if enforce {
		enforceServiceQuotas(tc.fixedLimit(), adapter)
	}

	url := sdktest.Serve(t, h)
	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return limitEnv{cfg: cfg, url: url}, adapter
}

func (tc limitCase) setup(t *testing.T, e limitEnv) {
	t.Helper()

	if tc.prepare != nil {
		require.NoError(t, tc.prepare(t.Context(), e))
	}
}

// countCase creates resources through the SDK and expects the quota usage to grow by exactly that many.
type countCase struct {
	handler func(t *testing.T) service.Registerable
	create  func(ctx context.Context, e limitEnv, i int) error
	name    string
	svc     string
	quota   string
	n       int
	step    int
}

func runCountCases(t *testing.T, cases []countCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lc := limitCase{handler: tc.handler, svc: tc.svc, quota: tc.quota}
			e, adapter := newLimitEnv(t, lc, false)
			before := lc.usage(t, adapter)

			for i := range tc.n {
				require.NoError(t, tc.create(t.Context(), e, i))
			}

			step := max(tc.step, 1)
			assert.InDelta(t, before+float64(tc.n*step), lc.usage(t, adapter), 0.001)
		})
	}
}

func (tc limitCase) usage(t *testing.T, a *quotaUsageAdapter) float64 {
	t.Helper()

	got, ok := a.QuotaUsage(quotaTestAccount, "us-east-1", tc.svc, tc.quota)
	require.True(t, ok, "%s/%s has no counter", tc.svc, tc.quota)

	return got
}

func runLimitCases(t *testing.T, cases []limitCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name+" enforced", func(t *testing.T) {
			t.Parallel()

			e, adapter := newLimitEnv(t, tc, true)
			ctx := t.Context()
			tc.setup(t, e)

			for i := range tc.limit - tc.existing {
				require.NoError(t, tc.create(ctx, e, i))
			}

			assert.InDelta(t, float64(tc.limit), tc.usage(t, adapter), 0.001)

			err := tc.create(ctx, e, tc.limit)
			require.Error(t, err)
			assert.Equal(t, tc.code, quotaErrCode(err))

			if tc.remove == nil {
				return
			}

			require.NoError(t, tc.remove(ctx, e, 0))
			require.NoError(t, tc.create(ctx, e, tc.limit))
		})

		t.Run(tc.name+" open", func(t *testing.T) {
			t.Parallel()

			e, adapter := newLimitEnv(t, tc, false)
			ctx := t.Context()
			tc.setup(t, e)

			for i := range tc.limit + 1 - tc.existing {
				require.NoError(t, tc.create(ctx, e, i))
			}

			assert.InDelta(t, float64(tc.limit+1), tc.usage(t, adapter), 0.001)
		})

		t.Run(tc.name+" concurrent", func(t *testing.T) {
			t.Parallel()

			e, _ := newLimitEnv(t, tc, true)
			ctx := t.Context()
			tc.setup(t, e)

			for i := range tc.limit - tc.existing - 1 {
				require.NoError(t, tc.create(ctx, e, i))
			}

			var (
				won atomic.Int32
				wg  sync.WaitGroup
			)

			base := tc.limit

			for i := range 6 {
				wg.Go(func() {
					if tc.create(ctx, e, base+i) == nil {
						won.Add(1)
					}
				})
			}

			wg.Wait()
			assert.Equal(t, int32(1), won.Load())
		})
	}
}
