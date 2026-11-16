package iam_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestQuotaLimiterCustomerManagedPoliciesAndProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(b *iam.InMemoryBackend, i int) error
		count  func(b *iam.InMemoryBackend) int
		name   string
		code   string
	}{
		{
			name: "policies", code: "L-0DA4ABF3",
			create: func(b *iam.InMemoryBackend, i int) error {
				_, err := b.CreatePolicy("p"+strconv.Itoa(i), "/", allowS3GetDoc)

				return err
			},
			count: (*iam.InMemoryBackend).PolicyCount,
		},
		{
			name: "instance profiles", code: "L-E95E4862",
			create: func(b *iam.InMemoryBackend, i int) error {
				_, err := b.CreateInstanceProfile("ip"+strconv.Itoa(i), "/")

				return err
			},
			count: (*iam.InMemoryBackend).InstanceProfileCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			b.SetQuotaLimiter(func(_, _, svc, code string) (float64, bool) {
				return 3, svc == "iam" && code == tt.code
			})

			for i := range 3 {
				require.NoError(t, tt.create(b, i))
			}

			require.ErrorIs(t, tt.create(b, 3), iam.ErrLimitExceeded)
			assert.Equal(t, 3, tt.count(b))

			b.SetQuotaLimiter(nil)
			require.NoError(t, tt.create(b, 4))
		})
	}
}

func TestQuotaLimiterConcurrentCreatesStayWithinLimit(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	b.SetQuotaLimiter(func(_, _, _, code string) (float64, bool) { return 5, code == "L-E95E4862" })

	var wg sync.WaitGroup

	for i := range 40 {
		wg.Go(func() { _, _ = b.CreateInstanceProfile("ip"+strconv.Itoa(i), "/") })
	}

	wg.Wait()
	assert.Equal(t, 5, b.InstanceProfileCount())
}
