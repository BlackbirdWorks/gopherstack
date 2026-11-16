package azurefile_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

func TestSDK_ProvisionedRangesFollowAccountMedia(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sku  string
		iops int64
		bad  bool
	}{
		{name: "hdd within range", sku: "StandardV2_LRS", iops: 5000},
		{name: "hdd above range", sku: "StandardV2_LRS", iops: 60000, bad: true},
		{name: "hdd above guardrail", sku: "StandardV2_LRS", iops: 7000, bad: true},
		{name: "ssd below range", sku: "PremiumV2_LRS", iops: 1000, bad: true},
		{name: "ssd within range", sku: "PremiumV2_LRS", iops: 15000},
		{name: "v1 account refuses provisioning", sku: "Standard_LRS", iops: 1000, bad: true},
		{name: "premium v1 refuses provisioning", sku: "Premium_LRS", iops: 4000, bad: true},
		{name: "unknown account keeps union", sku: "", iops: 60000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.h.Backend.AccountSKU = func(string) (string, bool) { return tt.sku, tt.sku != "" }

			_, err := e.svc.NewShareClient("provshare").
				Create(ctx(t), &share.CreateOptions{ShareProvisionedIops: &tt.iops, Quota: new(int32(100))})
			if tt.bad {
				requireCode(t, err, fileerror.InvalidHeaderValue)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_ProvisionedV2Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sku        string
		iops       int64
		mibps      int64
		burstIops  int64
		burstCreds int64
	}{
		{name: "hdd", sku: "StandardV2_LRS", iops: 1020, mibps: 62, burstIops: 5000, burstCreds: (5000 - 1020) * 3600},
		{
			name:       "ssd",
			sku:        "PremiumV2_LRS",
			iops:       3100,
			mibps:      110,
			burstIops:  10000,
			burstCreds: (10000 - 3100) * 3600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.h.Backend.AccountSKU = func(string) (string, bool) { return tt.sku, true }

			sc := e.svc.NewShareClient("v2share")
			_, err := sc.Create(ctx(t), &share.CreateOptions{Quota: new(int32(100))})
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			require.NotNil(t, props.ProvisionedIops)
			require.NotNil(t, props.IncludedBurstIops)
			require.NotNil(t, props.MaxBurstCreditsForIops)
			assert.EqualValues(t, tt.iops, *props.ProvisionedIops)
			assert.EqualValues(t, tt.mibps, *props.ProvisionedBandwidthMiBps)
			assert.Equal(t, tt.burstIops, *props.IncludedBurstIops)
			assert.Equal(t, tt.burstCreds, *props.MaxBurstCreditsForIops)
		})
	}
}

func TestBackend_ManagementViewCarriesBurstFigures(t *testing.T) {
	t.Parallel()

	b := azurefile.NewInMemoryBackend()
	b.AccountSKU = func(string) (string, bool) { return "PremiumV2_LRS", true }

	view, _, err := b.PutShare("acct", "burstshare", azurefile.ShareInput{Quota: new(int64(100))}, true)
	require.NoError(t, err)
	require.NotNil(t, view.IncludedBurst)
	require.NotNil(t, view.BurstCredits)
	assert.EqualValues(t, 10000, *view.IncludedBurst)
	assert.EqualValues(t, (10000-3100)*3600, *view.BurstCredits)

	_, _, err = b.PutShare(
		"acct",
		"toofast",
		azurefile.ShareInput{Quota: new(int64(100)), Iops: new(int64(20000))},
		true,
	)
	require.Error(t, err)
}
