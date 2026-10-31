package azurearm_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apimServiceBody(
	sku armapimanagement.SKUType, capacity int32, email, pub string,
) armapimanagement.ServiceResource {
	return armapimanagement.ServiceResource{
		Location: new("westus"),
		SKU:      &armapimanagement.ServiceSKUProperties{Name: &sku, Capacity: &capacity},
		Properties: &armapimanagement.ServiceProperties{
			PublisherEmail: &email, PublisherName: &pub,
		},
	}
}

func TestAPIMService_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svc      string
		sku      armapimanagement.SKUType
		email    string
		pub      string
		wantCode string
		capacity int32
	}{
		{
			name:     "ok developer",
			svc:      "okdev",
			sku:      armapimanagement.SKUTypeDeveloper,
			capacity: 1,
			email:    "a@b.io",
			pub:      "P",
		},
		{name: "ok consumption", svc: "okcons", sku: armapimanagement.SKUTypeConsumption, email: "a@b.io", pub: "P"},
		{
			name:     "ok premium",
			svc:      "okprem",
			sku:      armapimanagement.SKUTypePremium,
			capacity: 12,
			email:    "a@b.io",
			pub:      "P",
		},
		{
			name: "consumption capacity", svc: "c1", sku: armapimanagement.SKUTypeConsumption, capacity: 1,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "developer capacity", svc: "d2", sku: armapimanagement.SKUTypeDeveloper, capacity: 2,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "basic capacity", svc: "b3", sku: armapimanagement.SKUTypeBasic, capacity: 3,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "standard capacity", svc: "s5", sku: armapimanagement.SKUTypeStandard, capacity: 5,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "premium capacity", svc: "p13", sku: armapimanagement.SKUTypePremium, capacity: 13,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "unknown sku", svc: "iso", sku: armapimanagement.SKUTypeIsolated, capacity: 1,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "bad email", svc: "bm", sku: armapimanagement.SKUTypeDeveloper, capacity: 1,
			email: "nope", pub: "P", wantCode: "ValidationError",
		},
		{
			name: "missing publisher", svc: "mp", sku: armapimanagement.SKUTypeDeveloper, capacity: 1,
			email: "a@b.io", pub: "", wantCode: "ValidationError",
		},
		{
			name: "bad name", svc: "-bad", sku: armapimanagement.SKUTypeDeveloper, capacity: 1,
			email: "a@b.io", pub: "P", wantCode: "ValidationError",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)

			_, err := s.f.NewServiceClient().BeginCreateOrUpdate(t.Context(), apimRG, tc.svc,
				apimServiceBody(tc.sku, tc.capacity, tc.email, tc.pub), nil)

			if tc.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := s.f.NewServiceClient().Get(t.Context(), apimRG, tc.svc, nil)
			require.NoError(t, err)
			assert.Equal(t, "https://"+tc.svc+".azure-api.net", *got.Properties.GatewayURL)
			assert.Equal(t, tc.sku, *got.SKU.Name)
			assert.Equal(t, tc.email, *got.Properties.PublisherEmail)
		})
	}
}

func TestAPIMService_Lifecycle(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	ctx := t.Context()
	svcs := s.f.NewServiceClient()

	poller, err := svcs.BeginCreateOrUpdate(ctx, apimRG, "life1",
		apimServiceBody(armapimanagement.SKUTypeDeveloper, 1, "a@b.io", "P"), nil)
	require.NoError(t, err)

	got, err := svcs.Get(ctx, apimRG, "life1", nil)
	require.NoError(t, err)
	assert.Equal(t, "Activating", *got.Properties.ProvisioningState)
	assert.NotEmpty(t, *got.Etag)

	s.clock.Advance(time.Hour)

	res, err := poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: 5 * time.Millisecond})
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", *res.Properties.ProvisioningState)
	assert.Equal(t, "apimgmt-noreply@mail.windowsazure.com", *res.Properties.NotificationSenderEmail)
	assert.Equal(t, "https://life1.management.azure-api.net", *res.Properties.ManagementAPIURL)

	bySub := 0

	pager := svcs.NewListPager(nil)
	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		bySub += len(page.Value)
	}

	assert.Equal(t, 1, bySub)

	upd, err := svcs.BeginUpdate(ctx, apimRG, "life1", armapimanagement.ServiceUpdateParameters{
		Tags: map[string]*string{"env": new("test")},
		SKU: &armapimanagement.ServiceSKUProperties{
			Name: new(armapimanagement.SKUTypeBasic), Capacity: new(int32(2)),
		},
	}, nil)
	require.NoError(t, err)

	got, err = svcs.Get(ctx, apimRG, "life1", nil)
	require.NoError(t, err)
	assert.Equal(t, "Updating", *got.Properties.ProvisioningState)
	assert.Equal(t, "test", *got.Tags["env"])

	s.clock.Advance(time.Hour)

	_, err = upd.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: 5 * time.Millisecond})
	require.NoError(t, err)

	got, err = svcs.Get(ctx, apimRG, "life1", nil)
	require.NoError(t, err)
	assert.Equal(t, armapimanagement.SKUTypeBasic, *got.SKU.Name)
	assert.Equal(t, "Succeeded", *got.Properties.ProvisioningState)
	assert.Equal(t, "a@b.io", *got.Properties.PublisherEmail)

	del, err := svcs.BeginDelete(ctx, apimRG, "life1", nil)
	require.NoError(t, err)

	s.clock.Advance(time.Hour)

	_, err = del.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: 5 * time.Millisecond})
	require.NoError(t, err)

	_, err = svcs.Get(ctx, apimRG, "life1", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestAPIMService_ConsumptionActivatesQuickly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sku      armapimanagement.SKUType
		advance  time.Duration
		capacity int32
		wantDone bool
	}{
		{sku: armapimanagement.SKUTypeConsumption, advance: 2 * time.Second, wantDone: true},
		{sku: armapimanagement.SKUTypeDeveloper, advance: 2 * time.Second, capacity: 1, wantDone: false},
		{sku: armapimanagement.SKUTypeDeveloper, advance: 10 * time.Second, capacity: 1, wantDone: true},
	}

	for _, tc := range tests {
		t.Run(string(tc.sku)+tc.advance.String(), func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			ctx := t.Context()

			_, err := s.f.NewServiceClient().BeginCreateOrUpdate(ctx, apimRG, "fast",
				apimServiceBody(tc.sku, tc.capacity, "a@b.io", "P"), nil)
			require.NoError(t, err)

			s.clock.Advance(tc.advance)

			got, err := s.f.NewServiceClient().Get(ctx, apimRG, "fast", nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDone, *got.Properties.ProvisioningState == "Succeeded")
		})
	}
}

func TestAPIMService_BusyWhileActivating(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	ctx := t.Context()

	_, err := s.f.NewServiceClient().BeginCreateOrUpdate(ctx, apimRG, apimSvc,
		apimServiceBody(armapimanagement.SKUTypeDeveloper, 1, "a@b.io", "P"), nil)
	require.NoError(t, err)

	_, err = s.f.NewProductClient().CreateOrUpdate(ctx, apimRG, apimSvc, "p1", armapimanagement.ProductContract{
		Properties: &armapimanagement.ProductContractProperties{DisplayName: new("P1")},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	status, _, _ := s.gw(t, http.MethodGet, "/x", "", nil)
	assert.Equal(t, http.StatusServiceUnavailable, status)
}

func TestAPIMService_NameConflictAndAvailability(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	ctx := t.Context()
	svcs := s.f.NewServiceClient()

	s.createService(t, "taken", armapimanagement.SKUTypeConsumption, 0)

	s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/rg2", `{"location":"westus"}`, nil)

	_, err := svcs.BeginCreateOrUpdate(ctx, "rg2", "taken",
		apimServiceBody(armapimanagement.SKUTypeConsumption, 0, "a@b.io", "P"), nil)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	tests := []struct {
		name   string
		reason armapimanagement.NameAvailabilityReason
		avail  bool
	}{
		{name: "taken", reason: armapimanagement.NameAvailabilityReasonAlreadyExists},
		{name: "fresh-name", reason: armapimanagement.NameAvailabilityReasonValid, avail: true},
		{name: "9bad", reason: armapimanagement.NameAvailabilityReasonInvalid},
	}

	for _, tc := range tests {
		got, cerr := svcs.CheckNameAvailability(ctx, armapimanagement.ServiceCheckNameAvailabilityParameters{
			Name: new(tc.name),
		}, nil)
		require.NoError(t, cerr, tc.name)
		assert.Equal(t, tc.avail, *got.NameAvailable, tc.name)
		assert.Equal(t, tc.reason, *got.Reason, tc.name)
	}
}

func TestAPIMService_GetMissing(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)

	_, err := s.f.NewServiceClient().Get(t.Context(), apimRG, "nothere", nil)
	require.Error(t, err)

	re, ok := errors.AsType[*azcore.ResponseError](err)
	require.True(t, ok)
	assert.Equal(t, http.StatusNotFound, re.StatusCode)

	status, _, _ := s.raw(t, http.MethodGet, s.armPath("/apis"), "", nil)
	assert.Equal(t, http.StatusNotFound, status)
}
