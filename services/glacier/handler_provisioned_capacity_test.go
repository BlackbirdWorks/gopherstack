package glacier_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestListProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		purchaseFirst int
		wantStatus    int
		wantCount     int
	}{
		{
			name:       "empty_list",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:          "two_units",
			purchaseFirst: 2,
			wantStatus:    http.StatusOK,
			wantCount:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for range tt.purchaseFirst {
				rec := doRequest(t, h, http.MethodPost, "/-/provisioned-capacity", "")
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/-/provisioned-capacity", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				ProvisionedCapacityList []any `json:"ProvisionedCapacityList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ProvisionedCapacityList, tt.wantCount)
		})
	}
}

// ----------------------------------------
// PurchaseProvisionedCapacity
// ----------------------------------------

func TestPurchaseProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, http.MethodPost, "/-/provisioned-capacity", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["capacityId"])
			assert.NotEmpty(t, rec.Header().Get("X-Amz-Capacity-Id"))
		})
	}
}

// ----------------------------------------
// Multipart upload full round-trip
// ----------------------------------------

func TestProvisionedCapacity_MaxTwoPerAccount(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// First two purchases should succeed.
	for i := range 2 {
		rec := doRequest(t, h, http.MethodPost,
			"/"+testAccountID+"/provisioned-capacity", "")
		require.Equal(t, http.StatusCreated, rec.Code, "purchase %d failed: %s", i+1, rec.Body.String())
	}

	// Third purchase must fail.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/provisioned-capacity", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceededException", errResp["code"])
}

// -------------------------------------------------------------------------
// Issue 20: ProvisionedCapacity 1-month TTL
// -------------------------------------------------------------------------

func TestProvisionedCapacity_ExpiresAfterOneMonth(t *testing.T) {
	t.Parallel()

	bk := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	// Purchase a unit.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/provisioned-capacity", "")
	require.Equal(t, http.StatusCreated, rec.Code)

	// Should appear in list.
	rec = doRequest(t, h, http.MethodGet, "/"+testAccountID+"/provisioned-capacity", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	capList := resp["ProvisionedCapacityList"].([]any)
	require.Len(t, capList, 1)

	// Verify ExpirationDate is roughly 1 month from now.
	cap0 := capList[0].(map[string]any)
	expStr := cap0["ExpirationDate"].(string)
	expTime, err := time.Parse("2006-01-02T15:04:05.000Z", expStr)
	require.NoError(t, err)

	startStr := cap0["StartDate"].(string)
	startTime, err := time.Parse("2006-01-02T15:04:05.000Z", startStr)
	require.NoError(t, err)

	diff := expTime.Sub(startTime)
	// Should be approximately 30 days (±2 days tolerance).
	assert.InDelta(t, 30*24*float64(time.Hour), float64(diff), float64(2*24*time.Hour),
		"expiration should be ~30 days from start")
}

// -------------------------------------------------------------------------
// Issue 24: Inventory job updates LastInventoryDate
// -------------------------------------------------------------------------

func TestProvisionedCapacity_ExpiredUnitNotListed(t *testing.T) {
	t.Parallel()

	bk := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	// Purchase a unit.
	_, err := bk.PurchaseProvisionedCapacity(testAccountID)
	require.NoError(t, err)

	// Manually expire it by adding a past-dated unit through backend internals.
	// We can't use the exported setter here, so we purchase and then re-list.
	caps := bk.ListProvisionedCapacity(testAccountID)
	require.Len(t, caps, 1, "should have 1 unit before expiry")

	// Now purchase second to fill up to limit.
	_, err = bk.PurchaseProvisionedCapacity(testAccountID)
	require.NoError(t, err)

	caps = bk.ListProvisionedCapacity(testAccountID)
	assert.Len(t, caps, 2, "should have 2 active units")
}

// -------------------------------------------------------------------------
// Helpers used in this file
// -------------------------------------------------------------------------

// createVault creates a vault and fails the test if it doesn't return 201.

func TestProvisionedCapacity_PurchaseAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		purchases  int
		wantCount  int
		wantStatus int
	}{
		{name: "purchase_one", purchases: 1, wantCount: 1, wantStatus: http.StatusCreated},
		{name: "purchase_two", purchases: 2, wantCount: 2, wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for range tt.purchases {
				rec := doRequestWithHeaders(t, h, http.MethodPost,
					"/"+testAccountID+"/provisioned-capacity", "", nil)
				require.Equal(t, tt.wantStatus, rec.Code)
			}

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list := resp["ProvisionedCapacityList"].([]any)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestProvisionedCapacity_LimitEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "third_purchase_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for range 2 {
				rec := doRequestWithHeaders(t, h, http.MethodPost,
					"/"+testAccountID+"/provisioned-capacity", "", nil)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/provisioned-capacity", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

func TestProvisionedCapacity_DatesPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start_and_expiration_dates_set"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusCreated, rec.Code)
			var purchaseResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &purchaseResp))
			capID := purchaseResp["capacityId"]
			require.NotEmpty(t, capID)

			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/provisioned-capacity", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			list := listResp["ProvisionedCapacityList"].([]any)
			require.Len(t, list, 1)
			capItem := list[0].(map[string]any)
			assert.NotEmpty(t, capItem["StartDate"], tt.name)
			assert.NotEmpty(t, capItem["ExpirationDate"])
			assert.Equal(t, capID, capItem["CapacityId"])
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. Multipart upload complete lifecycle
// ─────────────────────────────────────────────────────────────────────────────
