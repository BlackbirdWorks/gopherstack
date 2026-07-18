package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestEKS_Subscription_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	// Create subscription
	rec := doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions", map[string]any{
		"name":            "my-sub",
		"licenseType":     "Cluster",
		"licenseQuantity": 5,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	subID, _ := resp["subscription"].(map[string]any)["id"].(string)
	require.NotEmpty(t, subID)

	// List subscriptions
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe subscription
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions/"+subID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent subscription
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Update subscription (POST to the same leaf path, not PUT)
	qty := int32(10)
	rec = doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions/"+subID, map[string]any{
		"licenseType":     "Cluster",
		"licenseQuantity": qty,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update nonexistent
	rec = doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions/nonexistent", map[string]any{
		"licenseType": "Cluster",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete subscription
	rec = doREST(t, h, http.MethodDelete, "/eks-anywhere-subscriptions/"+subID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete nonexistent
	rec = doREST(t, h, http.MethodDelete, "/eks-anywhere-subscriptions/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestEKS_CreateEksAnywhereSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_subscription_success",
			body: map[string]any{
				"name":            "my-sub",
				"licenseType":     "License",
				"licenseQuantity": 5,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_subscription_missing_name",
			body:       map[string]any{"licenseType": "License"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			rec := doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				sub, ok := resp["subscription"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-sub", sub["name"])
				assert.Equal(t, "ACTIVE", sub["status"])
				assert.NotEmpty(t, sub["id"])
				assert.NotEmpty(t, sub["arn"])
			}
		})
	}
}

func TestAnywhereSubscriptionTypeNotStutter(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	sub := &eks.AnywhereSubscription{
		ID:     "sub1",
		Name:   "test",
		Status: "ACTIVE",
	}

	b.AddSubscriptionInternal(sub)
	assert.Equal(t, 1, b.SubscriptionCount())
}
