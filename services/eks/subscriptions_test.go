package eks_test

import (
	"net/http"
	"strconv"
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
		"term":            map[string]any{"unit": "MONTHS", "duration": 12},
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
				"term":            map[string]any{"unit": "MONTHS", "duration": 36},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_subscription_missing_name",
			body: map[string]any{
				"licenseType": "License",
				"term":        map[string]any{"unit": "MONTHS", "duration": 12},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_subscription_missing_term",
			body: map[string]any{
				"name":            "no-term-sub",
				"licenseType":     "License",
				"licenseQuantity": 5,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_subscription_invalid_term_duration",
			body: map[string]any{
				"name": "bad-term-sub",
				"term": map[string]any{"unit": "MONTHS", "duration": 6},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_subscription_invalid_term_unit",
			body: map[string]any{
				"name": "bad-unit-sub",
				"term": map[string]any{"unit": "DAYS", "duration": 12},
			},
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

// TestEksAnywhereSubscription_TermFields verifies term/autoRenew/
// effectiveDate/expirationDate -- all required or otherwise real fields on
// aws-sdk-go-v2/service/eks's CreateEksAnywhereSubscriptionInput/
// EksAnywhereSubscription that were previously entirely unmodeled -- are
// wired through Create and reflected in the response.
func TestEksAnywhereSubscription_TermFields(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	rec := doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions", map[string]any{
		"name":      "term-sub",
		"autoRenew": true,
		"term":      map[string]any{"unit": "MONTHS", "duration": 36},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	sub := parseResp(t, rec)["subscription"].(map[string]any)
	assert.Equal(t, true, sub["autoRenew"])
	assert.NotEmpty(t, sub["effectiveDate"])
	assert.NotEmpty(t, sub["expirationDate"])

	term, ok := sub["term"].(map[string]any)
	require.True(t, ok, "term must be present in the response")
	assert.Equal(t, "MONTHS", term["unit"])
	assert.InEpsilon(t, float64(36), term["duration"], 0.001)
}

// TestListEksAnywhereSubscriptions_TiedNamePageWalk proves
// ListEksAnywhereSubscriptions sorts on Name alone -- a field
// CreateEksAnywhereSubscription never checks for uniqueness -- over
// b.subscriptions.All() (a store.Table map walk, unstable between calls).
// The handler then paginates that unsorted-by-uniqueness order with
// pkgs/page.New, an offset-index scheme. Several subscriptions sharing one
// Name can therefore land in a different relative order on each call, so a
// page boundary that fell between two tied subscriptions on one call falls
// between two different tied subscriptions on the next -- one gets dropped
// or duplicated across the page boundary with nothing else changed. Looped:
// a single walk can pass by luck since map iteration is randomized per-call.
func TestListEksAnywhereSubscriptions_TiedNamePageWalk(t *testing.T) {
	t.Parallel()

	h, b := newHandlerAndBackend(t)

	const total = 12

	want := make(map[string]bool, total)

	for i := range total {
		id := "sub-tied-" + strconv.Itoa(i)
		b.AddSubscriptionInternal(&eks.AnywhereSubscription{
			ID:     id,
			Name:   "shared-name",
			Status: "ACTIVE",
		})
		want[id] = true
	}

	const pageSize = 5

	for iter := range 30 {
		got := make(map[string]int, total)

		token := ""
		for range total/pageSize + 2 {
			path := "/eks-anywhere-subscriptions?maxResults=" + strconv.Itoa(pageSize)
			if token != "" {
				path += "&nextToken=" + token
			}

			rec := doREST(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)

			subs, ok := resp["subscriptions"].([]any)
			require.True(t, ok, "unexpected subscriptions type %T", resp["subscriptions"])

			for _, s := range subs {
				sub, subOK := s.(map[string]any)
				require.True(t, subOK, "unexpected subscription element type %T", s)
				id, _ := sub["id"].(string)
				got[id]++
			}

			next, _ := resp["nextToken"].(string)
			if next == "" {
				break
			}

			token = next
		}

		require.Lenf(
			t,
			got,
			total,
			"iteration %d: page walk produced %d distinct subscriptions, want %d",
			iter,
			len(got),
			total,
		)

		for id := range want {
			require.Equalf(
				t,
				1,
				got[id],
				"iteration %d: subscription %s appeared %d times across the page walk",
				iter,
				id,
				got[id],
			)
		}
	}
}
