package iot_test

import (
	"net/http"
	"testing"
)

// TestNewOps_BillingGroup tests BillingGroup CRUD.
func TestBillingGroup(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateBillingGroup
	out := iotOK(t, h, http.MethodPost, "/billing-groups/my-group", map[string]any{
		"billingGroupProperties": map[string]any{
			"billingGroupDescription": "test group",
		},
	})
	if out["billingGroupName"] != "my-group" {
		t.Errorf("billingGroupName mismatch: %v", out)
	}

	// DescribeBillingGroup
	out2 := iotOK(t, h, http.MethodGet, "/billing-groups/my-group", nil)
	if out2["billingGroupName"] != "my-group" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListBillingGroups
	out3 := iotOK(t, h, http.MethodGet, "/billing-groups", nil)
	groups, _ := out3["billingGroups"].([]any)
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
	}

	// UpdateBillingGroup
	out4 := iotOK(t, h, http.MethodPatch, "/billing-groups/my-group", map[string]any{
		"billingGroupProperties": map[string]any{
			"billingGroupDescription": "updated",
		},
	})
	if out4["version"] == nil {
		t.Error("expected version in update response")
	}

	// DeleteBillingGroup
	iotOK(t, h, http.MethodDelete, "/billing-groups/my-group", nil)

	iotExpectError(t, h, "/billing-groups/my-group")
}
