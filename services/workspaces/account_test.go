package workspaces_test

import (
	"net/http"
	"testing"
)

func TestDescribeAndModifyAccount(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	// Describe account (defaults)
	rec := doTargetRequest(t, h, "DescribeAccount", map[string]any{})
	if rec.Code != http.StatusOK {
		t.Fatalf("describe account: expected 200, got %d", rec.Code)
	}

	var descOut map[string]string
	decodeJSON(t, rec.Body.Bytes(), &descOut)

	if descOut["DedicatedTenancySupport"] != "ENABLED" {
		t.Fatalf("expected ENABLED, got %s", descOut["DedicatedTenancySupport"])
	}

	// Describe account modifications: no ModifyAccount calls yet, so the
	// history must be empty.
	rec2 := doTargetRequest(t, h, "DescribeAccountModifications", map[string]any{})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe modifications: expected 200, got %d", rec2.Code)
	}

	var modsOut struct {
		AccountModifications []map[string]any `json:"AccountModifications"`
	}
	decodeJSON(t, rec2.Body.Bytes(), &modsOut)

	if len(modsOut.AccountModifications) != 0 {
		t.Fatalf("expected no modifications yet, got %d", len(modsOut.AccountModifications))
	}

	// Modify account
	rec3 := doTargetRequest(t, h, "ModifyAccount", map[string]any{
		"DedicatedTenancyManagementCidrRange": "10.0.0.0/16",
		"DedicatedTenancySupport":             "ENABLED",
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("modify account: expected 200, got %d", rec3.Code)
	}

	// Verify change
	rec4 := doTargetRequest(t, h, "DescribeAccount", map[string]any{})
	var descOut2 map[string]string
	decodeJSON(t, rec4.Body.Bytes(), &descOut2)

	if descOut2["DedicatedTenancyManagementCidrRange"] != "10.0.0.0/16" {
		t.Fatalf("expected 10.0.0.0/16, got %s", descOut2["DedicatedTenancyManagementCidrRange"])
	}

	// ModifyAccount must now be recorded in the modification history.
	rec4b := doTargetRequest(t, h, "DescribeAccountModifications", map[string]any{})
	if rec4b.Code != http.StatusOK {
		t.Fatalf("describe modifications after modify: expected 200, got %d", rec4b.Code)
	}

	var modsOut2 struct {
		AccountModifications []map[string]any `json:"AccountModifications"`
	}
	decodeJSON(t, rec4b.Body.Bytes(), &modsOut2)

	if len(modsOut2.AccountModifications) != 1 {
		t.Fatalf("expected 1 modification recorded, got %d", len(modsOut2.AccountModifications))
	}

	mod := modsOut2.AccountModifications[0]
	if mod["ModificationState"] != "COMPLETED" {
		t.Fatalf("expected COMPLETED, got %v", mod["ModificationState"])
	}

	if mod["DedicatedTenancyManagementCidrRange"] != "10.0.0.0/16" {
		t.Fatalf(
			"expected DedicatedTenancyManagementCidrRange=10.0.0.0/16, got %v",
			mod["DedicatedTenancyManagementCidrRange"],
		)
	}

	if _, ok := mod["StartTime"].(float64); !ok {
		t.Fatalf("expected numeric StartTime, got %v (%T)", mod["StartTime"], mod["StartTime"])
	}

	// Modify endpoint encryption
	rec5 := doTargetRequest(t, h, "ModifyEndpointEncryptionMode", map[string]any{
		"DirectoryId":            "d-test",
		"EndpointEncryptionMode": "FIPS_VALIDATED",
	})
	if rec5.Code != http.StatusOK {
		t.Fatalf("modify endpoint encryption: expected 200, got %d", rec5.Code)
	}
}
