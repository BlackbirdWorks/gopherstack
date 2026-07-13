package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_RegisterDeregisterTargetsSuccessfulField verifies that
// RegisterTargets/DeregisterTargets responses include the "successful" list
// of targets, matching the real API's RegisterTargetsOutput/
// DeregisterTargetsOutput shape (Successful []Target, Unsuccessful
// []TargetFailure). The emulator previously omitted "successful" entirely,
// so SDK clients reading resp.Successful always saw an empty slice even on a
// fully successful call.
func TestParity_RegisterDeregisterTargetsSuccessfulField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-successful-field",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// register two targets, one of which will later fail to deregister
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.2", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	successful, _ := resp["successful"].([]any)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, successful, 2, "RegisterTargets must report both targets as successful")
	assert.Empty(t, unsuccessful)

	first, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", first["id"])
	assert.InEpsilon(t, float64(80), first["port"], 0)

	// register a duplicate -> should fail, and NOT appear in successful
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.3", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1)
	require.Len(t, unsuccessful, 1)
	successOne, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.3", successOne["id"])

	// deregister: one present, one absent
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.99", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1, "DeregisterTargets must report the removed target as successful")
	require.Len(t, unsuccessful, 1)
	successOne, _ = successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", successOne["id"])
}

// TestParity_TargetFailureUsesFailureCodeFailureMessageKeys verifies that
// target failure entries (from RegisterTargets/DeregisterTargets) use the
// wire keys "failureCode"/"failureMessage", matching the real
// TargetFailure shape. The emulator previously emitted "code"/"message",
// which real SDK clients (expecting FailureCode/FailureMessage) would never
// populate.
func TestParity_TargetFailureUsesFailureCodeFailureMessageKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-failure-keys",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// deregister a target that was never registered -> guaranteed failure
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{map[string]any{"id": "10.0.0.1", "port": 80}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, unsuccessful, 1)

	failure, _ := unsuccessful[0].(map[string]any)
	assert.NotEmpty(t, failure["failureCode"], "TargetFailure must use failureCode, not code")
	assert.NotEmpty(t, failure["failureMessage"], "TargetFailure must use failureMessage, not message")
	assert.Nil(t, failure["code"])
	assert.Nil(t, failure["message"])
}

// TestParity_BatchUpdateRuleFailureUsesFailureCodeFailureMessageKeys mirrors
// TestParity_TargetFailureUsesFailureCodeFailureMessageKeys for
// RuleUpdateFailure, whose real wire keys are also
// "failureCode"/"failureMessage" rather than "code"/"message".
func TestParity_BatchUpdateRuleFailureUsesFailureCodeFailureMessageKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-batch-fail-keys"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	recL := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "l1",
		"protocol": "HTTP",
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, recL.Code)
	listenerID, _ := parseBody(t, recL)["id"].(string)

	rec := doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules",
		map[string]any{
			"rules": []any{
				map[string]any{"ruleIdentifier": "rule-notexist", "priority": 99},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, unsuccessful, 1)

	failure, _ := unsuccessful[0].(map[string]any)
	assert.NotEmpty(t, failure["failureCode"], "RuleUpdateFailure must use failureCode, not code")
	assert.NotEmpty(t, failure["failureMessage"], "RuleUpdateFailure must use failureMessage, not message")
	assert.Nil(t, failure["code"])
	assert.Nil(t, failure["message"])
}

// TestParity_SNSAIncludesCustomDomainNameAndDNSEntry verifies that
// ServiceNetworkServiceAssociation responses include "customDomainName" and
// "dnsEntry" when the underlying service has them set, matching the real
// CreateServiceNetworkServiceAssociationOutput/
// GetServiceNetworkServiceAssociationOutput shapes. The emulator previously
// captured these fields internally but never serialized them.
func TestParity_SNSAIncludesCustomDomainNameAndDNSEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{
		"name":             "svc-snsa-dns",
		"customDomainName": "example.com",
	})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svc := parseBody(t, recSvc)
	svcID, _ := svc["id"].(string)

	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snsa-dns"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assert.Equal(t, "example.com", assoc["customDomainName"])
	dnsEntry, ok := assoc["dnsEntry"].(map[string]any)
	require.True(t, ok, "dnsEntry must be present on CreateServiceNetworkServiceAssociation response")
	assert.NotEmpty(t, dnsEntry["domainName"])

	assocID, _ := assoc["id"].(string)
	getRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	got := parseBody(t, getRec)
	assert.Equal(t, "example.com", got["customDomainName"])

	listRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)
	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "example.com", summary["customDomainName"])
	assert.NotEmpty(t, summary["dnsEntry"])
}

// TestParity_TargetGroupSummaryWireShape verifies ListTargetGroups summary
// entries use "vpcIdentifier" (not "vpcId") and include lastUpdatedAt,
// matching the real TargetGroupSummary shape. The emulator previously
// emitted "vpcId", which real SDK clients (populating VpcIdentifier) would
// never see, and omitted lastUpdatedAt entirely.
func TestParity_TargetGroupSummaryWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-summary-shape",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-summary",
			"ipAddressType":               "IPV4",
			"lambdaEventStructureVersion": "V1",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/targetgroups", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)

	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "vpc-summary", summary["vpcIdentifier"], "summary must use vpcIdentifier wire key")
	assert.Nil(t, summary["vpcId"], "summary must not use the vpcId wire key")
	assert.NotEmpty(t, summary["lastUpdatedAt"])
	assert.Equal(t, "IPV4", summary["ipAddressType"])
	assert.Equal(t, "V1", summary["lambdaEventStructureVersion"])
}

// TestParity_TargetGroupConfigRoundTripsIPAddressType verifies that
// ipAddressType/lambdaEventStructureVersion set on CreateTargetGroup are
// echoed back in GetTargetGroup's config, matching real AWS's
// GetTargetGroupOutput.Config shape. The emulator captured these fields but
// never serialized them back to clients.
func TestParity_TargetGroupConfigRoundTripsIPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-config-roundtrip",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-rt",
			"ipAddressType":               "IPV6",
			"lambdaEventStructureVersion": "V2",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	config, ok := parseBody(t, getRec)["config"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "IPV6", config["ipAddressType"])
	assert.Equal(t, "V2", config["lambdaEventStructureVersion"])
}

// TestParity_RuleSummaryIncludesTimestamps verifies that ListRules summary
// entries include createdAt/lastUpdatedAt, matching the real RuleSummary
// shape. The emulator previously omitted both fields.
func TestParity_RuleSummaryIncludesTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-rule-summary-ts"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	recL := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "l1",
		"protocol": "HTTP",
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, recL.Code)
	listenerID, _ := parseBody(t, recL)["id"].(string)

	listRec := doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1, "expected the auto-created default rule")

	summary, _ := items[0].(map[string]any)
	assert.NotEmpty(t, summary["createdAt"])
	assert.NotEmpty(t, summary["lastUpdatedAt"])
}
