package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpgradeDomain_RealSDKClient proves UpgradeDomainOutput
// (opensearch@v1.75.4 api_op_UpgradeDomain.go:59-79) round-trips its real
// members -- DomainName/TargetVersion/UpgradeId/PerformCheckOnly -- through
// the typed client. UpgradeDomainOutput has no StepStatus member; that name
// belongs to types.UpgradeStepItem, a GetUpgradeHistory/GetUpgradeStatus
// type, per types/types.go.
func TestUpgradeDomain_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("wire-upgrade-domain"),
	})
	require.NoError(t, err)

	out, err := client.UpgradeDomain(ctx, &opensearchsdk.UpgradeDomainInput{
		DomainName:       aws.String("wire-upgrade-domain"),
		TargetVersion:    aws.String("OpenSearch_2.17"),
		PerformCheckOnly: aws.Bool(false),
	})
	require.NoError(t, err)

	assert.Equal(t, "wire-upgrade-domain", aws.ToString(out.DomainName))
	assert.Equal(t, "OpenSearch_2.17", aws.ToString(out.TargetVersion))
	assert.NotEmpty(t, aws.ToString(out.UpgradeId))
	require.NotNil(t, out.PerformCheckOnly, "PerformCheckOnly must round-trip, not be silently dropped")
	assert.False(t, *out.PerformCheckOnly)
}

// TestUpgradeDomain_RawBody_NoInventedStepStatus catches the actual defect a
// typed client cannot see: unknown JSON keys are silently discarded on
// decode, so a fabricated "StepStatus" key never surfaces as a decode error
// or an observable zero value on UpgradeDomainOutput -- only a raw-body
// check catches it.
func TestUpgradeDomain_RawBody_NoInventedStepStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainAndGetARN(t, h, "wire-upgrade-raw-domain")

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/upgradeDomain", map[string]any{
		"DomainName":    "wire-upgrade-raw-domain",
		"TargetVersion": "OpenSearch_2.17",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	_, hasStepStatus := out["StepStatus"]
	assert.False(t, hasStepStatus, "UpgradeDomainOutput has no StepStatus member")
}

// TestVpcEndpoint_RawBody_NoLeakedStatusUntil catches gopherstack-rz6y:
// VpcEndpoint's internal StatusUntil scheduling field (used to run a
// DELETING window) carried a json:"statusUntil,omitzero" tag, so it reached
// the wire on DescribeVpcEndpoints whenever non-zero. Real
// types.VpcEndpoint (opensearch@v1.75.4 types/types.go:3442) has no such
// member. A typed client silently drops unknown keys, so this needs a
// raw-body assertion over a non-empty DescribeVpcEndpoints result.
func TestVpcEndpoint_RawBody_NoLeakedStatusUntil(t *testing.T) {
	t.Parallel()

	b, h := newTestHandlerAndBackend()
	b.SetProcessingDelay(time.Minute)
	domARN := createDomainAndGetARN(t, h, "vpc-statusuntil-domain")

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{"SubnetIds": []string{"subnet-1"}}})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	epID := cOut["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	del := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/vpcEndpoints/"+epID, nil)
	del.Body.Close()

	descResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints/describe",
		map[string]any{"VpcEndpointIds": []string{epID}})
	defer descResp.Body.Close()
	require.Equal(t, http.StatusOK, descResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))
	endpoints, ok := out["VpcEndpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 1, "endpoint must still be present during its DELETING window")

	item := endpoints[0].(map[string]any)
	assert.Equal(t, "DELETING", item["Status"])
	_, hasStatusUntil := item["statusUntil"]
	assert.False(t, hasStatusUntil, "types.VpcEndpoint has no statusUntil member")
}
