package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

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
