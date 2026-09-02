package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchsdktypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
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

// TestDescribeDomainNodes_Storage_RealClient covers a "state tracked, never surfaced"
// bug (gopherstack-6flj/21my): real DomainNodesStatus (opensearch@v1.75.4
// types/types.go) declares StorageSize/StorageType alongside StorageVolumeType, but
// GetDomainNodes (domain_status.go) only ever populated StorageVolumeType from the
// domain's tracked EBSOptions -- StorageSize (EBSOptions.VolumeSize, already read by
// this same function's sibling EBS handling elsewhere in the service) and StorageType
// were never emitted at all, regardless of what volume size the domain was configured
// with.
func TestDescribeDomainNodes_Storage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("nodes-storage-domain"),
		EBSOptions: &opensearchsdktypes.EBSOptions{
			EBSEnabled: aws.Bool(true),
			VolumeType: opensearchsdktypes.VolumeTypeGp3,
			VolumeSize: aws.Int32(42),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDomainNodes(ctx, &opensearchsdk.DescribeDomainNodesInput{
		DomainName: aws.String("nodes-storage-domain"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.DomainNodesStatusList)

	for _, node := range out.DomainNodesStatusList {
		assert.Equal(t, "42", aws.ToString(node.StorageSize),
			"StorageSize must round-trip from the domain's EBSOptions.VolumeSize, not decode empty")
		assert.NotEmpty(t, aws.ToString(node.StorageType),
			"StorageType must round-trip, not decode empty")
	}
}

// TestDescribeDryRunProgress_DryRunResults_RealClient covers a missing top-level member
// (gopherstack-6flj/21my): real DescribeDryRunProgressOutput (opensearch@v1.75.4
// api_op_DescribeDryRunProgress.go) declares three top-level members --
// DryRunConfig/DryRunProgressStatus/DryRunResults -- but the handler
// (handler_domain_status.go) only ever emitted DryRunProgressStatus. DryRunResults
// (types.DryRunResults: DeploymentType/Message) is the same shape UpdateDomainConfig's own
// DryRun path already synthesizes (handler_domain_config.go's dryRunResultsJSON), so a
// real client's DescribeDryRunProgress().DryRunResults was always nil even after the
// identically-shaped value was already computable elsewhere in this service.
func TestDescribeDryRunProgress_DryRunResults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	createTestDomain(t, h, "dryrun-results-domain")

	out, err := client.DescribeDryRunProgress(ctx, &opensearchsdk.DescribeDryRunProgressInput{
		DomainName: aws.String("dryrun-results-domain"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.DryRunResults, "DryRunResults must round-trip, not decode nil")
	assert.NotEmpty(t, aws.ToString(out.DryRunResults.DeploymentType))
}
