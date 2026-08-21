package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// gopherstack-r80d batch 10 (required-output-member sweep, second service).
// apprunner is not the "one wrapper key" shape (pinpoint/bedrockagent/
// cleanrooms) or the map[string]any-literal shape (s3tables/codecommit) --
// responses are tagged structs with mostly-flat per-op required members --
// but only Service and its nested source-config structs carry any required
// fields at all (AutoScalingConfiguration/Connection/ObservabilityConfiguration/
// VpcConnector/VpcIngressConnection and their *Summary siblings declare zero
// required fields in apprunner@v1.42.4's types.go, confirmed via a
// type-block AST walk, not a grep window). One counted bug
// (Test_SDKRoundTrip_CustomDomain_VpcDNSTargets) plus two fixed-but-NOT-
// counted findings below, each with its own reason for not counting.

// Test_CodeRepository_SourceCodeVersion_Required is a fixed-but-NOT-counted
// proof: CodeRepository.SourceCodeVersion (types.go:245-263, required
// alongside RepositoryUrl) was never validated by validateSourceConfig
// (services.go), which only checked RepositoryURL -- so a request omitting
// SourceCodeVersion got a 200 back with the required wire key entirely
// absent from codeRepositoryOutput (toCodeRepositoryOutput only sets it
// `if cs.SourceCodeVersionType != ""`). NOT driven through the real
// aws-sdk-go-v2 client: its own generated validateCodeRepository
// (validators.go:792-806) already rejects a nil SourceCodeVersion
// client-side before any request is sent, so a real Go SDK client can never
// reach gopherstack in this state -- the campaign's real-SDK-client
// round-trip proof standard cannot apply here even though the bug is real
// for any other caller (raw HTTP, a non-validating SDK). Proven instead via
// a raw request through this package's own doRequest/newTestHandler test
// helpers (handler_test.go), which bypass the Go SDK's client-side
// validation the way a hand-crafted or different-language request would.
func Test_CodeRepository_SourceCodeVersion_Required(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "r80d-no-scv",
		"SourceConfiguration": map[string]any{
			"CodeRepository": map[string]any{
				"RepositoryUrl": "https://github.com/example/repo",
			},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"CodeRepository without SourceCodeVersion must be rejected, not silently accepted")

	rec = doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "r80d-with-scv",
		"SourceConfiguration": map[string]any{
			"CodeRepository": map[string]any{
				"RepositoryUrl": "https://github.com/example/repo",
				"SourceCodeVersion": map[string]any{
					"Type":  "BRANCH",
					"Value": "main",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut struct {
		Service struct {
			ServiceArn string `json:"ServiceArn"`
		} `json:"Service"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

	rec = doRequest(t, h, "DescribeService", map[string]any{
		"ServiceArn": createOut.Service.ServiceArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		Service struct {
			SourceConfiguration struct {
				CodeRepository struct {
					SourceCodeVersion *struct {
						Value string `json:"Value"`
					} `json:"SourceCodeVersion"`
				} `json:"CodeRepository"`
			} `json:"SourceConfiguration"`
		} `json:"Service"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.Service.SourceConfiguration.CodeRepository.SourceCodeVersion)
	assert.Equal(t, "main", descOut.Service.SourceConfiguration.CodeRepository.SourceCodeVersion.Value)
}

// Test_SDKRoundTrip_CustomDomain_VpcDNSTargets proves AssociateCustomDomain/
// DisassociateCustomDomain's required vpcDNSTargets
// (api_op_AssociateCustomDomain.go / api_op_DisassociateCustomDomain.go,
// both required) decode as a present (if empty) slice, not a dropped
// field -- before the fix, associateCustomDomainOutput/
// disassociateCustomDomainOutput had no VpcDNSTargets field at all, even
// though DescribeCustomDomains (the sibling op with the identical required
// set) already emitted it correctly.
func Test_SDKRoundTrip_CustomDomain_VpcDNSTargets(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
	h := apprunner.NewHandler(backend)
	client := newTestAppRunnerClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("r80d-custom-domain-svc"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	assocOut, err := client.AssociateCustomDomain(ctx, &apprunnersdk.AssociateCustomDomainInput{
		ServiceArn: createOut.Service.ServiceArn,
		DomainName: aws.String("r80d.example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.VpcDNSTargets, "vpcDNSTargets must decode as [], not a dropped required field")
	assert.Empty(t, assocOut.VpcDNSTargets)

	disassocOut, err := client.DisassociateCustomDomain(ctx, &apprunnersdk.DisassociateCustomDomainInput{
		ServiceArn: createOut.Service.ServiceArn,
		DomainName: aws.String("r80d.example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, disassocOut.VpcDNSTargets, "vpcDNSTargets must decode as [], not a dropped required field")
	assert.Empty(t, disassocOut.VpcDNSTargets)
}

// Test_SDKRoundTrip_ObservabilityConfiguration_TraceConfiguration is a
// fixed-but-NOT-counted proof: ObservabilityConfiguration.TraceConfiguration
// is optional on the real type (types.go:601, "If not specified, tracing
// isn't enabled" -- not a required-member bug this campaign's cut targets),
// but this backend captured TracingVendor on Create and never echoed it
// back at all. Fixed alongside the required-field bugs above since it was
// found while auditing the same struct family, using only the
// already-stored value (no fabrication).
func Test_SDKRoundTrip_ObservabilityConfiguration_TraceConfiguration(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
	h := apprunner.NewHandler(backend)
	client := newTestAppRunnerClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateObservabilityConfiguration(ctx, &apprunnersdk.CreateObservabilityConfigurationInput{
		ObservabilityConfigurationName: aws.String("r80d-obs-trace"),
		TraceConfiguration: &types.TraceConfiguration{
			Vendor: types.TracingVendorAwsxray,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ObservabilityConfiguration.TraceConfiguration)
	assert.Equal(t, types.TracingVendorAwsxray, createOut.ObservabilityConfiguration.TraceConfiguration.Vendor)

	descOut, err := client.DescribeObservabilityConfiguration(
		ctx,
		&apprunnersdk.DescribeObservabilityConfigurationInput{
			ObservabilityConfigurationArn: createOut.ObservabilityConfiguration.ObservabilityConfigurationArn,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, descOut.ObservabilityConfiguration.TraceConfiguration)
	assert.Equal(t, types.TracingVendorAwsxray, descOut.ObservabilityConfiguration.TraceConfiguration.Vendor)
}
