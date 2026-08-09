package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAPIGatewayAuditClient returns an API Gateway (v1) client pointed at the
// shared test container. Named uniquely to avoid colliding with any helper that
// may later be added to main_test.go.
func createAPIGatewayAuditClient(t *testing.T) *apigwsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_APIGatewayAudit_ImportApiKeysThenGetApiKeys verifies that
// ImportApiKeys actually creates API keys from the supplied CSV payload and that
// GetApiKeys subsequently returns the imported key.
func TestIntegration_APIGatewayAudit_ImportApiKeysThenGetApiKeys(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	const keyName = "audit-imported-key"

	// AWS API key CSV file format: a header row naming columns, then one row per
	// key. The "key" column holds the secret value; "name" the key name.
	csvPayload := []byte("name,key,enabled\n" + keyName + ",auditsecretvalue123,true\n")

	importOut, err := client.ImportApiKeys(ctx, &apigwsdk.ImportApiKeysInput{
		Body:   csvPayload,
		Format: apigwtypes.ApiKeysFormatCsv,
	})
	require.NoError(t, err, "ImportApiKeys should succeed")
	require.NotEmpty(t, importOut.Ids, "ImportApiKeys should return the id of the created key")

	importedID := importOut.Ids[0]

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApiKey(cleanupCtx, &apigwsdk.DeleteApiKeyInput{ApiKey: aws.String(importedID)})
	})

	// GetApiKeys must reflect the imported key.
	getOut, err := client.GetApiKeys(ctx, &apigwsdk.GetApiKeysInput{})
	require.NoError(t, err, "GetApiKeys should succeed")

	var found bool
	for _, k := range getOut.Items {
		if aws.ToString(k.Id) == importedID {
			found = true

			assert.Equal(t, keyName, aws.ToString(k.Name), "imported key should keep its name")

			break
		}
	}

	assert.True(t, found, "GetApiKeys should contain the imported key %q", importedID)
}

// TestIntegration_APIGatewayAudit_GetSdkValidatesInput verifies that GetSdk
// performs AWS-accurate input validation: a non-existent REST API yields a
// NotFoundException rather than a silent empty success.
func TestIntegration_APIGatewayAudit_GetSdkValidatesInput(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	_, err := client.GetSdk(ctx, &apigwsdk.GetSdkInput{
		RestApiId: aws.String("does-not-exist"),
		StageName: aws.String("prod"),
		SdkType:   aws.String("javascript"),
	})
	require.Error(t, err, "GetSdk against a missing REST API should fail validation")
}

// TestIntegration_APIGatewayAudit_UpdateDomainNameDocumentedFields drives
// UpdateDomainName through the real aws-sdk-go-v2 client with
// apigwtypes.PatchOperation entries for every DomainName field
// patch-operations.html documents as patchable that gopherstack-npq5 found
// missing from the model entirely (certificateName, regionalCertificateName,
// ownershipVerificationCertificateArn, managementPolicy, policy, routingMode,
// endpointAccessMode). Before the fix these had no matching struct field, so
// encoding/json silently dropped every one of them and the PATCH did nothing.
func TestIntegration_APIGatewayAudit_UpdateDomainNameDocumentedFields(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	const domain = "audit-npq5.example.com"

	_, err := client.CreateDomainName(ctx, &apigwsdk.CreateDomainNameInput{DomainName: aws.String(domain)})
	require.NoError(t, err, "CreateDomainName should succeed")

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDomainName(cleanupCtx, &apigwsdk.DeleteDomainNameInput{DomainName: aws.String(domain)})
	})

	replaceOnly := map[string]string{
		"managementPolicy":   `{"Version":"2012-10-17","Statement":[]}`,
		"policy":             `{"Version":"2012-10-17","Statement":[]}`,
		"routingMode":        "API_MAPPING_ONLY",
		"endpointAccessMode": "PRIVATE",
	}

	patches := make([]apigwtypes.PatchOperation, 0, len(replaceOnly))
	for path, value := range replaceOnly {
		patches = append(patches, apigwtypes.PatchOperation{
			Op:    apigwtypes.OpReplace,
			Path:  aws.String("/" + path),
			Value: aws.String(value),
		})
	}

	out, err := client.UpdateDomainName(ctx, &apigwsdk.UpdateDomainNameInput{
		DomainName:      aws.String(domain),
		PatchOperations: patches,
	})
	require.NoError(t, err, "UpdateDomainName should succeed")

	assert.Equal(t, replaceOnly["managementPolicy"], aws.ToString(out.ManagementPolicy))
	assert.Equal(t, replaceOnly["policy"], aws.ToString(out.Policy))
	assert.Equal(t, replaceOnly["routingMode"], string(out.RoutingMode))
	assert.Equal(t, replaceOnly["endpointAccessMode"], string(out.EndpointAccessMode))

	// certificateName/regionalCertificateName/ownershipVerificationCertificateArn
	// document add/replace/remove support; exercise add then remove for each.
	removable := []string{"certificateName", "regionalCertificateName", "ownershipVerificationCertificateArn"}

	addPatches := make([]apigwtypes.PatchOperation, 0, len(removable))
	for _, path := range removable {
		addPatches = append(addPatches, apigwtypes.PatchOperation{
			Op:    apigwtypes.OpAdd,
			Path:  aws.String("/" + path),
			Value: aws.String("cert-value-" + path),
		})
	}

	out, err = client.UpdateDomainName(ctx, &apigwsdk.UpdateDomainNameInput{
		DomainName:      aws.String(domain),
		PatchOperations: addPatches,
	})
	require.NoError(t, err, "UpdateDomainName add should succeed")

	assert.Equal(t, "cert-value-certificateName", aws.ToString(out.CertificateName))
	assert.Equal(t, "cert-value-regionalCertificateName", aws.ToString(out.RegionalCertificateName))
	assert.Equal(t, "cert-value-ownershipVerificationCertificateArn",
		aws.ToString(out.OwnershipVerificationCertificateArn))

	removePatches := make([]apigwtypes.PatchOperation, 0, len(removable))
	for _, path := range removable {
		removePatches = append(removePatches, apigwtypes.PatchOperation{
			Op:   apigwtypes.OpRemove,
			Path: aws.String("/" + path),
		})
	}

	out, err = client.UpdateDomainName(ctx, &apigwsdk.UpdateDomainNameInput{
		DomainName:      aws.String(domain),
		PatchOperations: removePatches,
	})
	require.NoError(t, err, "UpdateDomainName remove should succeed")

	assert.Empty(t, aws.ToString(out.CertificateName), "explicit remove must clear certificateName")
	assert.Empty(t, aws.ToString(out.RegionalCertificateName), "explicit remove must clear regionalCertificateName")
	assert.Empty(t, aws.ToString(out.OwnershipVerificationCertificateArn),
		"explicit remove must clear ownershipVerificationCertificateArn")
}

// TestIntegration_APIGatewayAudit_UpdateUsagePlanProductCode drives
// UpdateUsagePlan through the real aws-sdk-go-v2 client with a
// "/productCode" apigwtypes.PatchOperation, which patch-operations.html
// documents as supporting add/replace/remove (gopherstack-npq5). UsagePlan
// had no ProductCode field at all before the fix, so the PATCH was accepted
// and silently did nothing.
func TestIntegration_APIGatewayAudit_UpdateUsagePlanProductCode(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	createOut, err := client.CreateUsagePlan(ctx, &apigwsdk.CreateUsagePlanInput{
		Name: aws.String("audit-npq5-plan"),
	})
	require.NoError(t, err, "CreateUsagePlan should succeed")

	planID := aws.ToString(createOut.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteUsagePlan(cleanupCtx, &apigwsdk.DeleteUsagePlanInput{UsagePlanId: aws.String(planID)})
	})

	addOut, err := client.UpdateUsagePlan(ctx, &apigwsdk.UpdateUsagePlanInput{
		UsagePlanId: aws.String(planID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpAdd, Path: aws.String("/productCode"), Value: aws.String("prod-abc123")},
		},
	})
	require.NoError(t, err, "UpdateUsagePlan add should succeed")
	assert.Equal(t, "prod-abc123", aws.ToString(addOut.ProductCode))

	removeOut, err := client.UpdateUsagePlan(ctx, &apigwsdk.UpdateUsagePlanInput{
		UsagePlanId: aws.String(planID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpRemove, Path: aws.String("/productCode")},
		},
	})
	require.NoError(t, err, "UpdateUsagePlan remove should succeed")
	assert.Empty(t, aws.ToString(removeOut.ProductCode), "explicit remove must clear productCode")
}
