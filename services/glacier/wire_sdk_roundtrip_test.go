package glacier_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// newWireTestClient stands up a fresh in-memory glacier backend (with the async
// retrieval window disabled so initiated jobs complete synchronously) and a real
// aws-sdk-go-v2 client against an httptest server running its Handler. Round-tripping
// through the genuine SDK serializer/deserializer proves wire-compatibility that a
// direct handler call or raw-body assertion would miss.
func newWireTestClient(t *testing.T) *glaciersdk.Client {
	t.Helper()

	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return glaciersdk.NewFromConfig(cfg, func(o *glaciersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetVaultAccessPolicy_SDKRoundTrip proves GetVaultAccessPolicy's response body is
// FLAT at the response root, matching the real op's live HandleDeserialize
// (aws-sdk-go-v2/service/glacier@v1.35.4's awsRestjson1_deserializeOpGetVaultAccessPolicy,
// which calls awsRestjson1_deserializeDocumentVaultAccessPolicy(&output.Policy, shape)
// directly on the whole decoded body). A "policy"-wrapping
// awsRestjson1_deserializeOpDocumentGetVaultAccessPolicyOutput helper also exists in
// that file but is DEAD CODE -- never called from HandleDeserialize -- so wrapping the
// response under it (tried and reverted while auditing this op) would have been a
// regression, not a fix. gopherstack's existing flat shape is correct; this is a
// regression guard, not evidence of a prior bug.
func TestGetVaultAccessPolicy_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newWireTestClient(t)

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-access-policy-vault"),
	})
	require.NoError(t, err)

	const policyDoc = `{"Version":"2012-10-17","Statement":[]}`
	_, err = client.SetVaultAccessPolicy(t.Context(), &glaciersdk.SetVaultAccessPolicyInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-access-policy-vault"),
		Policy: &glaciertypes.VaultAccessPolicy{Policy: aws.String(policyDoc)},
	})
	require.NoError(t, err)

	out, err := client.GetVaultAccessPolicy(t.Context(), &glaciersdk.GetVaultAccessPolicyInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-access-policy-vault"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Policy, "typed SDK client must decode a non-nil Policy")
	assert.JSONEq(t, policyDoc, aws.ToString(out.Policy.Policy))
}

// TestGetVaultNotifications_SDKRoundTrip proves GetVaultNotifications's response body
// is FLAT at the response root, same dead-code trap as GetVaultAccessPolicy above:
// awsRestjson1_deserializeOpGetVaultNotifications's live HandleDeserialize calls
// awsRestjson1_deserializeDocumentVaultNotificationConfig(&output.VaultNotificationConfig,
// shape) directly on the whole decoded body; the "vaultNotificationConfig"-wrapping
// OpDocument helper is dead code. gopherstack's existing flat shape is correct; this is
// a regression guard, not evidence of a prior bug.
func TestGetVaultNotifications_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newWireTestClient(t)

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-notif-vault"),
	})
	require.NoError(t, err)

	_, err = client.SetVaultNotifications(t.Context(), &glaciersdk.SetVaultNotificationsInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-notif-vault"),
		VaultNotificationConfig: &glaciertypes.VaultNotificationConfig{
			SNSTopic: aws.String("arn:aws:sns:us-east-1:000000000000:wire-topic"),
			Events:   []string{"ArchiveRetrievalCompleted"},
		},
	})
	require.NoError(t, err)

	out, err := client.GetVaultNotifications(t.Context(), &glaciersdk.GetVaultNotificationsInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-notif-vault"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.VaultNotificationConfig, "typed SDK client must decode a non-nil VaultNotificationConfig")
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:wire-topic", aws.ToString(out.VaultNotificationConfig.SNSTopic))
	assert.Equal(t, []string{"ArchiveRetrievalCompleted"}, out.VaultNotificationConfig.Events)
}

// TestDescribeJob_SelectCsvSerialization_SDKRoundTrip proves SelectParameters'
// InputSerialization/OutputSerialization.Csv wire key is lowercase "csv", matching
// awsRestjson1_deserializeDocumentInputSerialization/OutputSerialization (`case
// "csv":`) -- an anomaly among glacier's otherwise-PascalCase field names. Before the
// fix gopherstack marshaled the response with "Csv" (capital), so a real SDK client's
// typed out.SelectParameters.InputSerialization.Csv / OutputSerialization.Csv were
// always nil on DescribeJob even though gopherstack had real values to report.
func TestDescribeJob_SelectCsvSerialization_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newWireTestClient(t)

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-select-vault"),
	})
	require.NoError(t, err)

	up, err := client.UploadArchive(t.Context(), &glaciersdk.UploadArchiveInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-select-vault"),
		Body: bytes.NewReader([]byte("1,alice,30\n2,bob,25\n")),
	})
	require.NoError(t, err)

	init, err := client.InitiateJob(t.Context(), &glaciersdk.InitiateJobInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-select-vault"),
		JobParameters: &glaciertypes.JobParameters{
			Type:      aws.String("select"),
			ArchiveId: up.ArchiveId,
			SelectParameters: &glaciertypes.SelectParameters{
				Expression:     aws.String("SELECT * FROM archive"),
				ExpressionType: glaciertypes.ExpressionTypeSql,
				InputSerialization: &glaciertypes.InputSerialization{
					Csv: &glaciertypes.CSVInput{FileHeaderInfo: glaciertypes.FileHeaderInfoNone},
				},
				OutputSerialization: &glaciertypes.OutputSerialization{
					Csv: &glaciertypes.CSVOutput{},
				},
			},
			OutputLocation: &glaciertypes.OutputLocation{
				S3: &glaciertypes.S3Location{BucketName: aws.String("wire-results"), Prefix: aws.String("out/")},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeJob(t.Context(), &glaciersdk.DescribeJobInput{
		AccountId: aws.String("-"), VaultName: aws.String("wire-select-vault"), JobId: init.JobId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.SelectParameters)
	require.NotNil(t, out.SelectParameters.InputSerialization,
		"typed SDK client must decode a non-nil InputSerialization")
	require.NotNil(t, out.SelectParameters.InputSerialization.Csv,
		"typed SDK client must decode a non-nil InputSerialization.Csv")
	assert.Equal(t, glaciertypes.FileHeaderInfoNone, out.SelectParameters.InputSerialization.Csv.FileHeaderInfo)
	require.NotNil(t, out.SelectParameters.OutputSerialization,
		"typed SDK client must decode a non-nil OutputSerialization")
	assert.NotNil(t, out.SelectParameters.OutputSerialization.Csv,
		"typed SDK client must decode a non-nil OutputSerialization.Csv")
}
