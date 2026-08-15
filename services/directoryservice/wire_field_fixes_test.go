package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestADAssessment_RealClientRoundTrip drives Start/Describe/List/Delete
// through a real SDK client. Before this fix, DeleteADAssessmentInput and
// DescribeADAssessmentInput are {AssessmentId} only on the pinned SDK
// (directoryservice@v1.41.4 api_op_DeleteADAssessment.go/
// api_op_DescribeADAssessment.go) -- neither has a DirectoryId member -- so
// the real client never sent one, and this handler's own validation
// rejected every real Describe/Delete call with "DirectoryId ... required".
// Separately, DescribeADAssessmentOutput/ListADAssessmentsOutput wrap in
// "Assessment"/"Assessments", not the fabricated "ADAssessment"/
// "ADAssessments" this handler used to emit, so even a request that got past
// validation would have decoded to a nil/empty result.
func TestADAssessment_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	started, err := client.StartADAssessment(t.Context(), &directoryservicesdk.StartADAssessmentInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)
	assessID := aws.ToString(started.AssessmentId)
	require.NotEmpty(t, assessID)

	listed, err := client.ListADAssessments(t.Context(), &directoryservicesdk.ListADAssessmentsInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)
	require.Len(t, listed.Assessments, 1, "ListADAssessmentsOutput.Assessments must decode -- real wrapper key")
	assert.Equal(t, assessID, aws.ToString(listed.Assessments[0].AssessmentId))

	described, err := client.DescribeADAssessment(t.Context(), &directoryservicesdk.DescribeADAssessmentInput{
		AssessmentId: aws.String(assessID),
	})
	require.NoError(t, err, "real DescribeADAssessmentInput has no DirectoryId member")
	require.NotNil(t, described.Assessment, "DescribeADAssessmentOutput.Assessment must decode -- real wrapper key")
	assert.Equal(t, assessID, aws.ToString(described.Assessment.AssessmentId))
	assert.Equal(t, dirID, aws.ToString(described.Assessment.DirectoryId))

	_, err = client.DeleteADAssessment(t.Context(), &directoryservicesdk.DeleteADAssessmentInput{
		AssessmentId: aws.String(assessID),
	})
	require.NoError(t, err, "real DeleteADAssessmentInput has no DirectoryId member")

	afterDelete, err := client.ListADAssessments(t.Context(), &directoryservicesdk.ListADAssessmentsInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)
	assert.Empty(t, afterDelete.Assessments)
}

// TestRegisterCertificate_OCSPUrl_RoundTrip proves RegisterCertificateInput's
// real, optional ClientCertAuthSettings.OCSPUrl member (directoryservice@
// v1.41.4 api_op_RegisterCertificate.go/types.ClientCertAuthSettings) is
// captured and echoed back on DescribeCertificate's Certificate.
// ClientCertAuthSettings, instead of being silently discarded entirely (this
// backend had no OCSPUrl-shaped field anywhere before this fix).
func TestRegisterCertificate_OCSPUrl_RoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	ocspURL := "http://ocsp.example.com"

	registered, err := client.RegisterCertificate(t.Context(), &directoryservicesdk.RegisterCertificateInput{
		DirectoryId:     aws.String(dirID),
		CertificateData: aws.String(testCertPEM),
		Type:            types.CertificateTypeClientCertAuth,
		ClientCertAuthSettings: &types.ClientCertAuthSettings{
			OCSPUrl: aws.String(ocspURL),
		},
	})
	require.NoError(t, err)
	certID := aws.ToString(registered.CertificateId)
	require.NotEmpty(t, certID)

	described, err := client.DescribeCertificate(t.Context(), &directoryservicesdk.DescribeCertificateInput{
		DirectoryId:   aws.String(dirID),
		CertificateId: aws.String(certID),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Certificate)
	require.NotNil(t, described.Certificate.ClientCertAuthSettings,
		"ClientCertAuthSettings must round-trip -- previously discarded entirely")
	assert.Equal(t, ocspURL, aws.ToString(described.Certificate.ClientCertAuthSettings.OCSPUrl))
}

// TestDescribeUpdateDirectory_RealClientRoundTrip proves DescribeUpdateDirectory
// decodes through a real SDK client. Before this fix, the response wrapped
// entries in the fabricated "UpdateDirectoryInfo" key instead of the real
// DescribeUpdateDirectoryOutput.UpdateActivities (directoryservice@v1.41.4
// api_op_DescribeUpdateDirectory.go), so resp.UpdateActivities always decoded
// to nil/empty. Separately, every entry also carried NewValue/PreviousValue
// as flat "" strings where the real types.UpdateInfoEntry member type is
// *types.UpdateValue (a nested struct) -- a real client's decode hard-failed
// on type mismatch, not just silent-empty.
func TestDescribeUpdateDirectory_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	_, err = client.UpdateDirectorySetup(t.Context(), &directoryservicesdk.UpdateDirectorySetupInput{
		DirectoryId: aws.String(dirID),
		UpdateType:  types.UpdateTypeOs,
	})
	require.NoError(t, err)

	described, err := client.DescribeUpdateDirectory(t.Context(), &directoryservicesdk.DescribeUpdateDirectoryInput{
		DirectoryId: aws.String(dirID),
		UpdateType:  types.UpdateTypeOs,
	})
	require.NoError(t, err, "must decode without a type-mismatch error on NewValue/PreviousValue")
	require.Len(t, described.UpdateActivities, 1,
		"DescribeUpdateDirectoryOutput.UpdateActivities must decode -- real wrapper key")
	assert.Equal(t, types.UpdateStatusUpdated, described.UpdateActivities[0].Status)
}

// TestDescribeSettings_RequestStatus_RealClientRoundTrip proves
// SettingEntry.RequestStatus decodes through a real SDK client. Real
// types.SettingEntry (directoryservice@v1.41.4 types/types.go) has no
// "Status" member at all -- the response emitted the request-side filter
// field's name ("Status", DescribeSettingsInput.Status) instead of the real
// response member "RequestStatus", so a real client's RequestStatus field
// always decoded to its zero value.
func TestDescribeSettings_RequestStatus_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	_, err = client.UpdateSettings(t.Context(), &directoryservicesdk.UpdateSettingsInput{
		DirectoryId: aws.String(dirID),
		Settings:    []types.Setting{{Name: aws.String("TLS_1_0"), Value: aws.String("Disable")}},
	})
	require.NoError(t, err)

	described, err := client.DescribeSettings(t.Context(), &directoryservicesdk.DescribeSettingsInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)
	require.Len(t, described.SettingEntries, 1)
	assert.Equal(t, types.DirectoryConfigurationStatusUpdated, described.SettingEntries[0].RequestStatus,
		"RequestStatus must decode -- real response member, not the request-side filter's \"Status\" name")
}

// TestAcceptSharedDirectory_RealClientRoundTrip proves AcceptSharedDirectory
// decodes a full types.SharedDirectory through a real SDK client.
// AcceptSharedDirectoryOutput.SharedDirectory (directoryservice@v1.41.4
// api_op_AcceptSharedDirectory.go) is the full SharedDirectory object, the
// same shape DescribeSharedDirectories already emits correctly -- before
// this fix, Accept's response carried only SharedDirectoryId and every other
// field (OwnerDirectoryId, ShareStatus, ...) silently decoded to nil/zero.
func TestAcceptSharedDirectory_RealClientRoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("111111111111", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	shared, err := client.ShareDirectory(t.Context(), &directoryservicesdk.ShareDirectoryInput{
		DirectoryId: aws.String(dirID),
		ShareMethod: types.ShareMethodHandshake,
		ShareTarget: &types.ShareTarget{Id: aws.String("222222222222"), Type: types.TargetTypeAccount},
	})
	require.NoError(t, err)
	sharedDirID := aws.ToString(shared.SharedDirectoryId)
	require.NotEmpty(t, sharedDirID)

	accepted, err := client.AcceptSharedDirectory(t.Context(), &directoryservicesdk.AcceptSharedDirectoryInput{
		SharedDirectoryId: aws.String(sharedDirID),
	})
	require.NoError(t, err)
	require.NotNil(t, accepted.SharedDirectory,
		"AcceptSharedDirectoryOutput.SharedDirectory must decode as a full object")
	assert.Equal(t, sharedDirID, aws.ToString(accepted.SharedDirectory.SharedDirectoryId))
	assert.Equal(t, dirID, aws.ToString(accepted.SharedDirectory.OwnerDirectoryId))
	assert.Equal(t, "222222222222", aws.ToString(accepted.SharedDirectory.SharedAccountId))
	assert.Equal(t, types.ShareStatusShared, accepted.SharedDirectory.ShareStatus)
	assert.NotNil(t, accepted.SharedDirectory.LastUpdatedDateTime)
}
