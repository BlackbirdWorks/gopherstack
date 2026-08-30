package macie2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// newTestMacie2SDKClient stands up the real aws-sdk-go-v2 macie2 client
// against an httptest server running this package's Handler.
func newTestMacie2SDKClient(t *testing.T, h *macie2.Handler) *macie2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return macie2sdk.NewFromConfig(cfg, func(o *macie2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetBucketStatistics_RealClient drives GetBucketStatistics through a
// real SDK client. Real GetBucketStatisticsOutput.ClassifiableObjectCount is
// the total number of classifiable OBJECTS across the buckets (confirmed at
// aws-sdk-go-v2/service/macie2's api_op_GetBucketStatistics.go), not a count
// of buckets that have any -- the pre-fix "classifiableBucketCount" key
// didn't exist on the real shape at all, so a real client's
// ClassifiableObjectCount was always 0 regardless of what DescribeBuckets
// showed for the same data. ObjectCount/SizeInBytes were missing entirely,
// even though the backend already tracks both per bucket.
func TestGetBucketStatistics_RealClient(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	h := macie2.NewHandler(b)
	client := newTestMacie2SDKClient(t, h)

	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:               "000000000000",
		BucketArn:               "arn:aws:s3:::bucket-a",
		BucketName:              "bucket-a",
		Region:                  "us-east-1",
		ClassifiableObjectCount: 10,
		ClassifiableSizeInBytes: 1000,
		ObjectCount:             25,
		SizeInBytes:             4096,
		PublicAccess:            "NOT_PUBLIC",
		EncryptionType:          "AES256",
		SharedAccess:            "NOT_SHARED",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:               "000000000000",
		BucketArn:               "arn:aws:s3:::bucket-b",
		BucketName:              "bucket-b",
		Region:                  "us-east-1",
		ClassifiableObjectCount: 5,
		ClassifiableSizeInBytes: 500,
		ObjectCount:             8,
		SizeInBytes:             2048,
		PublicAccess:            "NOT_PUBLIC",
		EncryptionType:          "AES256",
		SharedAccess:            "NOT_SHARED",
	})

	out, err := client.GetBucketStatistics(t.Context(), &macie2sdk.GetBucketStatisticsInput{})
	require.NoError(t, err)

	assert.Equal(t, int64(15), aws.ToInt64(out.ClassifiableObjectCount))
	assert.Equal(t, int64(1500), aws.ToInt64(out.ClassifiableSizeInBytes))
	assert.Equal(t, int64(33), aws.ToInt64(out.ObjectCount))
	assert.Equal(t, int64(6144), aws.ToInt64(out.SizeInBytes))
}

// TestUpdateResourceProfile_SensitivityScoreOverridden_RealClient drives
// UpdateResourceProfile then GetResourceProfile through a real SDK client.
// Real GetResourceProfileOutput's flag is "sensitivityScoreOverridden"
// (confirmed at api_op_GetResourceProfile.go); the pre-fix
// "sensitivityScoreOverride" key doesn't exist on the real shape, so a real
// client's SensitivityScoreOverridden stayed false even after a manual
// override was set.
func TestUpdateResourceProfile_SensitivityScoreOverridden_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	resourceARN := "arn:aws:s3:::override-bucket"

	_, err := client.UpdateResourceProfile(t.Context(), &macie2sdk.UpdateResourceProfileInput{
		ResourceArn:              aws.String(resourceARN),
		SensitivityScoreOverride: aws.Int32(100),
	})
	require.NoError(t, err)

	out, err := client.GetResourceProfile(t.Context(), &macie2sdk.GetResourceProfileInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)

	assert.True(t, aws.ToBool(out.SensitivityScoreOverridden))
	assert.Equal(t, int32(100), aws.ToInt32(out.SensitivityScore))
}

// TestGetSensitivityInspectionTemplate_RealClient drives
// ListSensitivityInspectionTemplates then GetSensitivityInspectionTemplate
// through a real SDK client. Real GetSensitivityInspectionTemplateOutput's ID
// field is wire key "sensitivityInspectionTemplateId" (confirmed at
// aws-sdk-go-v2/service/macie2@v1.54.4 deserializers.go:7839, function
// awsRestjson1_deserializeOpDocumentGetSensitivityInspectionTemplateOutput) --
// distinct from the "id" key used by the list-view SensitivityInspectionTemplatesEntry
// shape (deserializers.go:21230). The pre-fix backend emitted "id" for the Get
// response too, so a real client's SensitivityInspectionTemplateId was always
// nil regardless of the template's actual ID.
func TestGetSensitivityInspectionTemplate_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	listOut, err := client.ListSensitivityInspectionTemplates(
		t.Context(), &macie2sdk.ListSensitivityInspectionTemplatesInput{},
	)
	require.NoError(t, err)
	require.Len(t, listOut.SensitivityInspectionTemplates, 1)

	id := aws.ToString(listOut.SensitivityInspectionTemplates[0].Id)
	require.NotEmpty(t, id)

	_, err = client.UpdateSensitivityInspectionTemplate(
		t.Context(),
		&macie2sdk.UpdateSensitivityInspectionTemplateInput{
			Id:          aws.String(id),
			Description: aws.String("real-client description"),
		},
	)
	require.NoError(t, err)

	out, err := client.GetSensitivityInspectionTemplate(t.Context(), &macie2sdk.GetSensitivityInspectionTemplateInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)

	assert.Equal(t, id, aws.ToString(out.SensitivityInspectionTemplateId))
	assert.Equal(t, "real-client description", aws.ToString(out.Description))
}

// TestCreateMember_RelationshipStatus_RealClient proves GetMemberOutput.
// RelationshipStatus decodes as a real types.RelationshipStatus member.
// Real RelationshipStatus is mixed-case ("Created"/"Invited"/"Enabled"/...,
// macie2@v1.54.4 types/enums.go:811); pre-fix, gopherstack emitted
// all-caps "CREATED", not a member of that enum.
func TestCreateMember_RelationshipStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	_, err := client.CreateMember(t.Context(), &macie2sdk.CreateMemberInput{
		Account: &types.AccountDetail{
			AccountId: aws.String("111111111111"),
			Email:     aws.String("member@example.com"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetMember(t.Context(), &macie2sdk.GetMemberInput{
		Id: aws.String("111111111111"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RelationshipStatusCreated, out.RelationshipStatus)
}

// TestCreateInvitations_RelationshipStatus_RealClient proves
// ListInvitationsOutput's Invitation.RelationshipStatus decodes as a real
// types.RelationshipStatus member. Pre-fix, gopherstack emitted all-caps
// "INVITED", not a member of RelationshipStatus (whose invited value is
// "Invited").
func TestCreateInvitations_RelationshipStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	_, err := client.CreateInvitations(t.Context(), &macie2sdk.CreateInvitationsInput{
		AccountIds: []string{"222222222222"},
	})
	require.NoError(t, err)

	out, err := client.ListInvitations(t.Context(), &macie2sdk.ListInvitationsInput{})
	require.NoError(t, err)
	require.Len(t, out.Invitations, 1)
	assert.Equal(t, types.RelationshipStatusInvited, out.Invitations[0].RelationshipStatus)
}

// TestAcceptInvitation_RelationshipStatus_RealClient proves
// GetAdministratorAccountOutput.Administrator.RelationshipStatus decodes as
// a real types.RelationshipStatus member. Pre-fix, gopherstack reused the
// shared statusEnabled constant ("ENABLED", correct for the unrelated
// MacieStatus/RevealStatus enums) here too, but RelationshipStatus's
// enabled value is "Enabled".
func TestAcceptInvitation_RelationshipStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("222222222222", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	_, err := client.AcceptInvitation(t.Context(), &macie2sdk.AcceptInvitationInput{
		AdministratorAccountId: aws.String("111111111111"),
		InvitationId:           aws.String("some-invitation-id"),
	})
	require.NoError(t, err)

	out, err := client.GetAdministratorAccount(t.Context(), &macie2sdk.GetAdministratorAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Administrator)
	assert.Equal(t, types.RelationshipStatusEnabled, out.Administrator.RelationshipStatus)
}

// TestDisassociateMember_RelationshipStatus_RealClient proves GetMemberOutput.
// RelationshipStatus decodes as a real types.RelationshipStatus member after
// DisassociateMember. Pre-fix, gopherstack set the field to all-caps
// "DISASSOCIATED", which is not a member of RelationshipStatus at all
// (macie2@v1.54.4 types/enums.go:811-824); the value for an
// administrator-disassociated member is "Removed".
func TestDisassociateMember_RelationshipStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	_, err := client.CreateMember(t.Context(), &macie2sdk.CreateMemberInput{
		Account: &types.AccountDetail{
			AccountId: aws.String("333333333333"),
			Email:     aws.String("member3@example.com"),
		},
	})
	require.NoError(t, err)

	_, err = client.DisassociateMember(t.Context(), &macie2sdk.DisassociateMemberInput{
		Id: aws.String("333333333333"),
	})
	require.NoError(t, err)

	out, err := client.GetMember(t.Context(), &macie2sdk.GetMemberInput{
		Id: aws.String("333333333333"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RelationshipStatusRemoved, out.RelationshipStatus)
}

// TestDeclineInvitations_RelationshipStatus_RealClient proves
// ListInvitationsOutput's Invitation.RelationshipStatus decodes as a real
// types.RelationshipStatus member after DeclineInvitations. Pre-fix,
// gopherstack set the field to all-caps "RESIGNED", which is not a member of
// RelationshipStatus at all; the real mixed-case value is "Resigned".
func TestDeclineInvitations_RelationshipStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMacie2SDKClient(t, h)

	_, err := client.CreateInvitations(t.Context(), &macie2sdk.CreateInvitationsInput{
		AccountIds: []string{"444444444444"},
	})
	require.NoError(t, err)

	_, err = client.DeclineInvitations(t.Context(), &macie2sdk.DeclineInvitationsInput{
		AccountIds: []string{"444444444444"},
	})
	require.NoError(t, err)

	out, err := client.ListInvitations(t.Context(), &macie2sdk.ListInvitationsInput{})
	require.NoError(t, err)
	require.Len(t, out.Invitations, 1)
	assert.Equal(t, types.RelationshipStatusResigned, out.Invitations[0].RelationshipStatus)
}
