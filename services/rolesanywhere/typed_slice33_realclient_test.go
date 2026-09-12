package rolesanywhere_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rasdk "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	ratypes "github.com/aws/aws-sdk-go-v2/service/rolesanywhere/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

const raRealClientRegion = "us-east-1"

// newTestRolesAnywhereClient stands up the real aws-sdk-go-v2 rolesanywhere
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- the
// same pattern services/swf's wire_sdk_roundtrip_test.go uses (this service
// had no prior typed-client helper).
func newTestRolesAnywhereClient(t *testing.T, h *rolesanywhere.Handler) *rasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(raRealClientRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return rasdk.NewFromConfig(cfg, func(o *rasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSlice33_RolesAnywhere_RealClient drives every gopherstack-n3zi
// typed-slice-33 uncovered rolesanywhere op through the real aws-sdk-go-v2
// client.
func TestSlice33_RolesAnywhere_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testProfileLifecycleRealClient, "profile_lifecycle"},
		{testCrlLifecycleRealClient, "crl_lifecycle"},
		{testAttributeMappingsRealClient, "attribute_mappings"},
		{testNotificationSettingsRealClient, "notification_settings"},
		{testTagsRealClient, "tags"},
		{testTrustAnchorLifecycleExtrasRealClient, "trust_anchor_lifecycle_extras"},
		{testSubjectsStructuralGapRealClient, "subjects_structural_gap"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newTestRolesAnywhereHandler() *rolesanywhere.Handler {
	return rolesanywhere.NewHandler(rolesanywhere.NewInMemoryBackend("123456789012", raRealClientRegion))
}

// testProfileLifecycleRealClient covers CreateProfile, GetProfile,
// ListProfiles, UpdateProfile, EnableProfile, DisableProfile, DeleteProfile.
func testProfileLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateProfile(ctx, &rasdk.CreateProfileInput{
		Name:     aws.String("my-profile"),
		RoleArns: []string{"arn:aws:iam::123456789012:role/my-role"},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Profile)
	profileID := aws.ToString(createOut.Profile.ProfileId)
	assert.True(t, aws.ToBool(createOut.Profile.Enabled))

	getOut, err := client.GetProfile(ctx, &rasdk.GetProfileInput{ProfileId: aws.String(profileID)})
	require.NoError(t, err)
	assert.Equal(t, "my-profile", aws.ToString(getOut.Profile.Name))

	listOut, err := client.ListProfiles(ctx, &rasdk.ListProfilesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Profiles, 1)
	assert.Equal(t, profileID, aws.ToString(listOut.Profiles[0].ProfileId))

	updOut, err := client.UpdateProfile(ctx, &rasdk.UpdateProfileInput{
		ProfileId: aws.String(profileID),
		Name:      aws.String("my-profile-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-profile-renamed", aws.ToString(updOut.Profile.Name))

	disOut, err := client.DisableProfile(ctx, &rasdk.DisableProfileInput{ProfileId: aws.String(profileID)})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disOut.Profile.Enabled))

	enOut, err := client.EnableProfile(ctx, &rasdk.EnableProfileInput{ProfileId: aws.String(profileID)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enOut.Profile.Enabled))

	delOut, err := client.DeleteProfile(ctx, &rasdk.DeleteProfileInput{ProfileId: aws.String(profileID)})
	require.NoError(t, err)
	assert.Equal(t, profileID, aws.ToString(delOut.Profile.ProfileId))

	listOut2, err := client.ListProfiles(ctx, &rasdk.ListProfilesInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Profiles)
}

// testCrlLifecycleRealClient covers ImportCrl, GetCrl, ListCrls, UpdateCrl,
// EnableCrl, DisableCrl, DeleteCrl.
func testCrlLifecycleRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	taOut, err := client.CreateTrustAnchor(ctx, &rasdk.CreateTrustAnchorInput{
		Name:   aws.String("crl-anchor"),
		Source: &ratypes.Source{SourceType: ratypes.TrustAnchorTypeCertificateBundle},
	})
	require.NoError(t, err)
	taARN := aws.ToString(taOut.TrustAnchor.TrustAnchorArn)

	importOut, err := client.ImportCrl(ctx, &rasdk.ImportCrlInput{
		Name:           aws.String("my-crl"),
		CrlData:        []byte("fake-der-crl-bytes"),
		TrustAnchorArn: aws.String(taARN),
	})
	require.NoError(t, err)
	require.NotNil(t, importOut.Crl)
	crlID := aws.ToString(importOut.Crl.CrlId)
	assert.True(t, aws.ToBool(importOut.Crl.Enabled))

	getOut, err := client.GetCrl(ctx, &rasdk.GetCrlInput{CrlId: aws.String(crlID)})
	require.NoError(t, err)
	assert.Equal(t, "my-crl", aws.ToString(getOut.Crl.Name))

	listOut, err := client.ListCrls(ctx, &rasdk.ListCrlsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Crls, 1)

	updOut, err := client.UpdateCrl(ctx, &rasdk.UpdateCrlInput{
		CrlId:   aws.String(crlID),
		Name:    aws.String("my-crl-renamed"),
		CrlData: []byte("new-fake-der-crl-bytes"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-crl-renamed", aws.ToString(updOut.Crl.Name))

	disOut, err := client.DisableCrl(ctx, &rasdk.DisableCrlInput{CrlId: aws.String(crlID)})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disOut.Crl.Enabled))

	enOut, err := client.EnableCrl(ctx, &rasdk.EnableCrlInput{CrlId: aws.String(crlID)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enOut.Crl.Enabled))

	delOut, err := client.DeleteCrl(ctx, &rasdk.DeleteCrlInput{CrlId: aws.String(crlID)})
	require.NoError(t, err)
	assert.Equal(t, crlID, aws.ToString(delOut.Crl.CrlId))

	listOut2, err := client.ListCrls(ctx, &rasdk.ListCrlsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Crls)
}

// testAttributeMappingsRealClient covers PutAttributeMapping and
// DeleteAttributeMapping.
func testAttributeMappingsRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	profOut, err := client.CreateProfile(ctx, &rasdk.CreateProfileInput{
		Name:     aws.String("mapping-profile"),
		RoleArns: []string{"arn:aws:iam::123456789012:role/my-role"},
	})
	require.NoError(t, err)
	profileID := aws.ToString(profOut.Profile.ProfileId)

	putOut, err := client.PutAttributeMapping(ctx, &rasdk.PutAttributeMappingInput{
		ProfileId:        aws.String(profileID),
		CertificateField: ratypes.CertificateFieldX509Subject,
		MappingRules:     []ratypes.MappingRule{{Specifier: aws.String("CN")}},
	})
	require.NoError(t, err)
	require.Len(t, putOut.Profile.AttributeMappings, 1)
	assert.Equal(t, ratypes.CertificateFieldX509Subject, putOut.Profile.AttributeMappings[0].CertificateField)
	require.Len(t, putOut.Profile.AttributeMappings[0].MappingRules, 1)
	assert.Equal(t, "CN", aws.ToString(putOut.Profile.AttributeMappings[0].MappingRules[0].Specifier))

	// Naming every specifier explicitly (rather than omitting Specifiers,
	// which removeFieldMapping uses to drop the field entirely) leaves the
	// CertificateField entry present with an empty MappingRules list --
	// deliberate, pre-existing behavior (removeSpecifiers,
	// attribute_mappings.go), matching crl_subject_test.go's own
	// TestAttributeMapping_PutGetDelete precedent.
	delOut, err := client.DeleteAttributeMapping(ctx, &rasdk.DeleteAttributeMappingInput{
		ProfileId:        aws.String(profileID),
		CertificateField: ratypes.CertificateFieldX509Subject,
		Specifiers:       []string{"CN"},
	})
	require.NoError(t, err)
	require.Len(t, delOut.Profile.AttributeMappings, 1)
	assert.Empty(t, delOut.Profile.AttributeMappings[0].MappingRules)
}

// testNotificationSettingsRealClient covers PutNotificationSettings and
// ResetNotificationSettings.
func testNotificationSettingsRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	taOut, err := client.CreateTrustAnchor(ctx, &rasdk.CreateTrustAnchorInput{
		Name:   aws.String("notif-anchor"),
		Source: &ratypes.Source{SourceType: ratypes.TrustAnchorTypeCertificateBundle},
	})
	require.NoError(t, err)
	taID := aws.ToString(taOut.TrustAnchor.TrustAnchorId)

	putOut, err := client.PutNotificationSettings(ctx, &rasdk.PutNotificationSettingsInput{
		TrustAnchorId: aws.String(taID),
		NotificationSettings: []ratypes.NotificationSetting{
			{Event: ratypes.NotificationEventCaCertificateExpiry, Enabled: aws.Bool(true)},
		},
	})
	require.NoError(t, err)
	require.Len(t, putOut.TrustAnchor.NotificationSettings, 1)
	assert.Equal(
		t, ratypes.NotificationEventCaCertificateExpiry, putOut.TrustAnchor.NotificationSettings[0].Event,
	)
	assert.True(t, aws.ToBool(putOut.TrustAnchor.NotificationSettings[0].Enabled))

	resetOut, err := client.ResetNotificationSettings(ctx, &rasdk.ResetNotificationSettingsInput{
		TrustAnchorId: aws.String(taID),
		NotificationSettingKeys: []ratypes.NotificationSettingKey{
			{Event: ratypes.NotificationEventCaCertificateExpiry},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, resetOut.TrustAnchor.NotificationSettings)
}

// testTagsRealClient covers TagResource, UntagResource,
// ListTagsForResource.
func testTagsRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	profOut, err := client.CreateProfile(ctx, &rasdk.CreateProfileInput{
		Name:     aws.String("tag-profile"),
		RoleArns: []string{"arn:aws:iam::123456789012:role/my-role"},
	})
	require.NoError(t, err)
	profileARN := aws.ToString(profOut.Profile.ProfileArn)

	_, err = client.TagResource(ctx, &rasdk.TagResourceInput{
		ResourceArn: aws.String(profileARN),
		Tags: []ratypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("net")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &rasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(profileARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &rasdk.UntagResourceInput{
		ResourceArn: aws.String(profileARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &rasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(profileARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
}

// testTrustAnchorLifecycleExtrasRealClient covers EnableTrustAnchor,
// DisableTrustAnchor, UpdateTrustAnchor (CreateTrustAnchor/
// DescribeTrustAnchor-equivalent reads are already typed-covered via
// test/integration/rolesanywhere_test.go).
func testTrustAnchorLifecycleExtrasRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	taOut, err := client.CreateTrustAnchor(ctx, &rasdk.CreateTrustAnchorInput{
		Name:   aws.String("lifecycle-anchor"),
		Source: &ratypes.Source{SourceType: ratypes.TrustAnchorTypeCertificateBundle},
	})
	require.NoError(t, err)
	taID := aws.ToString(taOut.TrustAnchor.TrustAnchorId)
	assert.True(t, aws.ToBool(taOut.TrustAnchor.Enabled))

	disOut, err := client.DisableTrustAnchor(ctx, &rasdk.DisableTrustAnchorInput{TrustAnchorId: aws.String(taID)})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(disOut.TrustAnchor.Enabled))

	enOut, err := client.EnableTrustAnchor(ctx, &rasdk.EnableTrustAnchorInput{TrustAnchorId: aws.String(taID)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enOut.TrustAnchor.Enabled))

	updOut, err := client.UpdateTrustAnchor(ctx, &rasdk.UpdateTrustAnchorInput{
		TrustAnchorId: aws.String(taID),
		Name:          aws.String("lifecycle-anchor-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-anchor-renamed", aws.ToString(updOut.TrustAnchor.Name))
}

// testSubjectsStructuralGapRealClient covers GetSubject and ListSubjects.
// Per PARITY.md's documented gap, gopherstack never populates the Subject
// store -- there is no CreateSession endpoint (Roles Anywhere's session
// data-plane is mTLS-authenticated, out of scope, distinct from this
// SigV4 control-plane API). This proves the real, non-fabricated behavior
// through the typed client: an honest empty list and a real
// ResourceNotFoundException for an unknown subject ID.
func testSubjectsStructuralGapRealClient(t *testing.T) {
	t.Helper()

	h := newTestRolesAnywhereHandler()
	client := newTestRolesAnywhereClient(t, h)
	ctx := t.Context()

	listOut, err := client.ListSubjects(ctx, &rasdk.ListSubjectsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut.Subjects)

	_, err = client.GetSubject(
		ctx,
		&rasdk.GetSubjectInput{SubjectId: aws.String("00000000-0000-0000-0000-000000000000")},
	)
	require.Error(t, err)

	var nfe *ratypes.ResourceNotFoundException
	assert.ErrorAs(t, err, &nfe)
}
