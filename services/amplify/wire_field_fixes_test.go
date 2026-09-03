package amplify_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestCreateBranch_BackendComputeRoleEnableSkewProtectionRoundTrip proves
// CreateBranchInput's Backend/ComputeRoleArn/EnableSkewProtection (real,
// accepted request members -- api_op_CreateBranch.go) were previously
// silently dropped in their entirety: gopherstack's createBranchRequest had
// no field for any of the three, so a real client setting them on
// CreateBranch/UpdateBranch got a Branch that never reflected them on any
// later Get/List/Update.
func TestCreateBranch_BackendComputeRoleEnableSkewProtectionRoundTrip(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("branch-fields-app")})
	require.NoError(t, err)

	created, err := client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
		AppId:      app.App.AppId,
		BranchName: aws.String("main"),
		Backend: &amplifytypes.Backend{
			StackArn: aws.String("arn:aws:cloudformation:us-east-1:000000000000:stack/s1"),
		},
		ComputeRoleArn:       aws.String("arn:aws:iam::000000000000:role/compute-role"),
		EnableSkewProtection: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Branch.Backend)
	require.Equal(
		t,
		"arn:aws:cloudformation:us-east-1:000000000000:stack/s1",
		aws.ToString(created.Branch.Backend.StackArn),
	)
	require.Equal(t, "arn:aws:iam::000000000000:role/compute-role", aws.ToString(created.Branch.ComputeRoleArn))
	require.True(t, aws.ToBool(created.Branch.EnableSkewProtection))

	got, err := client.GetBranch(t.Context(), &amplifysdk.GetBranchInput{
		AppId:      app.App.AppId,
		BranchName: aws.String("main"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Branch.Backend, "Backend must round-trip through GetBranch")
	require.Equal(
		t,
		"arn:aws:cloudformation:us-east-1:000000000000:stack/s1",
		aws.ToString(got.Branch.Backend.StackArn),
	)
	require.Equal(t, "arn:aws:iam::000000000000:role/compute-role", aws.ToString(got.Branch.ComputeRoleArn))
	require.True(t, aws.ToBool(got.Branch.EnableSkewProtection))
}

// TestCreateApp_ComputeRoleArnJobConfigRoundTrip proves CreateAppInput's
// ComputeRoleArn/JobConfig (real, accepted request members --
// api_op_CreateApp.go) were previously silently dropped: gopherstack's
// createAppRequest had no field for either, so a real client setting them
// never saw them reflected on GetApp/ListApps.
func TestCreateApp_ComputeRoleArnJobConfigRoundTrip(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	created, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
		Name:           aws.String("job-config-app"),
		ComputeRoleArn: aws.String("arn:aws:iam::000000000000:role/app-compute-role"),
		JobConfig: &amplifytypes.JobConfig{
			BuildComputeType: amplifytypes.BuildComputeTypeLarge16gb,
		},
	})
	require.NoError(t, err)
	require.Equal(
		t,
		"arn:aws:iam::000000000000:role/app-compute-role",
		aws.ToString(created.App.ComputeRoleArn),
	)
	require.NotNil(t, created.App.JobConfig)
	require.Equal(t, amplifytypes.BuildComputeTypeLarge16gb, created.App.JobConfig.BuildComputeType)

	got, err := client.GetApp(t.Context(), &amplifysdk.GetAppInput{AppId: created.App.AppId})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:iam::000000000000:role/app-compute-role", aws.ToString(got.App.ComputeRoleArn))
	require.NotNil(t, got.App.JobConfig, "JobConfig must round-trip through GetApp")
	require.Equal(t, amplifytypes.BuildComputeTypeLarge16gb, got.App.JobConfig.BuildComputeType)
}

// TestCreateDomainAssociation_AutoSubDomainAndCertificateRoundTrip proves
// CreateDomainAssociationInput's AutoSubDomainCreationPatterns/
// AutoSubDomainIAMRole/CertificateSettings (real, accepted request members --
// api_op_CreateDomainAssociation.go) were previously silently dropped in
// their entirety: gopherstack's inline request struct had no field for any of
// the three, so a real client configuring auto-subdomain patterns/IAM role or
// a custom certificate never saw them reflected on Get/List, and
// DomainAssociation.Certificate (computable from the stored certificate type)
// was never emitted at all.
func TestCreateDomainAssociation_AutoSubDomainAndCertificateRoundTrip(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("domain-fields-app")})
	require.NoError(t, err)

	created, err := client.CreateDomainAssociation(t.Context(), &amplifysdk.CreateDomainAssociationInput{
		AppId:      app.App.AppId,
		DomainName: aws.String("example.com"),
		SubDomainSettings: []amplifytypes.SubDomainSetting{
			{Prefix: aws.String("www"), BranchName: aws.String("main")},
		},
		AutoSubDomainCreationPatterns: []string{
			"feature/*",
			"pr-*",
		},
		AutoSubDomainIAMRole: aws.String("arn:aws:iam::000000000000:role/auto-subdomain"),
		CertificateSettings: &amplifytypes.CertificateSettings{
			Type:                 amplifytypes.CertificateTypeCustom,
			CustomCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/c1"),
		},
	})
	require.NoError(t, err)

	da := created.DomainAssociation
	require.ElementsMatch(t, []string{"feature/*", "pr-*"}, da.AutoSubDomainCreationPatterns)
	require.Equal(t, "arn:aws:iam::000000000000:role/auto-subdomain", aws.ToString(da.AutoSubDomainIAMRole))
	require.NotNil(t, da.Certificate, "Certificate must be computed from CertificateSettings")
	require.Equal(t, amplifytypes.CertificateTypeCustom, da.Certificate.Type)
	require.Equal(
		t,
		"arn:aws:acm:us-east-1:000000000000:certificate/c1",
		aws.ToString(da.Certificate.CustomCertificateArn),
	)

	got, err := client.GetDomainAssociation(t.Context(), &amplifysdk.GetDomainAssociationInput{
		AppId:      app.App.AppId,
		DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"feature/*", "pr-*"}, got.DomainAssociation.AutoSubDomainCreationPatterns)
	require.Equal(
		t,
		"arn:aws:iam::000000000000:role/auto-subdomain",
		aws.ToString(got.DomainAssociation.AutoSubDomainIAMRole),
	)
	require.NotNil(t, got.DomainAssociation.Certificate)
	require.Equal(t, amplifytypes.CertificateTypeCustom, got.DomainAssociation.Certificate.Type)
}

// TestCreateDomainAssociation_DefaultCertificateIsAmplifyManaged proves the
// default Certificate.Type real Amplify applies when CertificateSettings is
// omitted (AMPLIFY_MANAGED) is computed too, not just the CUSTOM path above.
func TestCreateDomainAssociation_DefaultCertificateIsAmplifyManaged(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("default-cert-app")})
	require.NoError(t, err)

	created, err := client.CreateDomainAssociation(t.Context(), &amplifysdk.CreateDomainAssociationInput{
		AppId:      app.App.AppId,
		DomainName: aws.String("default.example.com"),
		SubDomainSettings: []amplifytypes.SubDomainSetting{
			{Prefix: aws.String("www"), BranchName: aws.String("main")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.DomainAssociation.Certificate)
	require.Equal(t, amplifytypes.CertificateTypeAmplifyManaged, created.DomainAssociation.Certificate.Type)
}
