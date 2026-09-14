package iot_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func timeNow() time.Time          { return time.Now() }
func timeNowMinusHour() time.Time { return time.Now().Add(-time.Hour) }

// TestRealClient_CoreResources covers iot's core resource op families
// (gopherstack-n3zi): thing types, thing groups, certificates, CA
// certificates, policies, topic rules and destinations, jobs, commands,
// fleet indexing, provisioning, OTA/streams, security profiles/audit,
// mitigation actions, domain configurations, billing groups, role aliases,
// authorizers and tags. Each subtest creates real state through the typed
// aws-sdk-go-v2 client and asserts decoded response values.
func TestRealClient_CoreResources(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testThingTypesRealClient, "thing_types"},
		{testThingGroupsRealClient, "thing_groups"},
		{testThingsRealClient, "things"},
		{testCertificatesRealClient, "certificates"},
		{testCACertificatesRealClient, "ca_certificates"},
		{testPoliciesRealClient, "policies"},
		{testTopicRulesRealClient, "topic_rules"},
		{testTopicRuleDestinationsRealClient, "topic_rule_destinations"},
		{testJobsExtraRealClient, "jobs_extra"},
		{testCommandsRealClient, "commands"},
		{testFleetIndexingRealClient, "fleet_indexing"},
		{testProvisioningRealClient, "provisioning"},
		{testOTAStreamsRealClient, "ota_streams"},
		{testSecurityAuditRealClient, "security_audit"},
		{testMitigationActionsRealClient, "mitigation_actions"},
		{testDomainConfigRealClient, "domain_config"},
		{testBillingGroupsRealClient, "billing_groups"},
		{testRoleAliasesRealClient, "role_aliases"},
		{testAuthorizersRealClient, "authorizers"},
		{testCustomMetricsDimensionsRealClient, "custom_metrics_dimensions"},
		{testFleetMetricsRealClient, "fleet_metrics"},
		{testPackagesRealClient, "packages"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newIoTTestClient(t *testing.T) *iotsdk.Client {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	return newTestIoTClient(t, h)
}

// testThingTypesRealClient covers CreateThingType, DescribeThingType,
// ListThingTypes, UpdateThingType, DeprecateThingType, DeleteThingType.
func testThingTypesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	created, err := client.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
		ThingTypeProperties: &types.ThingTypeProperties{
			ThingTypeDescription: aws.String("desc"),
			SearchableAttributes: []string{"color"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.ThingTypeArn))

	desc, err := client.DescribeThingType(t.Context(), &iotsdk.DescribeThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
	})
	require.NoError(t, err)
	assert.Equal(t, "desc", aws.ToString(desc.ThingTypeProperties.ThingTypeDescription))
	require.NotNil(t, desc.ThingTypeMetadata)
	assert.False(t, desc.ThingTypeMetadata.Deprecated)

	list, err := client.ListThingTypes(t.Context(), &iotsdk.ListThingTypesInput{})
	require.NoError(t, err)
	found := false

	for _, tt := range list.ThingTypes {
		if aws.ToString(tt.ThingTypeName) == "slice4-tt" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateThingType(t.Context(), &iotsdk.UpdateThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
		ThingTypeProperties: &types.ThingTypeProperties{
			ThingTypeDescription: aws.String("updated desc"),
		},
	})
	require.NoError(t, err)

	desc, err = client.DescribeThingType(t.Context(), &iotsdk.DescribeThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated desc", aws.ToString(desc.ThingTypeProperties.ThingTypeDescription))

	_, err = client.DeprecateThingType(t.Context(), &iotsdk.DeprecateThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeThingType(t.Context(), &iotsdk.DescribeThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
	})
	require.NoError(t, err)
	assert.True(t, desc.ThingTypeMetadata.Deprecated)

	// Real AWS only allows DeleteThingType on a deprecated thing type, so
	// this leaves it deprecated rather than undeprecating first.
	_, err = client.DeleteThingType(t.Context(), &iotsdk.DeleteThingTypeInput{
		ThingTypeName: aws.String("slice4-tt"),
	})
	require.NoError(t, err)
}

// testThingGroupsRealClient covers CreateThingGroup, UpdateThingGroup,
// DeleteThingGroup, AddThingToThingGroup, ListThingGroupsForThing,
// UpdateThingGroupsForThing, RemoveThingFromThingGroup,
// CreateDynamicThingGroup, UpdateDynamicThingGroup, DeleteDynamicThingGroup.
func testThingGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
		ThingGroupName: aws.String("slice4-tg"),
	})
	require.NoError(t, err)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-tg-thing")})
	require.NoError(t, err)

	_, err = client.AddThingToThingGroup(t.Context(), &iotsdk.AddThingToThingGroupInput{
		ThingName:      aws.String("slice4-tg-thing"),
		ThingGroupName: aws.String("slice4-tg"),
	})
	require.NoError(t, err)

	groups, err := client.ListThingGroupsForThing(t.Context(), &iotsdk.ListThingGroupsForThingInput{
		ThingName: aws.String("slice4-tg-thing"),
	})
	require.NoError(t, err)
	require.Len(t, groups.ThingGroups, 1)
	assert.Equal(t, "slice4-tg", aws.ToString(groups.ThingGroups[0].GroupName))

	updated, err := client.UpdateThingGroup(t.Context(), &iotsdk.UpdateThingGroupInput{
		ThingGroupName: aws.String("slice4-tg"),
		ThingGroupProperties: &types.ThingGroupProperties{
			ThingGroupDescription: aws.String("updated"),
		},
	})
	require.NoError(t, err)
	assert.NotZero(t, updated.Version)

	_, err = client.CreateThingGroup(
		t.Context(),
		&iotsdk.CreateThingGroupInput{ThingGroupName: aws.String("slice4-tg2")},
	)
	require.NoError(t, err)

	_, err = client.UpdateThingGroupsForThing(t.Context(), &iotsdk.UpdateThingGroupsForThingInput{
		ThingName:           aws.String("slice4-tg-thing"),
		ThingGroupsToAdd:    []string{"slice4-tg2"},
		ThingGroupsToRemove: []string{"slice4-tg"},
	})
	require.NoError(t, err)

	groups, err = client.ListThingGroupsForThing(t.Context(), &iotsdk.ListThingGroupsForThingInput{
		ThingName: aws.String("slice4-tg-thing"),
	})
	require.NoError(t, err)
	require.Len(t, groups.ThingGroups, 1)
	assert.Equal(t, "slice4-tg2", aws.ToString(groups.ThingGroups[0].GroupName))

	_, err = client.RemoveThingFromThingGroup(t.Context(), &iotsdk.RemoveThingFromThingGroupInput{
		ThingName:      aws.String("slice4-tg-thing"),
		ThingGroupName: aws.String("slice4-tg2"),
	})
	require.NoError(t, err)

	groups, err = client.ListThingGroupsForThing(t.Context(), &iotsdk.ListThingGroupsForThingInput{
		ThingName: aws.String("slice4-tg-thing"),
	})
	require.NoError(t, err)
	assert.Empty(t, groups.ThingGroups)

	_, err = client.DeleteThingGroup(
		t.Context(),
		&iotsdk.DeleteThingGroupInput{ThingGroupName: aws.String("slice4-tg")},
	)
	require.NoError(t, err)

	dyn, err := client.CreateDynamicThingGroup(t.Context(), &iotsdk.CreateDynamicThingGroupInput{
		ThingGroupName: aws.String("slice4-dtg"),
		QueryString:    aws.String("thingName:slice4-tg-thing"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(dyn.ThingGroupArn))

	dynUpdated, err := client.UpdateDynamicThingGroup(t.Context(), &iotsdk.UpdateDynamicThingGroupInput{
		ThingGroupName: aws.String("slice4-dtg"),
		ThingGroupProperties: &types.ThingGroupProperties{
			ThingGroupDescription: aws.String("dynamic"),
		},
	})
	require.NoError(t, err)
	assert.NotZero(t, dynUpdated.Version)

	_, err = client.DeleteDynamicThingGroup(t.Context(), &iotsdk.DeleteDynamicThingGroupInput{
		ThingGroupName: aws.String("slice4-dtg"),
	})
	require.NoError(t, err)
}

// testThingsRealClient covers DescribeThing, UpdateThing, ListThings,
// ListThingPrincipals, ListPrincipalThings, DetachThingPrincipal,
// GetThingConnectivityData.
func testThingsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
		ThingName: aws.String("slice4-thing"),
		AttributePayload: &types.AttributePayload{
			Attributes: map[string]string{"model": "x"},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeThing(t.Context(), &iotsdk.DescribeThingInput{ThingName: aws.String("slice4-thing")})
	require.NoError(t, err)
	assert.Equal(t, "x", desc.Attributes["model"])

	_, err = client.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
		ThingName: aws.String("slice4-thing"),
		AttributePayload: &types.AttributePayload{
			Attributes: map[string]string{"model": "y"},
		},
	})
	require.NoError(t, err)

	desc, err = client.DescribeThing(t.Context(), &iotsdk.DescribeThingInput{ThingName: aws.String("slice4-thing")})
	require.NoError(t, err)
	assert.Equal(t, "y", desc.Attributes["model"])

	list, err := client.ListThings(t.Context(), &iotsdk.ListThingsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.Things)

	cert, err := client.CreateKeysAndCertificate(t.Context(), &iotsdk.CreateKeysAndCertificateInput{SetAsActive: true})
	require.NoError(t, err)

	_, err = client.AttachThingPrincipal(t.Context(), &iotsdk.AttachThingPrincipalInput{
		ThingName: aws.String("slice4-thing"),
		Principal: cert.CertificateArn,
	})
	require.NoError(t, err)

	principals, err := client.ListThingPrincipals(t.Context(), &iotsdk.ListThingPrincipalsInput{
		ThingName: aws.String("slice4-thing"),
	})
	require.NoError(t, err)
	require.Len(t, principals.Principals, 1)
	assert.Equal(t, aws.ToString(cert.CertificateArn), principals.Principals[0])

	thingsForPrincipal, err := client.ListPrincipalThings(t.Context(), &iotsdk.ListPrincipalThingsInput{
		Principal: cert.CertificateArn,
	})
	require.NoError(t, err)
	require.Len(t, thingsForPrincipal.Things, 1)
	assert.Equal(t, "slice4-thing", thingsForPrincipal.Things[0])

	_, err = client.DetachThingPrincipal(t.Context(), &iotsdk.DetachThingPrincipalInput{
		ThingName: aws.String("slice4-thing"),
		Principal: cert.CertificateArn,
	})
	require.NoError(t, err)

	principals, err = client.ListThingPrincipals(t.Context(), &iotsdk.ListThingPrincipalsInput{
		ThingName: aws.String("slice4-thing"),
	})
	require.NoError(t, err)
	assert.Empty(t, principals.Principals)

	connectivity, err := client.GetThingConnectivityData(t.Context(), &iotsdk.GetThingConnectivityDataInput{
		ThingName: aws.String("slice4-thing"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice4-thing", aws.ToString(connectivity.ThingName))
}

// testCertificatesRealClient covers CreateKeysAndCertificate,
// CreateCertificateFromCsr, RegisterCertificate,
// RegisterCertificateWithoutCA, UpdateCertificate, DeleteCertificate,
// ListOutgoingCertificates, TransferCertificate,
// AcceptCertificateTransfer, RejectCertificateTransfer,
// CancelCertificateTransfer.
func testCertificatesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	created, err := client.CreateKeysAndCertificate(
		t.Context(),
		&iotsdk.CreateKeysAndCertificateInput{SetAsActive: true},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.CertificateId))
	assert.NotEmpty(t, aws.ToString(created.CertificatePem))
	require.NotNil(t, created.KeyPair)
	assert.NotEmpty(t, aws.ToString(created.KeyPair.PrivateKey))

	fromCsr, err := client.CreateCertificateFromCsr(t.Context(), &iotsdk.CreateCertificateFromCsrInput{
		CertificateSigningRequest: aws.String(
			"-----BEGIN CERTIFICATE REQUEST-----\nzz\n-----END CERTIFICATE REQUEST-----",
		),
		SetAsActive: true,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(fromCsr.CertificateId))

	fromCsrDesc, err := client.DescribeCertificate(t.Context(), &iotsdk.DescribeCertificateInput{
		CertificateId: fromCsr.CertificateId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CertificateStatusActive, fromCsrDesc.CertificateDescription.Status)

	registered, err := client.RegisterCertificate(t.Context(), &iotsdk.RegisterCertificateInput{
		CertificatePem: aws.String("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"),
		Status:         types.CertificateStatusActive,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(registered.CertificateId))

	registeredNoCA, err := client.RegisterCertificateWithoutCA(t.Context(), &iotsdk.RegisterCertificateWithoutCAInput{
		CertificatePem: aws.String("-----BEGIN CERTIFICATE-----\ndef\n-----END CERTIFICATE-----"),
		Status:         types.CertificateStatusActive,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(registeredNoCA.CertificateId))

	_, err = client.UpdateCertificate(t.Context(), &iotsdk.UpdateCertificateInput{
		CertificateId: created.CertificateId,
		NewStatus:     types.CertificateStatusInactive,
	})
	require.NoError(t, err)

	desc, err := client.DescribeCertificate(t.Context(), &iotsdk.DescribeCertificateInput{
		CertificateId: created.CertificateId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CertificateStatusInactive, desc.CertificateDescription.Status)

	_, err = client.TransferCertificate(t.Context(), &iotsdk.TransferCertificateInput{
		CertificateId:    created.CertificateId,
		TargetAwsAccount: aws.String("222222222222"),
		TransferMessage:  aws.String("please accept"),
	})
	require.NoError(t, err)

	outgoing, err := client.ListOutgoingCertificates(t.Context(), &iotsdk.ListOutgoingCertificatesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, outgoing.OutgoingCertificates)

	_, err = client.CancelCertificateTransfer(t.Context(), &iotsdk.CancelCertificateTransferInput{
		CertificateId: created.CertificateId,
	})
	require.NoError(t, err)

	_, err = client.TransferCertificate(t.Context(), &iotsdk.TransferCertificateInput{
		CertificateId:    created.CertificateId,
		TargetAwsAccount: aws.String("222222222222"),
	})
	require.NoError(t, err)

	_, err = client.RejectCertificateTransfer(t.Context(), &iotsdk.RejectCertificateTransferInput{
		CertificateId: created.CertificateId,
		RejectReason:  aws.String("nope"),
	})
	require.NoError(t, err)

	_, err = client.TransferCertificate(t.Context(), &iotsdk.TransferCertificateInput{
		CertificateId:    created.CertificateId,
		TargetAwsAccount: aws.String("222222222222"),
	})
	require.NoError(t, err)

	_, err = client.AcceptCertificateTransfer(t.Context(), &iotsdk.AcceptCertificateTransferInput{
		CertificateId: created.CertificateId,
		SetAsActive:   true,
	})
	require.NoError(t, err)

	// Real AWS rejects DeleteCertificate on an ACTIVE certificate regardless
	// of ForceDelete (which only bypasses the separate "attached to a thing"
	// block, not the active-status one), so deactivate first.
	_, err = client.UpdateCertificate(t.Context(), &iotsdk.UpdateCertificateInput{
		CertificateId: registeredNoCA.CertificateId,
		NewStatus:     types.CertificateStatusInactive,
	})
	require.NoError(t, err)

	_, err = client.DeleteCertificate(t.Context(), &iotsdk.DeleteCertificateInput{
		CertificateId: registeredNoCA.CertificateId,
	})
	require.NoError(t, err)
}

// testCACertificatesRealClient covers RegisterCACertificate,
// DescribeCACertificate, UpdateCACertificate, DeleteCACertificate,
// GetRegistrationCode, DeleteRegistrationCode.
func testCACertificatesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	registered, err := client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
		CaCertificate:   aws.String("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----"),
		CertificateMode: types.CertificateModeSniOnly,
		SetAsActive:     true,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(registered.CertificateId))

	desc, err := client.DescribeCACertificate(t.Context(), &iotsdk.DescribeCACertificateInput{
		CertificateId: registered.CertificateId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.CertificateDescription)
	assert.Equal(t, types.CACertificateStatusActive, desc.CertificateDescription.Status)

	_, err = client.UpdateCACertificate(t.Context(), &iotsdk.UpdateCACertificateInput{
		CertificateId: registered.CertificateId,
		NewStatus:     types.CACertificateStatusInactive,
	})
	require.NoError(t, err)

	desc, err = client.DescribeCACertificate(t.Context(), &iotsdk.DescribeCACertificateInput{
		CertificateId: registered.CertificateId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CACertificateStatusInactive, desc.CertificateDescription.Status)

	regCode, err := client.GetRegistrationCode(t.Context(), &iotsdk.GetRegistrationCodeInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(regCode.RegistrationCode))

	_, err = client.DeleteRegistrationCode(t.Context(), &iotsdk.DeleteRegistrationCodeInput{})
	require.NoError(t, err)

	_, err = client.DeleteCACertificate(t.Context(), &iotsdk.DeleteCACertificateInput{
		CertificateId: registered.CertificateId,
	})
	require.NoError(t, err)
}

// testPoliciesRealClient covers CreatePolicyVersion, GetPolicy,
// GetPolicyVersion, ListPolicyVersions, SetDefaultPolicyVersion,
// DeletePolicyVersion, ListTargetsForPolicy, ListAttachedPolicies,
// ListPolicyPrincipals, AttachPolicy, DetachPolicy, AttachPrincipalPolicy,
// DetachPrincipalPolicy.
func testPoliciesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	doc1 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}]}`
	_, err := client.CreatePolicy(t.Context(), &iotsdk.CreatePolicyInput{
		PolicyName:     aws.String("slice4-policy"),
		PolicyDocument: aws.String(doc1),
	})
	require.NoError(t, err)

	policy, err := client.GetPolicy(t.Context(), &iotsdk.GetPolicyInput{PolicyName: aws.String("slice4-policy")})
	require.NoError(t, err)
	assert.Equal(t, doc1, aws.ToString(policy.PolicyDocument))
	assert.Equal(t, "1", aws.ToString(policy.DefaultVersionId))

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:Publish","Resource":"*"}]}`
	v2, err := client.CreatePolicyVersion(t.Context(), &iotsdk.CreatePolicyVersionInput{
		PolicyName:     aws.String("slice4-policy"),
		PolicyDocument: aws.String(doc2),
		SetAsDefault:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(v2.PolicyVersionId))
	assert.True(t, v2.IsDefaultVersion)

	gv, err := client.GetPolicyVersion(t.Context(), &iotsdk.GetPolicyVersionInput{
		PolicyName: aws.String("slice4-policy"), PolicyVersionId: aws.String("2"),
	})
	require.NoError(t, err)
	assert.Equal(t, doc2, aws.ToString(gv.PolicyDocument))

	versions, err := client.ListPolicyVersions(t.Context(), &iotsdk.ListPolicyVersionsInput{
		PolicyName: aws.String("slice4-policy"),
	})
	require.NoError(t, err)
	assert.Len(t, versions.PolicyVersions, 2)

	_, err = client.SetDefaultPolicyVersion(t.Context(), &iotsdk.SetDefaultPolicyVersionInput{
		PolicyName: aws.String("slice4-policy"), PolicyVersionId: aws.String("1"),
	})
	require.NoError(t, err)

	policy, err = client.GetPolicy(t.Context(), &iotsdk.GetPolicyInput{PolicyName: aws.String("slice4-policy")})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(policy.DefaultVersionId))

	_, err = client.DeletePolicyVersion(t.Context(), &iotsdk.DeletePolicyVersionInput{
		PolicyName: aws.String("slice4-policy"), PolicyVersionId: aws.String("2"),
	})
	require.NoError(t, err)

	versions, err = client.ListPolicyVersions(t.Context(), &iotsdk.ListPolicyVersionsInput{
		PolicyName: aws.String("slice4-policy"),
	})
	require.NoError(t, err)
	assert.Len(t, versions.PolicyVersions, 1)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-policy-thing")})
	require.NoError(t, err)

	thingARN := "arn:aws:iot:us-east-1:000000000000:thing/slice4-policy-thing"

	_, err = client.AttachPolicy(t.Context(), &iotsdk.AttachPolicyInput{
		PolicyName: aws.String("slice4-policy"), Target: aws.String(thingARN),
	})
	require.NoError(t, err)

	targets, err := client.ListTargetsForPolicy(t.Context(), &iotsdk.ListTargetsForPolicyInput{
		PolicyName: aws.String("slice4-policy"),
	})
	require.NoError(t, err)
	assert.Contains(t, targets.Targets, thingARN)

	attached, err := client.ListAttachedPolicies(t.Context(), &iotsdk.ListAttachedPoliciesInput{
		Target: aws.String(thingARN),
	})
	require.NoError(t, err)
	require.Len(t, attached.Policies, 1)
	assert.Equal(t, "slice4-policy", aws.ToString(attached.Policies[0].PolicyName))

	//nolint:staticcheck // deprecated but still a real, uncovered op
	principals, err := client.ListPolicyPrincipals(t.Context(), &iotsdk.ListPolicyPrincipalsInput{
		PolicyName: aws.String("slice4-policy"),
	})
	require.NoError(t, err)
	assert.Contains(t, principals.Principals, thingARN)

	_, err = client.DetachPolicy(t.Context(), &iotsdk.DetachPolicyInput{
		PolicyName: aws.String("slice4-policy"), Target: aws.String(thingARN),
	})
	require.NoError(t, err)

	targets, err = client.ListTargetsForPolicy(t.Context(), &iotsdk.ListTargetsForPolicyInput{
		PolicyName: aws.String("slice4-policy"),
	})
	require.NoError(t, err)
	assert.NotContains(t, targets.Targets, thingARN)

	cert, err := client.CreateKeysAndCertificate(t.Context(), &iotsdk.CreateKeysAndCertificateInput{SetAsActive: true})
	require.NoError(t, err)

	//nolint:staticcheck // deprecated but still a real, uncovered op
	_, err = client.AttachPrincipalPolicy(t.Context(), &iotsdk.AttachPrincipalPolicyInput{
		PolicyName: aws.String("slice4-policy"), Principal: cert.CertificateArn,
	})
	require.NoError(t, err)

	//nolint:staticcheck // deprecated but still a real, uncovered op
	_, err = client.DetachPrincipalPolicy(t.Context(), &iotsdk.DetachPrincipalPolicyInput{
		PolicyName: aws.String("slice4-policy"), Principal: cert.CertificateArn,
	})
	require.NoError(t, err)
}

// testTopicRulesRealClient covers GetTopicRule, ReplaceTopicRule,
// DeleteTopicRule, EnableTopicRule, DisableTopicRule.
func testTopicRulesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	sql := "SELECT * FROM 'topic/a'"
	_, err := client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
		RuleName: aws.String("slice4rule"),
		TopicRulePayload: &types.TopicRulePayload{
			Sql:         aws.String(sql),
			Description: aws.String("orig"),
			Actions: []types.Action{
				{
					Republish: &types.RepublishAction{
						RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
						Topic:   aws.String("topic/b"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)
	require.NotNil(t, got.Rule)
	assert.Equal(t, sql, aws.ToString(got.Rule.Sql))
	assert.False(t, aws.ToBool(got.Rule.RuleDisabled))

	_, err = client.DisableTopicRule(t.Context(), &iotsdk.DisableTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)

	got, err = client.GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(got.Rule.RuleDisabled))

	_, err = client.EnableTopicRule(t.Context(), &iotsdk.EnableTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)

	newSQL := "SELECT * FROM 'topic/c'"
	_, err = client.ReplaceTopicRule(t.Context(), &iotsdk.ReplaceTopicRuleInput{
		RuleName: aws.String("slice4rule"),
		TopicRulePayload: &types.TopicRulePayload{
			Sql: aws.String(newSQL),
			Actions: []types.Action{
				{
					Republish: &types.RepublishAction{
						RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
						Topic:   aws.String("topic/b"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err = client.GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)
	assert.Equal(t, newSQL, aws.ToString(got.Rule.Sql))
	assert.False(t, aws.ToBool(got.Rule.RuleDisabled))

	_, err = client.DeleteTopicRule(t.Context(), &iotsdk.DeleteTopicRuleInput{RuleName: aws.String("slice4rule")})
	require.NoError(t, err)
}

// testTopicRuleDestinationsRealClient covers CreateTopicRuleDestination,
// GetTopicRuleDestination, ListTopicRuleDestinations,
// UpdateTopicRuleDestination, DeleteTopicRuleDestination.
func testTopicRuleDestinationsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	created, err := client.CreateTopicRuleDestination(t.Context(), &iotsdk.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &types.TopicRuleDestinationConfiguration{
			VpcConfiguration: &types.VpcDestinationConfiguration{
				RoleArn:   aws.String("arn:aws:iam::000000000000:role/vpc"),
				SubnetIds: []string{"subnet-1"},
				VpcId:     aws.String("vpc-1"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.TopicRuleDestination)
	destARN := created.TopicRuleDestination.Arn
	assert.Equal(t, types.TopicRuleDestinationStatusEnabled, created.TopicRuleDestination.Status)

	got, err := client.GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{Arn: destARN})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(destARN), aws.ToString(got.TopicRuleDestination.Arn))

	list, err := client.ListTopicRuleDestinations(t.Context(), &iotsdk.ListTopicRuleDestinationsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.DestinationSummaries)

	_, err = client.UpdateTopicRuleDestination(t.Context(), &iotsdk.UpdateTopicRuleDestinationInput{
		Arn:    destARN,
		Status: types.TopicRuleDestinationStatusDisabled,
	})
	require.NoError(t, err)

	got, err = client.GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{Arn: destARN})
	require.NoError(t, err)
	assert.Equal(t, types.TopicRuleDestinationStatusDisabled, got.TopicRuleDestination.Status)

	_, err = client.DeleteTopicRuleDestination(t.Context(), &iotsdk.DeleteTopicRuleDestinationInput{Arn: destARN})
	require.NoError(t, err)
}

// testJobsExtraRealClient covers DeleteJob, DeleteJobExecution,
// CancelJobExecution, ListJobExecutionsForThing, GetJobDocument,
// DescribeManagedJobTemplate, ListManagedJobTemplates.
func testJobsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-job-thing")})
	require.NoError(t, err)

	thingARN := "arn:aws:iot:us-east-1:000000000000:thing/slice4-job-thing"

	_, err = client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
		JobId:    aws.String("slice4-job"),
		Targets:  []string{thingARN},
		Document: aws.String(`{"step":1}`),
	})
	require.NoError(t, err)

	doc, err := client.GetJobDocument(t.Context(), &iotsdk.GetJobDocumentInput{JobId: aws.String("slice4-job")})
	require.NoError(t, err)
	assert.Equal(t, `{"step":1}`, aws.ToString(doc.Document))

	execs, err := client.ListJobExecutionsForThing(t.Context(), &iotsdk.ListJobExecutionsForThingInput{
		ThingName: aws.String("slice4-job-thing"),
	})
	require.NoError(t, err)
	require.Len(t, execs.ExecutionSummaries, 1)
	assert.Equal(t, "slice4-job", aws.ToString(execs.ExecutionSummaries[0].JobId))

	_, err = client.CancelJobExecution(t.Context(), &iotsdk.CancelJobExecutionInput{
		JobId: aws.String("slice4-job"), ThingName: aws.String("slice4-job-thing"),
	})
	require.NoError(t, err)

	_, err = client.DeleteJobExecution(t.Context(), &iotsdk.DeleteJobExecutionInput{
		JobId: aws.String("slice4-job"), ThingName: aws.String("slice4-job-thing"), ExecutionNumber: aws.Int64(1),
	})
	require.NoError(t, err)

	_, err = client.DeleteJob(t.Context(), &iotsdk.DeleteJobInput{JobId: aws.String("slice4-job"), Force: true})
	require.NoError(t, err)

	tmpl, err := client.ListManagedJobTemplates(t.Context(), &iotsdk.ListManagedJobTemplatesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, tmpl.ManagedJobTemplates)

	name := tmpl.ManagedJobTemplates[0].TemplateName

	described, err := client.DescribeManagedJobTemplate(t.Context(), &iotsdk.DescribeManagedJobTemplateInput{
		TemplateName: name,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(name), aws.ToString(described.TemplateName))
}

// testCommandsRealClient covers ListCommands, UpdateCommand.
func testCommandsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
		CommandId:   aws.String("slice4-cmd"),
		Namespace:   types.CommandNamespaceAWSIoT,
		DisplayName: aws.String("orig"),
		Payload: &types.CommandPayload{
			Content:     []byte(`{"reboot":true}`),
			ContentType: aws.String("application/json"),
		},
	})
	require.NoError(t, err)

	list, err := client.ListCommands(t.Context(), &iotsdk.ListCommandsInput{})
	require.NoError(t, err)
	found := false

	for _, cmd := range list.Commands {
		if aws.ToString(cmd.CommandId) == "slice4-cmd" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateCommand(t.Context(), &iotsdk.UpdateCommandInput{
		CommandId:   aws.String("slice4-cmd"),
		DisplayName: aws.String("renamed"),
		Deprecated:  aws.Bool(true),
	})
	require.NoError(t, err)

	got, err := client.GetCommand(t.Context(), &iotsdk.GetCommandInput{CommandId: aws.String("slice4-cmd")})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(got.DisplayName))
	assert.True(t, aws.ToBool(got.Deprecated))
}

// testFleetIndexingRealClient covers DescribeIndex, ListIndices,
// GetCardinality, GetPercentiles, GetStatistics, GetBucketsAggregation.
func testFleetIndexingRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.UpdateIndexingConfiguration(t.Context(), &iotsdk.UpdateIndexingConfigurationInput{
		ThingIndexingConfiguration: &types.ThingIndexingConfiguration{
			ThingIndexingMode: types.ThingIndexingModeRegistry,
		},
	})
	require.NoError(t, err)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-idx-thing")})
	require.NoError(t, err)

	indices, err := client.ListIndices(t.Context(), &iotsdk.ListIndicesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, indices.IndexNames)

	desc, err := client.DescribeIndex(
		t.Context(),
		&iotsdk.DescribeIndexInput{IndexName: aws.String(indices.IndexNames[0])},
	)
	require.NoError(t, err)
	assert.Equal(t, indices.IndexNames[0], aws.ToString(desc.IndexName))

	card, err := client.GetCardinality(t.Context(), &iotsdk.GetCardinalityInput{
		QueryString: aws.String("*"), AggregationField: aws.String("thingTypeName.keyword"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, card.Cardinality, int32(0))

	stats, err := client.GetStatistics(t.Context(), &iotsdk.GetStatisticsInput{
		QueryString: aws.String("*"), AggregationField: aws.String("thingTypeName.keyword"),
	})
	require.NoError(t, err)
	require.NotNil(t, stats.Statistics)

	pcts, err := client.GetPercentiles(t.Context(), &iotsdk.GetPercentilesInput{
		QueryString: aws.String("*"), AggregationField: aws.String("thingName.keyword"),
	})
	require.NoError(t, err)
	_ = pcts

	buckets, err := client.GetBucketsAggregation(t.Context(), &iotsdk.GetBucketsAggregationInput{
		QueryString:      aws.String("*"),
		AggregationField: aws.String("thingTypeName.keyword"),
		BucketsAggregationType: &types.BucketsAggregationType{
			TermsAggregation: &types.TermsAggregation{MaxBuckets: aws.Int32(10)},
		},
	})
	require.NoError(t, err)
	_ = buckets
}

// testProvisioningRealClient covers CreateProvisioningClaim, RegisterThing,
// DeleteProvisioningTemplate, DeleteProvisioningTemplateVersion,
// DescribeProvisioningTemplateVersion, ListProvisioningTemplateVersions,
// ListProvisioningTemplates.
func testProvisioningRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	body := `{"Parameters":{},"Resources":{},"DeviceConfiguration":{}}`
	_, err := client.CreateProvisioningTemplate(t.Context(), &iotsdk.CreateProvisioningTemplateInput{
		TemplateName:        aws.String("slice4-tmpl"),
		TemplateBody:        aws.String(body),
		ProvisioningRoleArn: aws.String("arn:aws:iam::000000000000:role/prov"),
	})
	require.NoError(t, err)

	list, err := client.ListProvisioningTemplates(t.Context(), &iotsdk.ListProvisioningTemplatesInput{})
	require.NoError(t, err)
	found := false

	for _, tp := range list.Templates {
		if aws.ToString(tp.TemplateName) == "slice4-tmpl" {
			found = true
		}
	}

	assert.True(t, found)

	versions, err := client.ListProvisioningTemplateVersions(t.Context(), &iotsdk.ListProvisioningTemplateVersionsInput{
		TemplateName: aws.String("slice4-tmpl"),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)

	vDesc, err := client.DescribeProvisioningTemplateVersion(
		t.Context(),
		&iotsdk.DescribeProvisioningTemplateVersionInput{
			TemplateName: aws.String("slice4-tmpl"), VersionId: versions.Versions[0].VersionId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, body, aws.ToString(vDesc.TemplateBody))

	claim, err := client.CreateProvisioningClaim(t.Context(), &iotsdk.CreateProvisioningClaimInput{
		TemplateName: aws.String("slice4-tmpl"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(claim.CertificatePem))
	require.NotNil(t, claim.KeyPair)

	registered, err := client.RegisterThing(t.Context(), &iotsdk.RegisterThingInput{
		TemplateBody: aws.String(body),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(registered.CertificatePem))

	_, err = client.DeleteProvisioningTemplateVersion(t.Context(), &iotsdk.DeleteProvisioningTemplateVersionInput{
		TemplateName: aws.String("slice4-tmpl"), VersionId: aws.Int32(2),
	})
	require.Error(t, err)

	_, err = client.DeleteProvisioningTemplate(t.Context(), &iotsdk.DeleteProvisioningTemplateInput{
		TemplateName: aws.String("slice4-tmpl"),
	})
	require.NoError(t, err)
}

// testOTAStreamsRealClient covers DeleteOTAUpdate, GetOTAUpdate,
// ListOTAUpdates, DeleteStream, DescribeStream, UpdateStream.
func testOTAStreamsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateStream(t.Context(), &iotsdk.CreateStreamInput{
		StreamId: aws.String("slice4-stream"),
		RoleArn:  aws.String("arn:aws:iam::000000000000:role/stream"),
		Files: []types.StreamFile{{
			FileId: aws.Int32(1),
			S3Location: &types.S3Location{
				Bucket: aws.String("b"), Key: aws.String("k"),
			},
		}},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &iotsdk.DescribeStreamInput{StreamId: aws.String("slice4-stream")})
	require.NoError(t, err)
	require.NotNil(t, desc.StreamInfo)
	assert.Equal(t, "slice4-stream", aws.ToString(desc.StreamInfo.StreamId))

	_, err = client.UpdateStream(t.Context(), &iotsdk.UpdateStreamInput{
		StreamId:    aws.String("slice4-stream"),
		Description: aws.String("updated"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeStream(t.Context(), &iotsdk.DescribeStreamInput{StreamId: aws.String("slice4-stream")})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.StreamInfo.Description))

	_, err = client.DeleteStream(t.Context(), &iotsdk.DeleteStreamInput{StreamId: aws.String("slice4-stream")})
	require.NoError(t, err)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-ota-thing")})
	require.NoError(t, err)

	thingARN := "arn:aws:iot:us-east-1:000000000000:thing/slice4-ota-thing"

	_, err = client.CreateOTAUpdate(t.Context(), &iotsdk.CreateOTAUpdateInput{
		OtaUpdateId: aws.String("slice4-ota"),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/ota"),
		Targets:     []string{thingARN},
		Files: []types.OTAUpdateFile{{
			FileName: aws.String("firmware.bin"),
		}},
	})
	require.NoError(t, err)

	got, err := client.GetOTAUpdate(t.Context(), &iotsdk.GetOTAUpdateInput{OtaUpdateId: aws.String("slice4-ota")})
	require.NoError(t, err)
	require.NotNil(t, got.OtaUpdateInfo)
	assert.Equal(t, "slice4-ota", aws.ToString(got.OtaUpdateInfo.OtaUpdateId))

	list, err := client.ListOTAUpdates(t.Context(), &iotsdk.ListOTAUpdatesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.OtaUpdates)

	_, err = client.DeleteOTAUpdate(t.Context(), &iotsdk.DeleteOTAUpdateInput{
		OtaUpdateId: aws.String("slice4-ota"), ForceDeleteAWSJob: true,
	})
	require.NoError(t, err)
}

// testSecurityAuditRealClient covers DescribeSecurityProfile,
// UpdateSecurityProfile, ValidateSecurityProfileBehaviors,
// CreateScheduledAudit, DeleteScheduledAudit, DescribeScheduledAudit,
// ListScheduledAudits, UpdateScheduledAudit, StartOnDemandAuditTask,
// CancelAuditTask, DescribeAuditTask, ListAuditTasks.
func testSecurityAuditRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	behaviors := []types.Behavior{{
		Name:   aws.String("high-msgs"),
		Metric: aws.String("aws:num-messages-sent"),
		Criteria: &types.BehaviorCriteria{
			ComparisonOperator: types.ComparisonOperatorGreaterThan,
			Value:              &types.MetricValue{Count: aws.Int64(100)},
		},
	}}

	valid, err := client.ValidateSecurityProfileBehaviors(t.Context(), &iotsdk.ValidateSecurityProfileBehaviorsInput{
		Behaviors: behaviors,
	})
	require.NoError(t, err)
	assert.True(t, valid.Valid)

	_, err = client.CreateSecurityProfile(t.Context(), &iotsdk.CreateSecurityProfileInput{
		SecurityProfileName: aws.String("slice4-sp"),
		Behaviors:           behaviors,
	})
	require.NoError(t, err)

	desc, err := client.DescribeSecurityProfile(t.Context(), &iotsdk.DescribeSecurityProfileInput{
		SecurityProfileName: aws.String("slice4-sp"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Behaviors, 1)

	_, err = client.UpdateSecurityProfile(t.Context(), &iotsdk.UpdateSecurityProfileInput{
		SecurityProfileName:        aws.String("slice4-sp"),
		SecurityProfileDescription: aws.String("updated"),
		ExpectedVersion:            aws.Int64(1),
	})
	require.NoError(t, err)

	desc, err = client.DescribeSecurityProfile(t.Context(), &iotsdk.DescribeSecurityProfileInput{
		SecurityProfileName: aws.String("slice4-sp"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.SecurityProfileDescription))

	_, err = client.CreateScheduledAudit(t.Context(), &iotsdk.CreateScheduledAuditInput{
		ScheduledAuditName: aws.String("slice4-audit"),
		Frequency:          types.AuditFrequencyDaily,
		TargetCheckNames:   []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	require.NoError(t, err)

	sDesc, err := client.DescribeScheduledAudit(t.Context(), &iotsdk.DescribeScheduledAuditInput{
		ScheduledAuditName: aws.String("slice4-audit"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AuditFrequencyDaily, sDesc.Frequency)

	sList, err := client.ListScheduledAudits(t.Context(), &iotsdk.ListScheduledAuditsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, sList.ScheduledAudits)

	_, err = client.UpdateScheduledAudit(t.Context(), &iotsdk.UpdateScheduledAuditInput{
		ScheduledAuditName: aws.String("slice4-audit"),
		Frequency:          types.AuditFrequencyWeekly,
		DayOfWeek:          types.DayOfWeekMon,
		TargetCheckNames:   []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	require.NoError(t, err)

	sDesc, err = client.DescribeScheduledAudit(t.Context(), &iotsdk.DescribeScheduledAuditInput{
		ScheduledAuditName: aws.String("slice4-audit"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AuditFrequencyWeekly, sDesc.Frequency)

	_, err = client.DeleteScheduledAudit(t.Context(), &iotsdk.DeleteScheduledAuditInput{
		ScheduledAuditName: aws.String("slice4-audit"),
	})
	require.NoError(t, err)

	started, err := client.StartOnDemandAuditTask(t.Context(), &iotsdk.StartOnDemandAuditTaskInput{
		TargetCheckNames: []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(started.TaskId))

	tDesc, err := client.DescribeAuditTask(t.Context(), &iotsdk.DescribeAuditTaskInput{TaskId: started.TaskId})
	require.NoError(t, err)
	assert.Equal(t, types.AuditTaskStatusInProgress, tDesc.TaskStatus)

	tList, err := client.ListAuditTasks(t.Context(), &iotsdk.ListAuditTasksInput{
		StartTime: aws.Time(timeNowMinusHour()), EndTime: aws.Time(timeNow()),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, tList.Tasks)

	_, err = client.CancelAuditTask(t.Context(), &iotsdk.CancelAuditTaskInput{TaskId: started.TaskId})
	require.NoError(t, err)

	tDesc, err = client.DescribeAuditTask(t.Context(), &iotsdk.DescribeAuditTaskInput{TaskId: started.TaskId})
	require.NoError(t, err)
	assert.Equal(t, types.AuditTaskStatusCanceled, tDesc.TaskStatus)
}

// testMitigationActionsRealClient covers DeleteMitigationAction,
// DescribeMitigationAction, ListMitigationActions, UpdateMitigationAction,
// PutVerificationStateOnViolation.
func testMitigationActionsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateMitigationAction(t.Context(), &iotsdk.CreateMitigationActionInput{
		ActionName: aws.String("slice4-mit"),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/mit"),
		ActionParams: &types.MitigationActionParams{
			PublishFindingToSnsParams: &types.PublishFindingToSnsParams{
				TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeMitigationAction(t.Context(), &iotsdk.DescribeMitigationActionInput{
		ActionName: aws.String("slice4-mit"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.ActionParams.PublishFindingToSnsParams)

	list, err := client.ListMitigationActions(t.Context(), &iotsdk.ListMitigationActionsInput{})
	require.NoError(t, err)
	found := false

	for _, m := range list.ActionIdentifiers {
		if aws.ToString(m.ActionName) == "slice4-mit" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateMitigationAction(t.Context(), &iotsdk.UpdateMitigationActionInput{
		ActionName: aws.String("slice4-mit"),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/mit2"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeMitigationAction(t.Context(), &iotsdk.DescribeMitigationActionInput{
		ActionName: aws.String("slice4-mit"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/mit2", aws.ToString(desc.RoleArn))

	_, err = client.DeleteMitigationAction(t.Context(), &iotsdk.DeleteMitigationActionInput{
		ActionName: aws.String("slice4-mit"),
	})
	require.NoError(t, err)
}

// testDomainConfigRealClient covers DeleteDomainConfiguration,
// DescribeDomainConfiguration, ListDomainConfigurations,
// UpdateDomainConfiguration.
func testDomainConfigRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateDomainConfiguration(t.Context(), &iotsdk.CreateDomainConfigurationInput{
		DomainConfigurationName: aws.String("slice4-domain"),
		ServiceType:             types.ServiceTypeData,
	})
	require.NoError(t, err)

	desc, err := client.DescribeDomainConfiguration(t.Context(), &iotsdk.DescribeDomainConfigurationInput{
		DomainConfigurationName: aws.String("slice4-domain"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ServiceTypeData, desc.ServiceType)

	list, err := client.ListDomainConfigurations(t.Context(), &iotsdk.ListDomainConfigurationsInput{})
	require.NoError(t, err)
	found := false

	for _, d := range list.DomainConfigurations {
		if aws.ToString(d.DomainConfigurationName) == "slice4-domain" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateDomainConfiguration(t.Context(), &iotsdk.UpdateDomainConfigurationInput{
		DomainConfigurationName:   aws.String("slice4-domain"),
		DomainConfigurationStatus: types.DomainConfigurationStatusDisabled,
	})
	require.NoError(t, err)

	desc, err = client.DescribeDomainConfiguration(t.Context(), &iotsdk.DescribeDomainConfigurationInput{
		DomainConfigurationName: aws.String("slice4-domain"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.DomainConfigurationStatusDisabled, desc.DomainConfigurationStatus)

	_, err = client.DeleteDomainConfiguration(t.Context(), &iotsdk.DeleteDomainConfigurationInput{
		DomainConfigurationName: aws.String("slice4-domain"),
	})
	require.NoError(t, err)
}

// testBillingGroupsRealClient covers DeleteBillingGroup,
// DescribeBillingGroup, UpdateBillingGroup, ListBillingGroups,
// AddThingToBillingGroup, RemoveThingFromBillingGroup,
// ListThingsInBillingGroup.
func testBillingGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateBillingGroup(t.Context(), &iotsdk.CreateBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeBillingGroup(t.Context(), &iotsdk.DescribeBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), desc.Version)

	list, err := client.ListBillingGroups(t.Context(), &iotsdk.ListBillingGroupsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, list.BillingGroups)

	updated, err := client.UpdateBillingGroup(t.Context(), &iotsdk.UpdateBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"),
		BillingGroupProperties: &types.BillingGroupProperties{
			BillingGroupDescription: aws.String("desc"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), updated.Version)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("slice4-bg-thing")})
	require.NoError(t, err)

	_, err = client.AddThingToBillingGroup(t.Context(), &iotsdk.AddThingToBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"), ThingName: aws.String("slice4-bg-thing"),
	})
	require.NoError(t, err)

	things, err := client.ListThingsInBillingGroup(t.Context(), &iotsdk.ListThingsInBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"),
	})
	require.NoError(t, err)
	assert.Contains(t, things.Things, "slice4-bg-thing")

	_, err = client.RemoveThingFromBillingGroup(t.Context(), &iotsdk.RemoveThingFromBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"), ThingName: aws.String("slice4-bg-thing"),
	})
	require.NoError(t, err)

	things, err = client.ListThingsInBillingGroup(t.Context(), &iotsdk.ListThingsInBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"),
	})
	require.NoError(t, err)
	assert.NotContains(t, things.Things, "slice4-bg-thing")

	_, err = client.DeleteBillingGroup(t.Context(), &iotsdk.DeleteBillingGroupInput{
		BillingGroupName: aws.String("slice4-bg"), ExpectedVersion: aws.Int64(2),
	})
	require.NoError(t, err)
}

// testRoleAliasesRealClient covers DeleteRoleAlias, DescribeRoleAlias,
// UpdateRoleAlias.
func testRoleAliasesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateRoleAlias(t.Context(), &iotsdk.CreateRoleAliasInput{
		RoleAlias: aws.String("slice4-alias"),
		RoleArn:   aws.String("arn:aws:iam::000000000000:role/orig"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeRoleAlias(t.Context(), &iotsdk.DescribeRoleAliasInput{
		RoleAlias: aws.String("slice4-alias"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/orig", aws.ToString(desc.RoleAliasDescription.RoleArn))

	_, err = client.UpdateRoleAlias(t.Context(), &iotsdk.UpdateRoleAliasInput{
		RoleAlias: aws.String("slice4-alias"),
		RoleArn:   aws.String("arn:aws:iam::000000000000:role/updated"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeRoleAlias(t.Context(), &iotsdk.DescribeRoleAliasInput{
		RoleAlias: aws.String("slice4-alias"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/updated", aws.ToString(desc.RoleAliasDescription.RoleArn))

	_, err = client.DeleteRoleAlias(t.Context(), &iotsdk.DeleteRoleAliasInput{RoleAlias: aws.String("slice4-alias")})
	require.NoError(t, err)
}

// testAuthorizersRealClient covers DeleteAuthorizer, DescribeAuthorizer,
// UpdateAuthorizer, ClearDefaultAuthorizer, SetDefaultAuthorizer,
// DescribeDefaultAuthorizer, TestInvokeAuthorizer.
func testAuthorizersRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateAuthorizer(t.Context(), &iotsdk.CreateAuthorizerInput{
		AuthorizerName:        aws.String("slice4-authz"),
		AuthorizerFunctionArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:f"),
		SigningDisabled:       aws.Bool(true),
		Status:                types.AuthorizerStatusActive,
	})
	require.NoError(t, err)

	desc, err := client.DescribeAuthorizer(t.Context(), &iotsdk.DescribeAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AuthorizerStatusActive, desc.AuthorizerDescription.Status)

	_, err = client.UpdateAuthorizer(t.Context(), &iotsdk.UpdateAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
		Status:         types.AuthorizerStatusInactive,
	})
	require.NoError(t, err)

	desc, err = client.DescribeAuthorizer(t.Context(), &iotsdk.DescribeAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AuthorizerStatusInactive, desc.AuthorizerDescription.Status)

	_, err = client.UpdateAuthorizer(t.Context(), &iotsdk.UpdateAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
		Status:         types.AuthorizerStatusActive,
	})
	require.NoError(t, err)

	_, err = client.SetDefaultAuthorizer(t.Context(), &iotsdk.SetDefaultAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
	})
	require.NoError(t, err)

	def, err := client.DescribeDefaultAuthorizer(t.Context(), &iotsdk.DescribeDefaultAuthorizerInput{})
	require.NoError(t, err)
	assert.Equal(t, "slice4-authz", aws.ToString(def.AuthorizerDescription.AuthorizerName))

	invoked, err := client.TestInvokeAuthorizer(t.Context(), &iotsdk.TestInvokeAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"), Token: aws.String("tok"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(invoked.IsAuthenticated))
	assert.NotEmpty(t, aws.ToString(invoked.PrincipalId))

	_, err = client.ClearDefaultAuthorizer(t.Context(), &iotsdk.ClearDefaultAuthorizerInput{})
	require.NoError(t, err)

	_, err = client.DescribeDefaultAuthorizer(t.Context(), &iotsdk.DescribeDefaultAuthorizerInput{})
	require.Error(t, err)

	_, err = client.DeleteAuthorizer(t.Context(), &iotsdk.DeleteAuthorizerInput{
		AuthorizerName: aws.String("slice4-authz"),
	})
	require.NoError(t, err)
}

// testCustomMetricsDimensionsRealClient covers DeleteCustomMetric,
// DescribeCustomMetric, ListCustomMetrics, UpdateCustomMetric,
// CreateDimension, DeleteDimension, DescribeDimension, ListDimensions,
// UpdateDimension.
func testCustomMetricsDimensionsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateCustomMetric(t.Context(), &iotsdk.CreateCustomMetricInput{
		MetricName: aws.String("slice4-metric"),
		MetricType: types.CustomMetricTypeNumber,
	})
	require.NoError(t, err)

	desc, err := client.DescribeCustomMetric(t.Context(), &iotsdk.DescribeCustomMetricInput{
		MetricName: aws.String("slice4-metric"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.CustomMetricTypeNumber, desc.MetricType)

	list, err := client.ListCustomMetrics(t.Context(), &iotsdk.ListCustomMetricsInput{})
	require.NoError(t, err)
	assert.Contains(t, list.MetricNames, "slice4-metric")

	_, err = client.UpdateCustomMetric(t.Context(), &iotsdk.UpdateCustomMetricInput{
		MetricName:  aws.String("slice4-metric"),
		DisplayName: aws.String("display"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeCustomMetric(t.Context(), &iotsdk.DescribeCustomMetricInput{
		MetricName: aws.String("slice4-metric"),
	})
	require.NoError(t, err)
	assert.Equal(t, "display", aws.ToString(desc.DisplayName))

	_, err = client.DeleteCustomMetric(t.Context(), &iotsdk.DeleteCustomMetricInput{
		MetricName: aws.String("slice4-metric"),
	})
	require.NoError(t, err)

	_, err = client.CreateDimension(t.Context(), &iotsdk.CreateDimensionInput{
		Name:         aws.String("slice4-dim"),
		Type:         types.DimensionTypeTopicFilter,
		StringValues: []string{"a/b"},
	})
	require.NoError(t, err)

	dDesc, err := client.DescribeDimension(t.Context(), &iotsdk.DescribeDimensionInput{Name: aws.String("slice4-dim")})
	require.NoError(t, err)
	assert.Equal(t, []string{"a/b"}, dDesc.StringValues)

	dList, err := client.ListDimensions(t.Context(), &iotsdk.ListDimensionsInput{})
	require.NoError(t, err)
	assert.Contains(t, dList.DimensionNames, "slice4-dim")

	_, err = client.UpdateDimension(t.Context(), &iotsdk.UpdateDimensionInput{
		Name: aws.String("slice4-dim"), StringValues: []string{"a/b", "c/d"},
	})
	require.NoError(t, err)

	dDesc, err = client.DescribeDimension(t.Context(), &iotsdk.DescribeDimensionInput{Name: aws.String("slice4-dim")})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a/b", "c/d"}, dDesc.StringValues)

	_, err = client.DeleteDimension(t.Context(), &iotsdk.DeleteDimensionInput{Name: aws.String("slice4-dim")})
	require.NoError(t, err)
}

// testFleetMetricsRealClient covers DeleteFleetMetric, DescribeFleetMetric,
// ListFleetMetrics, UpdateFleetMetric.
func testFleetMetricsRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreateFleetMetric(t.Context(), &iotsdk.CreateFleetMetricInput{
		MetricName:       aws.String("slice4-fm"),
		QueryString:      aws.String("*"),
		AggregationField: aws.String("thingTypeName.keyword"),
		AggregationType: &types.AggregationType{
			Name: types.AggregationTypeNameStatistics,
		},
		Period: aws.Int32(300),
	})
	require.NoError(t, err)

	desc, err := client.DescribeFleetMetric(t.Context(), &iotsdk.DescribeFleetMetricInput{
		MetricName: aws.String("slice4-fm"),
	})
	require.NoError(t, err)
	assert.Equal(t, "*", aws.ToString(desc.QueryString))

	list, err := client.ListFleetMetrics(t.Context(), &iotsdk.ListFleetMetricsInput{})
	require.NoError(t, err)
	found := false

	for _, m := range list.FleetMetrics {
		if aws.ToString(m.MetricName) == "slice4-fm" {
			found = true
		}
	}

	assert.True(t, found)

	_, err = client.UpdateFleetMetric(t.Context(), &iotsdk.UpdateFleetMetricInput{
		MetricName:      aws.String("slice4-fm"),
		QueryString:     aws.String("thingName:*"),
		AggregationType: &types.AggregationType{Name: types.AggregationTypeNameStatistics},
		Period:          aws.Int32(300),
		IndexName:       aws.String("AWS_Things"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeFleetMetric(t.Context(), &iotsdk.DescribeFleetMetricInput{
		MetricName: aws.String("slice4-fm"),
	})
	require.NoError(t, err)
	assert.Equal(t, "thingName:*", aws.ToString(desc.QueryString))

	_, err = client.DeleteFleetMetric(t.Context(), &iotsdk.DeleteFleetMetricInput{
		MetricName: aws.String("slice4-fm"),
	})
	require.NoError(t, err)
}

// testPackagesRealClient covers ListPackages, ListPackageVersions,
// GetPackageConfiguration, UpdatePackageConfiguration.
func testPackagesRealClient(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)

	_, err := client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{PackageName: aws.String("slice4-pkg")})
	require.NoError(t, err)

	_, err = client.CreatePackageVersion(t.Context(), &iotsdk.CreatePackageVersionInput{
		PackageName: aws.String("slice4-pkg"), VersionName: aws.String("v1"),
	})
	require.NoError(t, err)

	list, err := client.ListPackages(t.Context(), &iotsdk.ListPackagesInput{})
	require.NoError(t, err)
	found := false

	for _, p := range list.PackageSummaries {
		if aws.ToString(p.PackageName) == "slice4-pkg" {
			found = true
		}
	}

	assert.True(t, found)

	versions, err := client.ListPackageVersions(t.Context(), &iotsdk.ListPackageVersionsInput{
		PackageName: aws.String("slice4-pkg"),
	})
	require.NoError(t, err)
	require.Len(t, versions.PackageVersionSummaries, 1)
	assert.Equal(t, "v1", aws.ToString(versions.PackageVersionSummaries[0].VersionName))

	cfg, err := client.GetPackageConfiguration(t.Context(), &iotsdk.GetPackageConfigurationInput{})
	require.NoError(t, err)
	_ = cfg

	_, err = client.UpdatePackageConfiguration(t.Context(), &iotsdk.UpdatePackageConfigurationInput{
		VersionUpdateByJobsConfig: &types.VersionUpdateByJobsConfig{Enabled: aws.Bool(true)},
	})
	require.NoError(t, err)

	got, err := client.GetPackageConfiguration(t.Context(), &iotsdk.GetPackageConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, got.VersionUpdateByJobsConfig)
	assert.True(t, aws.ToBool(got.VersionUpdateByJobsConfig.Enabled))
}
