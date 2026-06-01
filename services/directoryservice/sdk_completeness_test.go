package directoryservice_test

import (
	"testing"

	dssdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// directoryservice client is either listed in GetSupportedOperations() or
// explicitly acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	h := directoryservice.NewHandler(backend)

	// Operations not yet implemented — AD Connector, trust management, RADIUS,
	// LDAPS, client auth, certificates, log subscriptions, schema extensions,
	// IP routes, event topics, shared directories, domain controllers, regions,
	// computer creation, directory data access, settings, hybrid AD, CA enrollment,
	// AD assessments, and update operations.
	notImplemented := []string{
		"AcceptSharedDirectory",
		"AddIpRoutes",
		"AddRegion",
		"CancelSchemaExtension",
		"ConnectDirectory",
		"CreateComputer",
		"CreateConditionalForwarder",
		"CreateHybridAD",
		"CreateLogSubscription",
		"CreateTrust",
		"DeleteADAssessment",
		"DeleteConditionalForwarder",
		"DeleteLogSubscription",
		"DeleteTrust",
		"DeregisterCertificate",
		"DeregisterEventTopic",
		"DescribeADAssessment",
		"DescribeCAEnrollmentPolicy",
		"DescribeCertificate",
		"DescribeClientAuthenticationSettings",
		"DescribeConditionalForwarders",
		"DescribeDirectoryDataAccess",
		"DescribeDomainControllers",
		"DescribeEventTopics",
		"DescribeHybridADUpdate",
		"DescribeLDAPSSettings",
		"DescribeRegions",
		"DescribeSettings",
		"DescribeSharedDirectories",
		"DescribeTrusts",
		"DescribeUpdateDirectory",
		"DisableCAEnrollmentPolicy",
		"DisableClientAuthentication",
		"DisableDirectoryDataAccess",
		"DisableLDAPS",
		"DisableRadius",
		"EnableCAEnrollmentPolicy",
		"EnableClientAuthentication",
		"EnableDirectoryDataAccess",
		"EnableLDAPS",
		"EnableRadius",
		"ListADAssessments",
		"ListCertificates",
		"ListIpRoutes",
		"ListLogSubscriptions",
		"ListSchemaExtensions",
		"RegisterCertificate",
		"RegisterEventTopic",
		"RejectSharedDirectory",
		"RemoveIpRoutes",
		"RemoveRegion",
		"ResetUserPassword",
		"ShareDirectory",
		"StartADAssessment",
		"StartSchemaExtension",
		"UnshareDirectory",
		"UpdateConditionalForwarder",
		"UpdateDirectorySetup",
		"UpdateHybridAD",
		"UpdateNumberOfDomainControllers",
		"UpdateRadius",
		"UpdateSettings",
		"UpdateTrust",
		"VerifyTrust",
	}

	sdkcheck.CheckCompleteness(t, &dssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
