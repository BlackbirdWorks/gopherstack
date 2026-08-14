package lightsail_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Lightsail
// operation, extracted from lightsail@v1.58.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Lightsail_20161128.<Op>")
// and always POSTs to "/" -- Lightsail is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// (via h.dispatch's h.opTable() map lookup) both derive the action the same
// way (TrimPrefix on "Lightsail_20161128."), so the class of bug this table
// catches is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case -- Lightsail is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 161 real Lightsail ops (lightsail@v1.58.4) --
// confirmed by TWO genuinely independent diffs. GetSupportedOperations() is
// a hand-written literal list in handler.go (NOT built by ranging over
// opTable()). opTable() is separately built by merging 16 per-family
// *Ops() builders (referenceDataOps, instanceOps, instanceAccessOps,
// instanceExtrasOps, keyPairStaticIPOps, diskOps, exportCfnOps,
// loadBalancerOps, databaseOps, containerOps, bucketOps,
// distributionCertOps, domainOps, alarmContactOps, taggingVpcMiscOps,
// operationOps). Extracting every key from all 16 builder functions
// directly (161 total, no duplicates across groups) and diffing both that
// set and GetSupportedOperations' literal against the SDK's target list:
// zero mismatches in either direction, no dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Lightsail_20161128.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AllocateStaticIp", "Lightsail_20161128.AllocateStaticIp"},
		{"AttachCertificateToDistribution", "Lightsail_20161128.AttachCertificateToDistribution"},
		{"AttachDisk", "Lightsail_20161128.AttachDisk"},
		{"AttachInstancesToLoadBalancer", "Lightsail_20161128.AttachInstancesToLoadBalancer"},
		{"AttachLoadBalancerTlsCertificate", "Lightsail_20161128.AttachLoadBalancerTlsCertificate"},
		{"AttachStaticIp", "Lightsail_20161128.AttachStaticIp"},
		{"CloseInstancePublicPorts", "Lightsail_20161128.CloseInstancePublicPorts"},
		{"CopySnapshot", "Lightsail_20161128.CopySnapshot"},
		{"CreateBucket", "Lightsail_20161128.CreateBucket"},
		{"CreateBucketAccessKey", "Lightsail_20161128.CreateBucketAccessKey"},
		{"CreateCertificate", "Lightsail_20161128.CreateCertificate"},
		{"CreateCloudFormationStack", "Lightsail_20161128.CreateCloudFormationStack"},
		{"CreateContactMethod", "Lightsail_20161128.CreateContactMethod"},
		{"CreateContainerService", "Lightsail_20161128.CreateContainerService"},
		{"CreateContainerServiceDeployment", "Lightsail_20161128.CreateContainerServiceDeployment"},
		{"CreateContainerServiceRegistryLogin", "Lightsail_20161128.CreateContainerServiceRegistryLogin"},
		{"CreateDisk", "Lightsail_20161128.CreateDisk"},
		{"CreateDiskFromSnapshot", "Lightsail_20161128.CreateDiskFromSnapshot"},
		{"CreateDiskSnapshot", "Lightsail_20161128.CreateDiskSnapshot"},
		{"CreateDistribution", "Lightsail_20161128.CreateDistribution"},
		{"CreateDomain", "Lightsail_20161128.CreateDomain"},
		{"CreateDomainEntry", "Lightsail_20161128.CreateDomainEntry"},
		{"CreateGUISessionAccessDetails", "Lightsail_20161128.CreateGUISessionAccessDetails"},
		{"CreateInstances", "Lightsail_20161128.CreateInstances"},
		{"CreateInstancesFromSnapshot", "Lightsail_20161128.CreateInstancesFromSnapshot"},
		{"CreateInstanceSnapshot", "Lightsail_20161128.CreateInstanceSnapshot"},
		{"CreateKeyPair", "Lightsail_20161128.CreateKeyPair"},
		{"CreateLoadBalancer", "Lightsail_20161128.CreateLoadBalancer"},
		{"CreateLoadBalancerTlsCertificate", "Lightsail_20161128.CreateLoadBalancerTlsCertificate"},
		{"CreateRelationalDatabase", "Lightsail_20161128.CreateRelationalDatabase"},
		{"CreateRelationalDatabaseFromSnapshot", "Lightsail_20161128.CreateRelationalDatabaseFromSnapshot"},
		{"CreateRelationalDatabaseSnapshot", "Lightsail_20161128.CreateRelationalDatabaseSnapshot"},
		{"DeleteAlarm", "Lightsail_20161128.DeleteAlarm"},
		{"DeleteAutoSnapshot", "Lightsail_20161128.DeleteAutoSnapshot"},
		{"DeleteBucket", "Lightsail_20161128.DeleteBucket"},
		{"DeleteBucketAccessKey", "Lightsail_20161128.DeleteBucketAccessKey"},
		{"DeleteCertificate", "Lightsail_20161128.DeleteCertificate"},
		{"DeleteContactMethod", "Lightsail_20161128.DeleteContactMethod"},
		{"DeleteContainerImage", "Lightsail_20161128.DeleteContainerImage"},
		{"DeleteContainerService", "Lightsail_20161128.DeleteContainerService"},
		{"DeleteDisk", "Lightsail_20161128.DeleteDisk"},
		{"DeleteDiskSnapshot", "Lightsail_20161128.DeleteDiskSnapshot"},
		{"DeleteDistribution", "Lightsail_20161128.DeleteDistribution"},
		{"DeleteDomain", "Lightsail_20161128.DeleteDomain"},
		{"DeleteDomainEntry", "Lightsail_20161128.DeleteDomainEntry"},
		{"DeleteInstance", "Lightsail_20161128.DeleteInstance"},
		{"DeleteInstanceSnapshot", "Lightsail_20161128.DeleteInstanceSnapshot"},
		{"DeleteKeyPair", "Lightsail_20161128.DeleteKeyPair"},
		{"DeleteKnownHostKeys", "Lightsail_20161128.DeleteKnownHostKeys"},
		{"DeleteLoadBalancer", "Lightsail_20161128.DeleteLoadBalancer"},
		{"DeleteLoadBalancerTlsCertificate", "Lightsail_20161128.DeleteLoadBalancerTlsCertificate"},
		{"DeleteRelationalDatabase", "Lightsail_20161128.DeleteRelationalDatabase"},
		{"DeleteRelationalDatabaseSnapshot", "Lightsail_20161128.DeleteRelationalDatabaseSnapshot"},
		{"DetachCertificateFromDistribution", "Lightsail_20161128.DetachCertificateFromDistribution"},
		{"DetachDisk", "Lightsail_20161128.DetachDisk"},
		{"DetachInstancesFromLoadBalancer", "Lightsail_20161128.DetachInstancesFromLoadBalancer"},
		{"DetachStaticIp", "Lightsail_20161128.DetachStaticIp"},
		{"DisableAddOn", "Lightsail_20161128.DisableAddOn"},
		{"DownloadDefaultKeyPair", "Lightsail_20161128.DownloadDefaultKeyPair"},
		{"EnableAddOn", "Lightsail_20161128.EnableAddOn"},
		{"ExportSnapshot", "Lightsail_20161128.ExportSnapshot"},
		{"GetActiveNames", "Lightsail_20161128.GetActiveNames"},
		{"GetAlarms", "Lightsail_20161128.GetAlarms"},
		{"GetAutoSnapshots", "Lightsail_20161128.GetAutoSnapshots"},
		{"GetBlueprints", "Lightsail_20161128.GetBlueprints"},
		{"GetBucketAccessKeys", "Lightsail_20161128.GetBucketAccessKeys"},
		{"GetBucketBundles", "Lightsail_20161128.GetBucketBundles"},
		{"GetBucketMetricData", "Lightsail_20161128.GetBucketMetricData"},
		{"GetBuckets", "Lightsail_20161128.GetBuckets"},
		{"GetBundles", "Lightsail_20161128.GetBundles"},
		{"GetCertificates", "Lightsail_20161128.GetCertificates"},
		{"GetCloudFormationStackRecords", "Lightsail_20161128.GetCloudFormationStackRecords"},
		{"GetContactMethods", "Lightsail_20161128.GetContactMethods"},
		{"GetContainerAPIMetadata", "Lightsail_20161128.GetContainerAPIMetadata"},
		{"GetContainerImages", "Lightsail_20161128.GetContainerImages"},
		{"GetContainerLog", "Lightsail_20161128.GetContainerLog"},
		{"GetContainerServiceDeployments", "Lightsail_20161128.GetContainerServiceDeployments"},
		{"GetContainerServiceMetricData", "Lightsail_20161128.GetContainerServiceMetricData"},
		{"GetContainerServicePowers", "Lightsail_20161128.GetContainerServicePowers"},
		{"GetContainerServices", "Lightsail_20161128.GetContainerServices"},
		{"GetCostEstimate", "Lightsail_20161128.GetCostEstimate"},
		{"GetDisk", "Lightsail_20161128.GetDisk"},
		{"GetDisks", "Lightsail_20161128.GetDisks"},
		{"GetDiskSnapshot", "Lightsail_20161128.GetDiskSnapshot"},
		{"GetDiskSnapshots", "Lightsail_20161128.GetDiskSnapshots"},
		{"GetDistributionBundles", "Lightsail_20161128.GetDistributionBundles"},
		{"GetDistributionLatestCacheReset", "Lightsail_20161128.GetDistributionLatestCacheReset"},
		{"GetDistributionMetricData", "Lightsail_20161128.GetDistributionMetricData"},
		{"GetDistributions", "Lightsail_20161128.GetDistributions"},
		{"GetDomain", "Lightsail_20161128.GetDomain"},
		{"GetDomains", "Lightsail_20161128.GetDomains"},
		{"GetExportSnapshotRecords", "Lightsail_20161128.GetExportSnapshotRecords"},
		{"GetInstance", "Lightsail_20161128.GetInstance"},
		{"GetInstanceAccessDetails", "Lightsail_20161128.GetInstanceAccessDetails"},
		{"GetInstanceMetricData", "Lightsail_20161128.GetInstanceMetricData"},
		{"GetInstancePortStates", "Lightsail_20161128.GetInstancePortStates"},
		{"GetInstances", "Lightsail_20161128.GetInstances"},
		{"GetInstanceSnapshot", "Lightsail_20161128.GetInstanceSnapshot"},
		{"GetInstanceSnapshots", "Lightsail_20161128.GetInstanceSnapshots"},
		{"GetInstanceState", "Lightsail_20161128.GetInstanceState"},
		{"GetKeyPair", "Lightsail_20161128.GetKeyPair"},
		{"GetKeyPairs", "Lightsail_20161128.GetKeyPairs"},
		{"GetLoadBalancer", "Lightsail_20161128.GetLoadBalancer"},
		{"GetLoadBalancerMetricData", "Lightsail_20161128.GetLoadBalancerMetricData"},
		{"GetLoadBalancers", "Lightsail_20161128.GetLoadBalancers"},
		{"GetLoadBalancerTlsCertificates", "Lightsail_20161128.GetLoadBalancerTlsCertificates"},
		{"GetLoadBalancerTlsPolicies", "Lightsail_20161128.GetLoadBalancerTlsPolicies"},
		{"GetOperation", "Lightsail_20161128.GetOperation"},
		{"GetOperations", "Lightsail_20161128.GetOperations"},
		{"GetOperationsForResource", "Lightsail_20161128.GetOperationsForResource"},
		{"GetRegions", "Lightsail_20161128.GetRegions"},
		{"GetRelationalDatabase", "Lightsail_20161128.GetRelationalDatabase"},
		{"GetRelationalDatabaseBlueprints", "Lightsail_20161128.GetRelationalDatabaseBlueprints"},
		{"GetRelationalDatabaseBundles", "Lightsail_20161128.GetRelationalDatabaseBundles"},
		{"GetRelationalDatabaseEvents", "Lightsail_20161128.GetRelationalDatabaseEvents"},
		{"GetRelationalDatabaseLogEvents", "Lightsail_20161128.GetRelationalDatabaseLogEvents"},
		{"GetRelationalDatabaseLogStreams", "Lightsail_20161128.GetRelationalDatabaseLogStreams"},
		{"GetRelationalDatabaseMasterUserPassword", "Lightsail_20161128.GetRelationalDatabaseMasterUserPassword"},
		{"GetRelationalDatabaseMetricData", "Lightsail_20161128.GetRelationalDatabaseMetricData"},
		{"GetRelationalDatabaseParameters", "Lightsail_20161128.GetRelationalDatabaseParameters"},
		{"GetRelationalDatabases", "Lightsail_20161128.GetRelationalDatabases"},
		{"GetRelationalDatabaseSnapshot", "Lightsail_20161128.GetRelationalDatabaseSnapshot"},
		{"GetRelationalDatabaseSnapshots", "Lightsail_20161128.GetRelationalDatabaseSnapshots"},
		{"GetSetupHistory", "Lightsail_20161128.GetSetupHistory"},
		{"GetStaticIp", "Lightsail_20161128.GetStaticIp"},
		{"GetStaticIps", "Lightsail_20161128.GetStaticIps"},
		{"ImportKeyPair", "Lightsail_20161128.ImportKeyPair"},
		{"IsVpcPeered", "Lightsail_20161128.IsVpcPeered"},
		{"OpenInstancePublicPorts", "Lightsail_20161128.OpenInstancePublicPorts"},
		{"PeerVpc", "Lightsail_20161128.PeerVpc"},
		{"PutAlarm", "Lightsail_20161128.PutAlarm"},
		{"PutInstancePublicPorts", "Lightsail_20161128.PutInstancePublicPorts"},
		{"RebootInstance", "Lightsail_20161128.RebootInstance"},
		{"RebootRelationalDatabase", "Lightsail_20161128.RebootRelationalDatabase"},
		{"RegisterContainerImage", "Lightsail_20161128.RegisterContainerImage"},
		{"ReleaseStaticIp", "Lightsail_20161128.ReleaseStaticIp"},
		{"ResetDistributionCache", "Lightsail_20161128.ResetDistributionCache"},
		{"SendContactMethodVerification", "Lightsail_20161128.SendContactMethodVerification"},
		{"SetIpAddressType", "Lightsail_20161128.SetIpAddressType"},
		{"SetResourceAccessForBucket", "Lightsail_20161128.SetResourceAccessForBucket"},
		{"SetupInstanceHttps", "Lightsail_20161128.SetupInstanceHttps"},
		{"StartGUISession", "Lightsail_20161128.StartGUISession"},
		{"StartInstance", "Lightsail_20161128.StartInstance"},
		{"StartRelationalDatabase", "Lightsail_20161128.StartRelationalDatabase"},
		{"StopGUISession", "Lightsail_20161128.StopGUISession"},
		{"StopInstance", "Lightsail_20161128.StopInstance"},
		{"StopRelationalDatabase", "Lightsail_20161128.StopRelationalDatabase"},
		{"TagResource", "Lightsail_20161128.TagResource"},
		{"TestAlarm", "Lightsail_20161128.TestAlarm"},
		{"UnpeerVpc", "Lightsail_20161128.UnpeerVpc"},
		{"UntagResource", "Lightsail_20161128.UntagResource"},
		{"UpdateBucket", "Lightsail_20161128.UpdateBucket"},
		{"UpdateBucketBundle", "Lightsail_20161128.UpdateBucketBundle"},
		{"UpdateContainerService", "Lightsail_20161128.UpdateContainerService"},
		{"UpdateDistribution", "Lightsail_20161128.UpdateDistribution"},
		{"UpdateDistributionBundle", "Lightsail_20161128.UpdateDistributionBundle"},
		{"UpdateDomainEntry", "Lightsail_20161128.UpdateDomainEntry"},
		{"UpdateInstanceMetadataOptions", "Lightsail_20161128.UpdateInstanceMetadataOptions"},
		{"UpdateLoadBalancerAttribute", "Lightsail_20161128.UpdateLoadBalancerAttribute"},
		{"UpdateRelationalDatabase", "Lightsail_20161128.UpdateRelationalDatabase"},
		{"UpdateRelationalDatabaseParameters", "Lightsail_20161128.UpdateRelationalDatabaseParameters"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Lightsail operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to h.dispatch's single unmatched-route return
// (fmt.Errorf("%w: %s", errUnknownOperation, action), handler.go's
// dispatch() single production call site).
//
// This asserts on MESSAGE TEXT ("unknown Lightsail operation"), not wire
// type. classifyLightsailError has no case for errUnknownOperation at all --
// it falls to the same `default:` branch ("InvalidInputException") that
// validationError's errInvalidInput sentinel also falls to (grepped
// errors.go: neither is checked by name in classifyLightsailError's
// switch), so asserting on __type would be structurally unsafe here,
// exactly the workmail/transfer/datasync/workspaces/codebuild/personalize
// pattern named in the task. errUnknownOperation's message ("unknown
// Lightsail operation: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path, so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := lightsail.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			h := lightsail.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown Lightsail operation",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
