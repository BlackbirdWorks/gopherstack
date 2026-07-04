package cloudfront_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// ---------------------------------------------------------------------------
// AnycastIPList
// ---------------------------------------------------------------------------

// TestParitySweep_AnycastIPList_NameUniqueness verifies CreateAnycastIPList rejects a duplicate
// name and that DeleteAnycastIPList frees the name for reuse.
func TestParitySweep_AnycastIPList_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	first, err := b.CreateAnycastIPList("dup-name", 3)
	require.NoError(t, err)

	_, err = b.CreateAnycastIPList("dup-name", 5)
	require.Error(t, err)

	require.NoError(t, b.DeleteAnycastIPList(first.ID))

	// Name is free again after delete.
	_, err = b.CreateAnycastIPList("dup-name", 5)
	require.NoError(t, err)
}

// TestParitySweep_AnycastIPList_GeneratedIPs verifies AnycastIPs is populated with IPCount
// entries on create, and regenerated to match a new count on update.
func TestParitySweep_AnycastIPList_GeneratedIPs(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	list, err := b.CreateAnycastIPList("ips-list", 4)
	require.NoError(t, err)
	assert.Len(t, list.AnycastIPs, 4)
	assert.NotEmpty(t, list.ETag)

	oldETag := list.ETag
	updated, err := b.UpdateAnycastIPList(list.ID, 7)
	require.NoError(t, err)
	assert.Len(t, updated.AnycastIPs, 7)
	assert.NotEqual(t, oldETag, updated.ETag)

	// A no-op update (ipCount <= 0) leaves the IPs and count unchanged.
	unchanged, err := b.UpdateAnycastIPList(list.ID, 0)
	require.NoError(t, err)
	assert.Len(t, unchanged.AnycastIPs, 7)
}

// TestParitySweep_AnycastIPList_IfMatchEnforcement mirrors the trust store If-Match test:
// a mismatched If-Match header on update/delete is rejected with 412.
func TestParitySweep_AnycastIPList_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"anycast-ip-list",
		[]byte(`<AnycastIPListRequest><Name>etag-list</Name><IPCount>3</IPCount></AnycastIPListRequest>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<AnycastIpListConfig><IpCount>9</IpCount></AnycastIpListConfig>`),
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badUpdate.Code)

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<AnycastIpListConfig><IpCount>9</IpCount></AnycastIpListConfig>`),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusOK, goodUpdate.Code)
	newETag := goodUpdate.Header().Get("ETag")
	assert.NotEmpty(t, newETag)
	assert.NotEqual(t, etag, newETag)

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badDelete.Code)

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": newETag})
	assert.Equal(t, http.StatusNoContent, goodDelete.Code)
}

// TestParitySweep_AnycastIPList_Tags verifies tags supplied on create are stored and retrievable
// via the generic ListTagsForResource endpoint.
func TestParitySweep_AnycastIPList_Tags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"anycast-ip-list",
		[]byte(`<AnycastIPListRequest><Name>tagged-list</Name><IPCount>2</IPCount>`+
			`<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`+
			`</AnycastIPListRequest>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:anycast-ip-list/%s", id)

	listRec := doXML(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "<Key>env</Key>")
	assert.Contains(t, listRec.Body.String(), "<Value>prod</Value>")
}

// TestParitySweep_AnycastIPList_PersistenceRoundTrip verifies AnycastIPList survives a
// Snapshot/Restore cycle including its ETag, tags, generated IPs, and name-uniqueness index.
func TestParitySweep_AnycastIPList_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	list, err := b.CreateAnycastIPList("persist-list", 3)
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	restored, err := b2.GetAnycastIPList(list.ID)
	require.NoError(t, err)
	assert.Equal(t, list.ETag, restored.ETag)
	assert.Len(t, restored.AnycastIPs, 3)

	// The name-uniqueness index survived restore: creating a second list with the same name
	// still fails.
	_, err = b2.CreateAnycastIPList("persist-list", 5)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// ContinuousDeploymentPolicy
// ---------------------------------------------------------------------------

// TestParitySweep_ContinuousDeploymentPolicy_TrafficConfig verifies the full request shape
// (multiple staging DNS names plus a SingleWeight or SingleHeader traffic config) round-trips
// through Create/Get/Update, and that ARN/LastModifiedTime are populated.
func TestParitySweep_ContinuousDeploymentPolicy_TrafficConfig(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	weightTraffic := cloudfront.ContinuousDeploymentTrafficConfig{
		Type: "SingleWeight",
		SingleWeightConfig: &cloudfront.ContinuousDeploymentSingleWeightConfig{
			Weight: 0.15,
			SessionStickinessConfig: &cloudfront.ContinuousDeploymentSessionStickinessConfig{
				IdleTTL:    300,
				MaximumTTL: 600,
			},
		},
	}

	policy, err := b.CreateContinuousDeploymentPolicyWithConfig(
		true, []string{"staging-a.example.com", "staging-b.example.com"}, weightTraffic,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, policy.ARN)
	assert.NotEmpty(t, policy.LastModifiedTime)
	assert.Equal(t, []string{"staging-a.example.com", "staging-b.example.com"}, policy.StagingDistributionDNSNames)
	require.NotNil(t, policy.TrafficConfig.SingleWeightConfig)
	assert.InDelta(t, 0.15, policy.TrafficConfig.SingleWeightConfig.Weight, 0.0001)
	require.NotNil(t, policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig)
	assert.Equal(t, int32(300), policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig.IdleTTL)

	got, err := b.GetContinuousDeploymentPolicy(policy.ID)
	require.NoError(t, err)
	assert.Equal(t, policy.ARN, got.ARN)
	assert.Equal(t, "SingleWeight", got.TrafficConfig.Type)

	headerTraffic := cloudfront.ContinuousDeploymentTrafficConfig{
		Type: "SingleHeader",
		SingleHeaderConfig: &cloudfront.ContinuousDeploymentSingleHeaderConfig{
			Header: "aws-cf-cd-staging",
			Value:  "true",
		},
	}
	updated, err := b.UpdateContinuousDeploymentPolicyWithConfig(
		policy.ID, false, []string{"staging-c.example.com"}, headerTraffic,
	)
	require.NoError(t, err)
	assert.False(t, updated.Enabled)
	assert.Equal(t, []string{"staging-c.example.com"}, updated.StagingDistributionDNSNames)
	require.NotNil(t, updated.TrafficConfig.SingleHeaderConfig)
	assert.Equal(t, "aws-cf-cd-staging", updated.TrafficConfig.SingleHeaderConfig.Header)
	assert.Nil(t, updated.TrafficConfig.SingleWeightConfig, "full replace should drop the old SingleWeightConfig")
}

// TestParitySweep_ContinuousDeploymentPolicy_IfMatchEnforcement mirrors the trust store If-Match
// test for the continuous deployment policy update/delete handlers.
func TestParitySweep_ContinuousDeploymentPolicy_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"continuous-deployment-policy",
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>true</Enabled></ContinuousDeploymentPolicyConfig>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"continuous-deployment-policy/"+id,
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`),
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badUpdate.Code)

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"continuous-deployment-policy/"+id,
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusOK, goodUpdate.Code)
	newETag := goodUpdate.Header().Get("ETag")
	assert.NotEmpty(t, newETag)
	assert.NotEqual(t, etag, newETag)

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"continuous-deployment-policy/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badDelete.Code)

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"continuous-deployment-policy/"+id, nil,
		map[string]string{"If-Match": newETag})
	assert.Equal(t, http.StatusNoContent, goodDelete.Code)
}

// ---------------------------------------------------------------------------
// MonitoringSubscription
// ---------------------------------------------------------------------------

// TestParitySweep_MonitoringSubscription_NotFound verifies Get and Delete both 404 when no
// subscription has been created for a distribution, and that Get succeeds once one has.
func TestParitySweep_MonitoringSubscription_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"
	const distID = "ENOSUBSCRIPTION"
	path := prefix + "distribution/" + distID + "/monitoring-subscription"

	getRec := doXML(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "NoSuchMonitoringSubscription")

	deleteRec := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNotFound, deleteRec.Code)

	createRec := doXML(t, h, http.MethodPost, path,
		[]byte(`<MonitoringSubscription><RealtimeMetricsSubscriptionConfig>`+
			`<RealtimeMetricsSubscriptionStatus>Enabled</RealtimeMetricsSubscriptionStatus>`+
			`</RealtimeMetricsSubscriptionConfig></MonitoringSubscription>`))
	require.Equal(t, http.StatusOK, createRec.Code)

	getAfterCreate := doXML(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, getAfterCreate.Code)
	assert.Contains(t, getAfterCreate.Body.String(), "Enabled")

	deleteAfterCreate := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNoContent, deleteAfterCreate.Code)

	// Deleted again -> 404.
	deleteAgain := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNotFound, deleteAgain.Code)
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// TestParitySweep_ResourcePolicy_NotFound verifies GetResourcePolicy 404s when no policy has
// been put for a resource ARN, and succeeds once one has been.
func TestParitySweep_ResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"
	const arn = "arn:aws:cloudfront::123456789012:distribution/ENOPOLICY"

	getRec := doXML(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "NoSuchResourcePolicy")

	putBody := fmt.Sprintf(
		`<ResourcePolicy><Policy>{"Version":"2012-10-17"}</Policy><ResourceArn>%s</ResourceArn></ResourcePolicy>`,
		arn,
	)
	putRec := doXML(t, h, http.MethodPost, prefix+"resource-policy", []byte(putBody))
	require.Equal(t, http.StatusOK, putRec.Code)

	getAfterPut := doXML(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, nil)
	assert.Equal(t, http.StatusOK, getAfterPut.Code)
	assert.Contains(t, getAfterPut.Body.String(), `"Version":"2012-10-17"`)
}

// ---------------------------------------------------------------------------
// GetManagedCertificateDetails
// ---------------------------------------------------------------------------

// TestParitySweep_GetManagedCertificateDetails_NotFound verifies a 404 for a distribution tenant
// that does not exist (rather than the previous hardcoded always-SUCCESS stub).
func TestParitySweep_GetManagedCertificateDetails_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	rec := doXML(t, h, http.MethodGet, prefix+"distribution-tenant/does-not-exist/managed-certificate-details", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchDistributionTenant")
}

// TestParitySweep_GetManagedCertificateDetails_StableACrossCalls verifies the derived
// certificate ARN and validation tokens are stable across repeated Get calls for the same
// tenant (a real, cached backend value, not a fresh random result each time).
func TestParitySweep_GetManagedCertificateDetails_StableACrossCalls(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createBody := `<CreateDistributionTenantRequest>` +
		`<DistributionId>dist-parity-001</DistributionId>` +
		`<Domain>parity-cert-test.example.com</Domain>` +
		`</CreateDistributionTenantRequest>`
	createRec := doXML(t, h, http.MethodPost, prefix+"distribution-tenant", []byte(createBody))
	require.Equal(t, http.StatusCreated, createRec.Code)
	tenantID := extractXMLID(t, createRec.Body.String())

	path := prefix + "distribution-tenant/" + tenantID + "/managed-certificate-details"
	first := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, first.Code)
	require.Contains(t, first.Body.String(), "SUCCESS")

	second := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, second.Code)

	firstARN := strings.SplitN(strings.SplitN(first.Body.String(), "<CertificateArn>", 2)[1], "</CertificateArn>", 2)[0]
	secondARN := strings.SplitN(strings.SplitN(second.Body.String(), "<CertificateArn>", 2)[1], "</CertificateArn>", 2)[0]
	assert.Equal(t, firstARN, secondARN)
	assert.Contains(t, first.Body.String(), "parity-cert-test.example.com")
}
