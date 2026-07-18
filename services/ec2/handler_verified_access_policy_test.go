package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHandler_VerifiedAccessEndpointPolicy(t *testing.T) {
	t.Parallel()

	h := newHandler()

	inst, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVerifiedAccessInstance"},
		"Description": {"http policy test"},
	})
	require.NoError(t, err)
	instID := extractBatch4XMLValue(inst, "verifiedAccessInstanceId")

	grp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                   {"CreateVerifiedAccessGroup"},
		"VerifiedAccessInstanceId": {instID},
		"Description":              {"http policy group"},
	})
	require.NoError(t, err)
	grpID := extractBatch4XMLValue(grp, "verifiedAccessGroupId")

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"CreateVerifiedAccessEndpoint"},
		"VerifiedAccessGroupId": {grpID},
		"EndpointType":          {"load-balancer"},
		"Description":           {"http policy endpoint"},
	})
	require.NoError(t, err)
	epID := extractBatch4XMLValue(ep, "verifiedAccessEndpointId")
	require.NotEmpty(t, epID)

	vals := url.Values{}
	vals.Set("Action", "ModifyVerifiedAccessEndpointPolicy")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessEndpointId", epID)
	vals.Set("PolicyEnabled", "true")
	vals.Set("PolicyDocument", "permit(principal,action,resource);")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ModifyVerifiedAccessEndpointPolicyResponse")
	assert.Contains(t, body, "<policyEnabled>true</policyEnabled>")
	assert.Contains(t, body, "<policyDocument>permit(principal,action,resource);</policyDocument>")

	getVals := url.Values{}
	getVals.Set("Action", "GetVerifiedAccessEndpointPolicy")
	getVals.Set("Version", "2016-11-15")
	getVals.Set("VerifiedAccessEndpointId", epID)

	getRec := postForm(t, h, getVals.Encode())
	require.Equal(t, http.StatusOK, getRec.Code)
	getBody := getRec.Body.String()
	assert.Contains(t, getBody, "<GetVerifiedAccessEndpointPolicyResponse")
	assert.Contains(t, getBody, "<policyEnabled>true</policyEnabled>")
}

func TestHandler_VerifiedAccessGroupPolicy(t *testing.T) {
	t.Parallel()

	h := newHandler()

	inst, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVerifiedAccessInstance"},
		"Description": {"grp policy test"},
	})
	require.NoError(t, err)
	instID := extractBatch4XMLValue(inst, "verifiedAccessInstanceId")

	grp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                   {"CreateVerifiedAccessGroup"},
		"VerifiedAccessInstanceId": {instID},
		"Description":              {"grp policy group"},
	})
	require.NoError(t, err)
	grpID := extractBatch4XMLValue(grp, "verifiedAccessGroupId")

	vals := url.Values{}
	vals.Set("Action", "ModifyVerifiedAccessGroupPolicy")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessGroupId", grpID)
	vals.Set("PolicyEnabled", "true")
	vals.Set("PolicyDocument", "permit(principal,action,resource);")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<ModifyVerifiedAccessGroupPolicyResponse")

	getVals := url.Values{}
	getVals.Set("Action", "GetVerifiedAccessGroupPolicy")
	getVals.Set("Version", "2016-11-15")
	getVals.Set("VerifiedAccessGroupId", grpID)

	getRec := postForm(t, h, getVals.Encode())
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "<policyEnabled>true</policyEnabled>")
}

func TestHandler_VerifiedAccessInstanceLoggingConfiguration(t *testing.T) {
	t.Parallel()

	h := newHandler()

	inst, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVerifiedAccessInstance"},
		"Description": {"logging http test"},
	})
	require.NoError(t, err)
	instID := extractBatch4XMLValue(inst, "verifiedAccessInstanceId")

	vals := url.Values{}
	vals.Set("Action", "ModifyVerifiedAccessInstanceLoggingConfiguration")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessInstanceId", instID)
	vals.Set("AccessLogs.CloudWatchLogs.Enabled", "true")
	vals.Set("AccessLogs.CloudWatchLogs.LogGroup", "my-log-group")
	vals.Set("AccessLogs.LogVersion", "ocsf-1.0.0-rc.2")
	vals.Set("AccessLogs.IncludeTrustContext", "true")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ModifyVerifiedAccessInstanceLoggingConfigurationResponse")
	assert.Contains(t, body, "<logGroup>my-log-group</logGroup>")
	assert.Contains(t, body, "<logVersion>ocsf-1.0.0-rc.2</logVersion>")

	descVals := url.Values{}
	descVals.Set("Action", "DescribeVerifiedAccessInstanceLoggingConfigurations")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("VerifiedAccessInstanceId.1", instID)

	descRec := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, descRec.Code)
	descBody := descRec.Body.String()
	assert.Contains(t, descBody, "<DescribeVerifiedAccessInstanceLoggingConfigurationsResponse")
	assert.Contains(t, descBody, "<logGroup>my-log-group</logGroup>")
}

func TestHandler_GetVerifiedAccessEndpointTargets(t *testing.T) {
	t.Parallel()

	h := newHandler()

	inst, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVerifiedAccessInstance"},
		"Description": {"targets http test"},
	})
	require.NoError(t, err)
	instID := extractBatch4XMLValue(inst, "verifiedAccessInstanceId")

	grp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                   {"CreateVerifiedAccessGroup"},
		"VerifiedAccessInstanceId": {instID},
		"Description":              {"targets group"},
	})
	require.NoError(t, err)
	grpID := extractBatch4XMLValue(grp, "verifiedAccessGroupId")

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"CreateVerifiedAccessEndpoint"},
		"VerifiedAccessGroupId": {grpID},
		"EndpointType":          {"cidr"},
		"Description":           {"targets endpoint"},
	})
	require.NoError(t, err)
	epID := extractBatch4XMLValue(ep, "verifiedAccessEndpointId")

	vals := url.Values{}
	vals.Set("Action", "GetVerifiedAccessEndpointTargets")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessEndpointId", epID)

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<GetVerifiedAccessEndpointTargetsResponse")
}

func TestHandler_ExportVerifiedAccessInstanceClientConfiguration(t *testing.T) {
	t.Parallel()

	h := newHandler()

	inst, err := ec2.ExportDispatch(h, url.Values{
		"Action":      {"CreateVerifiedAccessInstance"},
		"Description": {"export http test"},
	})
	require.NoError(t, err)
	instID := extractBatch4XMLValue(inst, "verifiedAccessInstanceId")

	vals := url.Values{}
	vals.Set("Action", "ExportVerifiedAccessInstanceClientConfiguration")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessInstanceId", instID)

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ExportVerifiedAccessInstanceClientConfigurationResponse")
	assert.Contains(t, body, "<region>us-east-1</region>")
}

func TestHandler_VerifiedAccessEndpointPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "GetVerifiedAccessEndpointPolicy")
	vals.Set("Version", "2016-11-15")
	vals.Set("VerifiedAccessEndpointId", "vae-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidVerifiedAccessEndpointId.NotFound")
}
