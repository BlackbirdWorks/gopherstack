package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// --- Integration ConnectionType / VPC_LINK ---

func TestIntegration_ConnectionType_VpcLink(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "vpc-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})

	vpcLink, _ := b.CreateVpcLink(apigateway.CreateVpcLinkInput{
		Name:       "my-link",
		TargetARNs: []string{"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/abc123"},
	})

	integ, err := b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:           "HTTP",
		HTTPMethod:     "GET",
		URI:            "https://internal.example.com/api",
		ConnectionType: "VPC_LINK",
		ConnectionID:   vpcLink.ID,
	})
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", integ.ConnectionType)
	assert.Equal(t, vpcLink.ID, integ.ConnectionID)

	got, err := b.GetIntegration(api.ID, rootID, "GET")
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", got.ConnectionType)
	assert.Equal(t, vpcLink.ID, got.ConnectionID)
}

func TestIntegration_ConnectionType_Internet(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "inet-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
	})

	integ, err := b.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{
		Type:           "HTTP_PROXY",
		HTTPMethod:     "POST",
		URI:            "https://api.example.com/v1/items",
		ConnectionType: "INTERNET",
	})
	require.NoError(t, err)
	assert.Equal(t, "INTERNET", integ.ConnectionType)
}

// --- Integration ContentHandling + Credentials + CacheNamespace ---

func TestIntegration_ContentHandling_Credentials_Cache(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cred-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "PUT",
		AuthorizationType: "NONE",
	})

	integ, err := b.PutIntegration(api.ID, rootID, "PUT", apigateway.PutIntegrationInput{
		Type:               "AWS",
		HTTPMethod:         "POST",
		URI:                "arn:aws:lambda:::function:fn",
		ContentHandling:    "CONVERT_TO_TEXT",
		Credentials:        "arn:aws:iam::123456789012:role/MyApiRole",
		CacheNamespace:     "myns",
		CacheKeyParameters: []string{"method.request.header.Authorization"},
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_TEXT", integ.ContentHandling)
	assert.Equal(t, "arn:aws:iam::123456789012:role/MyApiRole", integ.Credentials)
	assert.Equal(t, "myns", integ.CacheNamespace)
	assert.Equal(t, []string{"method.request.header.Authorization"}, integ.CacheKeyParameters)
}

// --- Integration RequestParameters ---

func TestIntegration_RequestParameters(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "rp-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})

	reqParams := map[string]string{
		"integration.request.header.X-User-Id": "method.request.header.Authorization",
	}
	integ, err := b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:              "AWS_PROXY",
		HTTPMethod:        "POST",
		URI:               "arn:aws:lambda:::function:fn",
		RequestParameters: reqParams,
	})
	require.NoError(t, err)
	assert.Equal(t, reqParams, integ.RequestParameters)

	newParams := map[string]string{
		"integration.request.querystring.userId": "method.request.querystring.id",
	}
	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		RequestParameters: newParams,
	})
	require.NoError(t, err)
	assert.Equal(t, newParams, updated.RequestParameters)
}

// --- Stage ClientCertificateID ---

func TestStage_ClientCertificateId_Create(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cert-api"})
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	cert, err := b.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{
		Description: "test cert",
	})
	require.NoError(t, err)

	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:           api.ID,
		StageName:           "prod",
		DeploymentID:        depl.ID,
		ClientCertificateID: cert.ClientCertificateID,
	})
	require.NoError(t, err)
	assert.Equal(t, cert.ClientCertificateID, stage.ClientCertificateID)

	got, err := b.GetStage(api.ID, "prod")
	require.NoError(t, err)
	assert.Equal(t, cert.ClientCertificateID, got.ClientCertificateID)
}

func TestStage_ClientCertificateId_Update(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cert-upd-api"})
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "dev",
		DeploymentID: depl.ID,
	})
	require.NoError(t, err)
	assert.Empty(t, stage.ClientCertificateID)

	cert, _ := b.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{})
	updated, err := b.UpdateStage(api.ID, "dev", apigateway.UpdateStageInput{
		ClientCertificateID: cert.ClientCertificateID,
	})
	require.NoError(t, err)
	assert.Equal(t, cert.ClientCertificateID, updated.ClientCertificateID)
}

// --- DomainName: regional + distribution fields + SecurityPolicy ---

func TestDomainName_RegionalAndDistributionFields(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:             "api.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
		SecurityPolicy:         "TLS_1_2",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
	assert.NotEmpty(t, dn.RegionalDomainName)
	assert.NotEmpty(t, dn.RegionalHostedZoneID)
	assert.NotEmpty(t, dn.DistributionDomainName)
	assert.NotEmpty(t, dn.DistributionHostedZoneID)
	assert.Equal(t, "AVAILABLE", dn.DomainNameStatus)
	assert.NotNil(t, dn.EndpointConfiguration)
}

func TestDomainName_DefaultSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "default.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/xyz",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
}

func TestDomainName_EndpointConfiguration_Edge(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	dn, err := b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "edge.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/edge",
		EndpointConfiguration: &apigateway.EndpointConfiguration{
			Types: []string{"EDGE"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dn.EndpointConfiguration)
	assert.Contains(t, dn.EndpointConfiguration.Types, "EDGE")
	assert.NotEmpty(t, dn.DistributionDomainName)
}

func TestDomainName_UpdateSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	_, _ = b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:     "update.example.com",
		CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/upd",
		SecurityPolicy: "TLS_1_0",
	})

	updated, err := b.UpdateDomainName(apigateway.UpdateDomainNameInput{
		DomainName:     "update.example.com",
		SecurityPolicy: "TLS_1_2",
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", updated.SecurityPolicy)
}

func TestDomainName_UpdateRegionalCert(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	_, _ = b.CreateDomainName(apigateway.CreateDomainNameInput{
		DomainName:             "regional.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/old",
	})

	updated, err := b.UpdateDomainName(apigateway.UpdateDomainNameInput{
		DomainName:             "regional.example.com",
		RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/new",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/new", updated.RegionalCertificateARN)
}

// --- UsagePlan APIStages ---

func TestUsagePlan_ApiStages_Create(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "stage-plan-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")

	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "throttle-plan",
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
		},
		Throttle: &apigateway.ThrottleSettings{RateLimit: 1000, BurstLimit: 500},
	})
	require.NoError(t, err)
	require.Len(t, plan.APIStages, 1)
	assert.Equal(t, api.ID, plan.APIStages[0].RestAPIID)
	assert.Equal(t, "prod", plan.APIStages[0].Stage)

	got, err := b.GetUsagePlan(plan.ID)
	require.NoError(t, err)
	require.Len(t, got.APIStages, 1)
}

func TestUsagePlan_ApiStages_Update(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-plan-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")
	api2, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-plan-api2"})
	_, _ = b.CreateDeployment(api2.ID, "v1", "v1")

	plan, _ := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "update-plan",
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
		},
	})

	updated, err := b.UpdateUsagePlan(apigateway.UpdateUsagePlanInput{
		UsagePlanID: plan.ID,
		APIStages: []apigateway.APIStageAssociation{
			{RestAPIID: api.ID, Stage: "prod"},
			{RestAPIID: api2.ID, Stage: "v1"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, updated.APIStages, 2)
}

func TestUsagePlan_ApiStages_PerStageThrottle(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "throttle-stage-api"})
	_, _ = b.CreateDeployment(api.ID, "prod", "v1")

	methodThrottle := map[string]*apigateway.ThrottleSettings{
		"GET /items": {RateLimit: 100, BurstLimit: 50},
	}
	plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name: "per-method-plan",
		APIStages: []apigateway.APIStageAssociation{
			{
				RestAPIID: api.ID,
				Stage:     "prod",
				Throttle:  methodThrottle,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, plan.APIStages, 1)
	require.NotNil(t, plan.APIStages[0].Throttle)
	assert.InDelta(t, 100.0, plan.APIStages[0].Throttle["GET /items"].RateLimit, 0.001)
}

// --- UpdateIntegration: ConnectionType + CacheKeyParameters ---

func TestUpdateIntegration_ConnectionType(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-integ-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "HTTP",
		HTTPMethod: "GET",
		URI:        "https://example.com/api",
	})

	vpcLink, _ := b.CreateVpcLink(apigateway.CreateVpcLinkInput{
		Name:       "my-link2",
		TargetARNs: []string{"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/nlb/abc"},
	})

	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "GET",
		ConnectionType:     "VPC_LINK",
		ConnectionID:       vpcLink.ID,
		ContentHandling:    "CONVERT_TO_BINARY",
		CacheNamespace:     "mynamespace",
		CacheKeyParameters: []string{"method.request.path.id"},
	})
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", updated.ConnectionType)
	assert.Equal(t, vpcLink.ID, updated.ConnectionID)
	assert.Equal(t, "CONVERT_TO_BINARY", updated.ContentHandling)
	assert.Equal(t, "mynamespace", updated.CacheNamespace)
	assert.Equal(t, []string{"method.request.path.id"}, updated.CacheKeyParameters)
}

// --- Handler: Integration ConnectionType via REST endpoint ---

func TestHandlerIntegration_ConnectionType_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"conn-type-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID := apiResp["id"].(string)

	rec = restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var resResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resResp))
	items := resResp["item"].([]any)
	rootID := items[0].(map[string]any)["id"].(string)

	rec = restRequest(t, h, http.MethodPut,
		"/restapis/"+apiID+"/resources/"+rootID+"/methods/GET",
		`{"httpMethod":"GET","authorizationType":"NONE"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = restRequest(t, h, http.MethodPut,
		"/restapis/"+apiID+"/resources/"+rootID+"/methods/GET/integration",
		`{"type":"HTTP","httpMethod":"GET","uri":"https://api.example.com/","connectionType":"INTERNET"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var integResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&integResp))
	assert.Equal(t, "INTERNET", integResp["connectionType"])
}

// --- Handler: DomainName with SecurityPolicy ---

func TestHandlerDomainName_SecurityPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(
		t,
		h,
		http.MethodPost,
		"/domainnames",
		`{"domainName":"secure.example.com",`+
			`"certificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/abc",`+
			`"securityPolicy":"TLS_1_2"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "TLS_1_2", resp["securityPolicy"])
	assert.NotEmpty(t, resp["regionalDomainName"])
	assert.NotEmpty(t, resp["distributionDomainName"])
}

// --- Handler: UsagePlan APIStages ---

func TestHandlerUsagePlan_ApiStages(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"plan-stage-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID := apiResp["id"].(string)

	rec = restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments",
		`{"stageName":"prod","description":"v1"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	body := `{"name":"stages-plan",` +
		`"apiStages":[{"restApiId":"` + apiID + `","stage":"prod"}],` +
		`"throttle":{"rateLimit":500,"burstLimit":200}}`
	rec = restRequest(t, h, http.MethodPost, "/usageplans", body)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var planResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&planResp))
	apiStages, ok := planResp["apiStages"].([]any)
	require.True(t, ok)
	require.Len(t, apiStages, 1)
	firstStage := apiStages[0].(map[string]any)
	assert.Equal(t, apiID, firstStage["restApiId"])
	assert.Equal(t, "prod", firstStage["stage"])
}

// --- Handler: Stage ClientCertificateID ---

func TestHandlerStage_ClientCertificateId(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPost, "/clientcertificates", `{"description":"test cert"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var certResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&certResp))
	certID := certResp["clientCertificateId"].(string)
	require.NotEmpty(t, certID)

	rec = restRequest(t, h, http.MethodPost, "/restapis", `{"name":"cert-stage-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID := apiResp["id"].(string)

	rec = restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments",
		`{"stageName":"prod","description":"v1"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = restRequest(t, h, http.MethodPatch, "/restapis/"+apiID+"/stages/prod",
		`{"clientCertificateId":"`+certID+`"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var stageResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&stageResp))
	assert.Equal(t, certID, stageResp["clientCertificateId"])
}
