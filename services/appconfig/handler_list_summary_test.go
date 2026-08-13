package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestHandler_ListOps_SummaryShape locks that every List op fixed by
// gopherstack-xs7l emits its real types.*Summary shape instead of
// raw-marshaling the full domain struct. These assertions decode the raw
// JSON body into map[string]any rather than a typed struct or going
// through an AWS SDK client -- either of those silently drops a JSON key
// they don't recognize, so neither would notice a leaked Get-only field
// still riding along on the wire.
func TestHandler_ListOps_SummaryShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, h *appconfig.Handler) map[string]any
		name    string
		present []string
		leaked  []string
	}{
		{
			name:    "configurationprofiles",
			present: []string{"ApplicationId", "Id", "Name", "LocationUri", "Type", "ValidatorTypes"},
			leaked:  []string{"Description", "RetrievalRoleArn", "Validators"},
			setup:   setupConfigurationProfileListLeak,
		},
		{
			name: "deployments",
			present: []string{
				"ConfigurationProfileId", "ConfigurationVersion", "State", "Type", "DeploymentNumber",
			},
			leaked: []string{
				"ApplicationId", "EnvironmentId", "DeploymentStrategyId", "Description",
				"ConfigurationLocationUri", "EventLog", "AppliedExtensions",
			},
			setup: setupDeploymentListLeak,
		},
		{
			name:    "experimentruns",
			present: []string{"ExperimentDefinitionId", "Description", "Status", "Run"},
			leaked: []string{
				"ApplicationId", "ExperimentDefinitionSnapshot", "ExposurePercentage", "Result",
				"TreatmentOverrides",
			},
			setup: setupExperimentRunListLeak,
		},
		{
			name: "experimentdefinitions",
			present: []string{
				"ApplicationId", "Id", "Name", "ConfigurationProfileId", "EnvironmentId", "FlagKey",
				"Hypothesis", "Status",
			},
			leaked: []string{
				"AudienceDescription", "AudienceRule", "Control", "KmsKeyIdentifier", "LaunchCriteria",
				"Treatments",
			},
			setup: setupExperimentDefinitionListLeak,
		},
		{
			name:    "extensionassociations",
			present: []string{"ExtensionArn", "Id", "ResourceArn"},
			leaked:  []string{"Arn", "Parameters", "ExtensionVersionNumber"},
			setup:   setupExtensionAssociationListLeak,
		},
		{
			name:    "extensions",
			present: []string{"Arn", "Description", "Id", "Name", "VersionNumber"},
			leaked:  []string{"Actions", "Parameters"},
			setup:   setupExtensionListLeak,
		},
		{
			name:    "hostedconfigurationversions",
			present: []string{"ApplicationId", "ConfigurationProfileId", "ContentType", "Description", "VersionNumber"},
			// CreatedAt is a genuine leak (Get-only, dropped by this fix).
			// KmsKeyArn is a real Summary member too, but was never emitted
			// before this fix either -- this backend has no honest source
			// for it (see HostedConfigurationVersionSummary's doc comment
			// in models.go), so it stays absent by design, not by leak.
			leaked: []string{"CreatedAt", "KmsKeyArn"},
			setup:  setupHostedConfigurationVersionListLeak,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			item := tt.setup(t, h)

			for _, k := range tt.present {
				assert.Contains(t, item, k, "expected real Summary member %q", k)
			}

			for _, k := range tt.leaked {
				assert.NotContains(t, item, k, "leaked Get-only member %q", k)
			}
		})
	}
}

// listSingleItem GETs path, requires exactly one entry under the "Items"
// key, and returns it as a raw map -- decoding into map[string]any rather
// than a typed struct so a leaked (or missing) key is actually visible to
// the assertions in TestHandler_ListOps_SummaryShape.
func listSingleItem(t *testing.T, h *appconfig.Handler, path string) map[string]any {
	t.Helper()

	rec := doRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, ok := resp["Items"].([]any)
	require.True(t, ok, "response missing Items list, got keys: %v", mapKeys(resp))
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)

	return item
}

func setupConfigurationProfileListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"cp-list-leak-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	profBody := []byte(`{
		"Name": "cp-list-leak-profile",
		"Description": "should not leak on List",
		"LocationUri": "hosted",
		"Type": "AWS.Freeform",
		"RetrievalRoleArn": "arn:aws:iam::123456789012:role/Retrieval",
		"Validators": [{"Type": "JSON_SCHEMA", "Content": "{}"}]
	}`)
	profRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profBody)
	require.Equal(t, http.StatusCreated, profRec.Code)

	return listSingleItem(t, h, "/applications/"+app.ID+"/configurationprofiles")
}

func setupDeploymentListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"dep-list-leak-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"dep-list-leak-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	profRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"dep-list-leak-profile","LocationUri":"hosted"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	hcvRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
		[]byte(`{"enabled":true}`))
	require.Equal(t, http.StatusCreated, hcvRec.Code)

	stratBody := []byte(
		`{"Name":"dep-list-leak-strat","DeploymentDurationInMinutes":0,"GrowthFactor":100,"ReplicateTo":"NONE"}`,
	)
	stratRec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", stratBody)
	require.Equal(t, http.StatusCreated, stratRec.Code)

	var strat struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(stratRec.Body.Bytes(), &strat))

	depBody := []byte(`{
		"ConfigurationProfileId": "` + prof.ID + `",
		"DeploymentStrategyId": "` + strat.ID + `",
		"ConfigurationVersion": "1",
		"Description": "should not leak on List"
	}`)
	depRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/environments/"+env.ID+"/deployments", depBody)
	require.Equal(t, http.StatusCreated, depRec.Code)

	var dep struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(depRec.Body.Bytes(), &dep))
	require.Equal(t, "COMPLETE", dep.State, "a zero-duration strategy must complete synchronously")

	return listSingleItem(t, h, "/applications/"+app.ID+"/environments/"+env.ID+"/deployments")
}

func setupExperimentRunListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	appID, def := seedExperimentRunHTTP(t, h)

	base := "/applications/" + appID + "/experimentdefinitions/" + def.ID + "/experimentruns"
	runRec := doRequest(t, h, http.MethodPost, base,
		[]byte(`{"Description":"should not leak on List","ExposurePercentage":10}`))
	require.Equal(t, http.StatusCreated, runRec.Code)

	return listSingleItem(t, h, base)
}

func setupExperimentDefinitionListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	appID, envID, profID := seedExperimentDefinitionHTTP(t, h)

	body := `{
		"Name": "def-list-leak",
		"AudienceRule": "true",
		"AudienceDescription": "everyone",
		"FlagKey": "flag1",
		"Hypothesis": "will improve retention",
		"LaunchCriteria": "p > 0.95",
		"EnvironmentIdentifier": "` + envID + `",
		"ConfigurationProfileIdentifier": "` + profID + `",
		"Control": {"FlagValue": {"Enabled": false}, "Weight": 50},
		"Treatments": [{"FlagValue": {"Enabled": true}, "Weight": 50}]
	}`
	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/experimentdefinitions", []byte(body))
	require.Equal(t, http.StatusCreated, rec.Code)

	return listSingleItem(t, h, "/experimentdefinitions?application_identifier="+appID)
}

func setupExtensionListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	body := []byte(`{
		"Name": "ext-list-leak",
		"Description": "should not leak on List",
		"Actions": {"ON_DEPLOYMENT_START": [{"Name": "act1", "Uri": "lambda-arn"}]},
		"Parameters": {"param1": {"Description": "p desc", "Required": true}}
	}`)
	rec := doRequest(t, h, http.MethodPost, "/extensions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	return listSingleItem(t, h, "/extensions")
}

func setupExtensionAssociationListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	extRec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"ext-assoc-list-leak"}`))
	require.Equal(t, http.StatusCreated, extRec.Code)

	var ext struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(extRec.Body.Bytes(), &ext))

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"ext-assoc-list-leak-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID
	assocBody := []byte(`{
		"ExtensionIdentifier": "` + ext.ID + `",
		"ResourceIdentifier": "` + resourceID + `",
		"Parameters": {"key1": "val1"}
	}`)
	assocRec := doRequest(t, h, http.MethodPost, "/extensionassociations", assocBody)
	require.Equal(t, http.StatusCreated, assocRec.Code)

	return listSingleItem(t, h, "/extensionassociations")
}

func setupHostedConfigurationVersionListLeak(t *testing.T, h *appconfig.Handler) map[string]any {
	t.Helper()

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"hcv-list-leak-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	profRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"hcv-list-leak-profile","LocationUri":"hosted"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	hcvRec := doRequestWithHeader(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
		"Description", "should not leak on List",
		[]byte(`{"enabled":true}`))
	require.Equal(t, http.StatusCreated, hcvRec.Code)

	return listSingleItem(
		t, h, "/applications/"+app.ID+"/configurationprofiles/"+prof.ID+"/hostedconfigurationversions",
	)
}

// TestListConfigurationProfiles_ValidatorTypesViaSDKClient proves the
// gopherstack-xs7l inverse-direction fix -- ConfigurationProfileSummary.
// ValidatorTypes was never emitted -- through the real aws-sdk-go-v2
// client rather than a raw-body decode. The SDK's own deserializer
// (awsRestjson1_deserializeDocumentConfigurationProfileSummary,
// deserializers.go:12061) only accepts a well-formed []types.ValidatorType,
// so a successful client-side decode here confirms the wire shape itself,
// not merely that some key named "ValidatorTypes" is present.
func TestListConfigurationProfiles_ValidatorTypesViaSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("cp-validatortypes-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: appOut.Id,
		Name:          aws.String("cp-validatortypes-profile"),
		LocationUri:   aws.String("hosted"),
		Validators: []types.Validator{
			{Type: types.ValidatorTypeJsonSchema, Content: aws.String("{}")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListConfigurationProfiles(t.Context(), &appconfigsdk.ListConfigurationProfilesInput{
		ApplicationId: appOut.Id,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Items, 1)

	assert.Equal(t, []types.ValidatorType{types.ValidatorTypeJsonSchema}, listOut.Items[0].ValidatorTypes)
}

// TestListDeployments_TypeViaSDKClient proves the gopherstack-xs7l
// inverse-direction fix -- DeploymentSummary.Type was never emitted, even
// though it is a real Summary-only member GetDeployment's own output shape
// lacks entirely -- through the real aws-sdk-go-v2 client. The SDK's own
// deserializer (awsRestjson1_deserializeDocumentDeploymentSummary,
// deserializers.go:12583) only accepts a valid types.DeploymentType
// string, so a successful client-side decode confirms the wire shape, not
// just key presence.
func TestListDeployments_TypeViaSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("dep-type-app"),
	})
	require.NoError(t, err)

	envOut, err := client.CreateEnvironment(t.Context(), &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: appOut.Id,
		Name:          aws.String("dep-type-env"),
	})
	require.NoError(t, err)

	profOut, err := client.CreateConfigurationProfile(t.Context(), &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: appOut.Id,
		Name:          aws.String("dep-type-profile"),
		LocationUri:   aws.String("hosted"),
	})
	require.NoError(t, err)

	_, err = client.CreateHostedConfigurationVersion(
		t.Context(),
		&appconfigsdk.CreateHostedConfigurationVersionInput{
			ApplicationId:          appOut.Id,
			ConfigurationProfileId: profOut.Id,
			Content:                []byte(`{"enabled":true}`),
			ContentType:            aws.String("application/json"),
		},
	)
	require.NoError(t, err)

	stratOut, err := client.CreateDeploymentStrategy(t.Context(), &appconfigsdk.CreateDeploymentStrategyInput{
		Name:                        aws.String("dep-type-strat"),
		DeploymentDurationInMinutes: aws.Int32(0),
		GrowthFactor:                aws.Float32(100),
		ReplicateTo:                 types.ReplicateToNone,
	})
	require.NoError(t, err)

	_, err = client.StartDeployment(t.Context(), &appconfigsdk.StartDeploymentInput{
		ApplicationId:          appOut.Id,
		EnvironmentId:          envOut.Id,
		ConfigurationProfileId: profOut.Id,
		DeploymentStrategyId:   stratOut.Id,
		ConfigurationVersion:   aws.String("1"),
	})
	require.NoError(t, err)

	listOut, err := client.ListDeployments(t.Context(), &appconfigsdk.ListDeploymentsInput{
		ApplicationId: appOut.Id,
		EnvironmentId: envOut.Id,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Items, 1)

	assert.Equal(t, types.DeploymentTypeUser, listOut.Items[0].Type)
}
