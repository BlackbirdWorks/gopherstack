package iot_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestNewOps_RoleAlias tests RoleAlias CRUD.
func TestRoleAlias(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateRoleAlias
	out := iotOK(t, h, http.MethodPost, "/role-aliases/my-alias", map[string]any{
		"roleArn":                   "arn:aws:iam::000000000000:role/MyRole",
		"credentialDurationSeconds": 3600,
	})
	if out["roleAlias"] != "my-alias" {
		t.Errorf("roleAlias mismatch: %v", out)
	}

	// DescribeRoleAlias
	out2 := iotOK(t, h, http.MethodGet, "/role-aliases/my-alias", nil)
	desc, _ := out2["roleAliasDescription"].(map[string]any)
	if desc["roleAlias"] != "my-alias" {
		t.Errorf("describe role alias mismatch: %v", out2)
	}

	// ListRoleAliases
	out3 := iotOK(t, h, http.MethodGet, "/role-aliases", nil)
	aliases, _ := out3["roleAliases"].([]any)
	if len(aliases) != 1 {
		t.Errorf("expected 1 alias, got %d", len(aliases))
	}

	// UpdateRoleAlias
	iotOK(t, h, http.MethodPut, "/role-aliases/my-alias", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/UpdatedRole",
	})

	// DeleteRoleAlias
	iotOK(t, h, http.MethodDelete, "/role-aliases/my-alias", nil)

	iotExpectError(t, h, "/role-aliases/my-alias")
}

// TestNewOps_DomainConfiguration tests DomainConfiguration CRUD.
func TestDomainConfiguration(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateDomainConfiguration
	out := iotOK(t, h, http.MethodPost, "/domainConfigurations/my-config", map[string]any{
		"serviceType": "DATA",
	})
	if out["domainConfigurationName"] != "my-config" {
		t.Errorf("name mismatch: %v", out)
	}

	// DescribeDomainConfiguration
	out2 := iotOK(t, h, http.MethodGet, "/domainConfigurations/my-config", nil)
	if out2["domainConfigurationName"] != "my-config" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListDomainConfigurations
	out3 := iotOK(t, h, http.MethodGet, "/domainConfigurations", nil)
	configs, _ := out3["domainConfigurations"].([]any)
	if len(configs) != 1 {
		t.Errorf("expected 1 config, got %d", len(configs))
	}

	// UpdateDomainConfiguration
	iotOK(t, h, http.MethodPut, "/domainConfigurations/my-config", map[string]any{
		"domainConfigurationStatus": "DISABLED",
	})

	// DeleteDomainConfiguration
	iotOK(t, h, http.MethodDelete, "/domainConfigurations/my-config", nil)

	iotExpectError(t, h, "/domainConfigurations/my-config")
}

// TestListDomainConfigurations_ServiceTypeFilterAndPagination guards
// ListDomainConfigurationsInput's real serviceType/pageSize/marker query
// params (iot@v1.77.4 serializers.go
// awsRestjson1_serializeOpHttpBindingsListDomainConfigurationsInput) and
// ListDomainConfigurationsOutput's real nextMarker member -- previously all
// three request params were silently ignored (every domain configuration
// was always returned in one page regardless of serviceType) and no
// nextMarker was ever returned.
func TestListDomainConfigurations_ServiceTypeFilterAndPagination(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	iotOK(t, h, http.MethodPost, "/domainConfigurations/data-config", map[string]any{"serviceType": "DATA"})
	iotOK(t, h, http.MethodPost, "/domainConfigurations/cred-config",
		map[string]any{"serviceType": "CREDENTIAL_PROVIDER"})

	t.Run("service_type_filter", func(t *testing.T) {
		t.Parallel()

		out := iotOK(t, h, http.MethodGet, "/domainConfigurations?serviceType=DATA", nil)
		configs, _ := out["domainConfigurations"].([]any)
		require.Len(t, configs, 1)
		entry, _ := configs[0].(map[string]any)
		assert.Equal(t, "data-config", entry["domainConfigurationName"])
	})

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		out := iotOK(t, h, http.MethodGet, "/domainConfigurations?pageSize=1", nil)
		configs, _ := out["domainConfigurations"].([]any)
		require.Len(t, configs, 1)
		require.NotEmpty(t, out["nextMarker"])

		out2 := iotOK(t, h, http.MethodGet, "/domainConfigurations?pageSize=1&marker="+out["nextMarker"].(string), nil)
		configs2, _ := out2["domainConfigurations"].([]any)
		require.Len(t, configs2, 1)
		assert.Empty(t, out2["nextMarker"])
	})
}

// TestNewOps_ProvisioningTemplate tests ProvisioningTemplate CRUD.
func TestProvisioningTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateProvisioningTemplate, including preProvisioningHook -- a real
	// CreateProvisioningTemplateInput field (iot@v1.77.4) previously unmodeled.
	out := iotOK(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName": "my-template",
		"templateBody": `{"Parameters":{}}`,
		"enabled":      true,
		"type":         "FLEET_PROVISIONING",
		"preProvisioningHook": map[string]any{
			"targetArn": "arn:aws:lambda:us-east-1:000000000000:function:PreProvHook",
		},
	})
	if out["templateName"] != "my-template" {
		t.Errorf("templateName mismatch: %v", out)
	}
	if out["defaultVersionId"] == nil {
		t.Errorf("expected defaultVersionId on CreateProvisioningTemplate, got %v", out)
	}

	// DescribeProvisioningTemplate
	out2 := iotOK(t, h, http.MethodGet, "/provisioning-templates/my-template", nil)
	if out2["templateName"] != "my-template" {
		t.Errorf("describe mismatch: %v", out2)
	}
	hook, _ := out2["preProvisioningHook"].(map[string]any)
	if hook["targetArn"] != "arn:aws:lambda:us-east-1:000000000000:function:PreProvHook" {
		t.Errorf("expected preProvisioningHook.targetArn on DescribeProvisioningTemplate, got %v", out2)
	}

	// ListProvisioningTemplates
	out3 := iotOK(t, h, http.MethodGet, "/provisioning-templates", nil)
	templates, _ := out3["templates"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}
	tmplEntry, _ := templates[0].(map[string]any)
	if tmplEntry["type"] != "FLEET_PROVISIONING" {
		t.Errorf("expected type on ListProvisioningTemplates entry, got %v", tmplEntry)
	}

	// CreateProvisioningTemplateVersion
	out4 := iotOK(
		t,
		h,
		http.MethodPost,
		"/provisioning-templates/my-template/versions",
		map[string]any{
			"templateBody": `{"Parameters":{"v2":{}}}`,
			"setAsDefault": true,
		},
	)
	if out4["templateName"] != "my-template" {
		t.Errorf("version templateName mismatch: %v", out4)
	}
	if out4["templateArn"] == "" || out4["templateArn"] == nil {
		t.Errorf("expected templateArn on CreateProvisioningTemplateVersion, got %v", out4)
	}
	if out4["isDefaultVersion"] != true {
		t.Errorf("expected isDefaultVersion true (setAsDefault was sent), got %v", out4)
	}

	// ListProvisioningTemplateVersions
	out5 := iotOK(t, h, http.MethodGet, "/provisioning-templates/my-template/versions", nil)
	versions, _ := out5["versions"].([]any)
	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}

	// UpdateProvisioningTemplate
	iotOK(t, h, http.MethodPatch, "/provisioning-templates/my-template", map[string]any{
		"description": "updated",
	})

	// DeleteProvisioningTemplateVersion
	iotOK(t, h, http.MethodDelete, "/provisioning-templates/my-template/versions/2", nil)

	// DeleteProvisioningTemplate
	iotOK(t, h, http.MethodDelete, "/provisioning-templates/my-template", nil)

	iotExpectError(t, h, "/provisioning-templates/my-template")
}

func TestDescribeProvisioningTemplateVersion(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
			TemplateName: "tmpl1",
			TemplateBody: `{"Version":"2020-01-01"}`,
		})
		require.NoError(t, err)

		v, err := b.CreateProvisioningTemplateVersion("tmpl1", `{"Version":"2020-01-01","v":2}`, false)
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet,
			"/provisioning-templates/tmpl1/versions/"+itoa(v.VersionID), nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"versionId":2`)
		assert.Contains(t, rec.Body.String(), `2020-01-01`)
	})

	t.Run("unknown_template_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet, "/provisioning-templates/no-such/versions/1", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("unknown_version_404", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
			TemplateName: "tmpl2",
			TemplateBody: `{}`,
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/provisioning-templates/tmpl2/versions/999", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestUpdateProvisioningTemplate_AdvancedFieldsSurvive guards
// UpdateProvisioningTemplateInput's real defaultVersionId/
// preProvisioningHook/removePreProvisioningHook members (iot@v1.77.4
// api_op_UpdateProvisioningTemplate.go), previously all silently dropped.
// Driven through a real generated AWS SDK v2 client.
func TestUpdateProvisioningTemplate_AdvancedFieldsSurvive(t *testing.T) {
	t.Parallel()

	client, _ := newIoTSDKClient(t)
	ctx := t.Context()

	_, err := client.CreateProvisioningTemplate(ctx, &iotsdk.CreateProvisioningTemplateInput{
		TemplateName:        aws.String("adv-tmpl"),
		TemplateBody:        aws.String(`{"Version":"2020-01-01"}`),
		ProvisioningRoleArn: aws.String("arn:aws:iam::000000000000:role/ProvRole"),
	})
	require.NoError(t, err)

	_, err = client.CreateProvisioningTemplateVersion(ctx, &iotsdk.CreateProvisioningTemplateVersionInput{
		TemplateName: aws.String("adv-tmpl"),
		TemplateBody: aws.String(`{"Version":"2020-01-01","v":2}`),
	})
	require.NoError(t, err)

	_, err = client.UpdateProvisioningTemplate(ctx, &iotsdk.UpdateProvisioningTemplateInput{
		TemplateName:     aws.String("adv-tmpl"),
		DefaultVersionId: aws.Int32(2),
		PreProvisioningHook: &iottypes.ProvisioningHook{
			TargetArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:hook"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeProvisioningTemplate(ctx, &iotsdk.DescribeProvisioningTemplateInput{
		TemplateName: aws.String("adv-tmpl"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, aws.ToInt32(out.DefaultVersionId))
	require.NotNil(t, out.PreProvisioningHook)
	assert.Equal(t,
		"arn:aws:lambda:us-east-1:000000000000:function:hook", aws.ToString(out.PreProvisioningHook.TargetArn),
	)

	_, err = client.UpdateProvisioningTemplate(ctx, &iotsdk.UpdateProvisioningTemplateInput{
		TemplateName:              aws.String("adv-tmpl"),
		RemovePreProvisioningHook: aws.Bool(true),
	})
	require.NoError(t, err)

	out2, err := client.DescribeProvisioningTemplate(ctx, &iotsdk.DescribeProvisioningTemplateInput{
		TemplateName: aws.String("adv-tmpl"),
	})
	require.NoError(t, err)
	assert.Nil(t, out2.PreProvisioningHook, "removePreProvisioningHook must clear the hook")
}
