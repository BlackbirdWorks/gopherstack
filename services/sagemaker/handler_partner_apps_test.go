package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreatePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "my-app",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["Arn"], "my-app")
}

func TestHandler_DescribePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "app-1",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "app-1", resp["Name"])
}

func TestHandler_DeletePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "app-del",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// TrainingPlan
// ---------------------------------------------------------------------------

func TestHandler_PartnerApp_ExtendedLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "papp-1",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]

	describeRec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "papp-1", describeResp["Name"])
	assert.Equal(t, "comet", describeResp["Type"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/partner", describeResp["ExecutionRoleArn"])
	assert.Equal(t, "small", describeResp["Tier"])

	updateRec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  appARN,
		"Tier": "large",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "large", describeResp["Tier"])

	listRec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	presignRec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["Url"], "papp-1")
}

func TestHandler_UpdatePartnerApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
		"Tier": "large",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdatePartnerApp_MissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{"Tier": "large"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreatePartnerAppPresignedUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListPartnerApps_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Summaries"])
}

func TestHandler_DeletePartnerApp_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "papp-del",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, createResp["Arn"], resp["Arn"])
}

// TestHandler_CreatePartnerApp_RequiredFieldsEnforced asserts AuthType/
// ExecutionRoleArn/Tier/Type -- all required on CreatePartnerAppInput but
// previously enforced by nothing beyond Name -- now reject when absent.
func TestHandler_CreatePartnerApp_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"Name":             "req-fields-app",
		"Type":             "comet",
		"AuthType":         "IAM",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	}

	for _, field := range []string{"AuthType", "ExecutionRoleArn", "Tier", "Type"} {
		t.Run("missing "+field, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			req := make(map[string]any, len(full))
			for k, v := range full {
				if k != field {
					req[k] = v
				}
			}

			rec := doSageMakerRequest(t, h, "CreatePartnerApp", req)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_CreatePartnerApp_FullFields_RealClient asserts
// EnableAutoMinorVersionUpgrade/EnableIamSessionBasedIdentity/KmsKeyId/
// MaintenanceConfig -- previously entirely absent from Create -- are now
// stored and echoed back on Describe, along with the newly synthesized
// BaseUrl.
func TestHandler_CreatePartnerApp_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	create, err := client.CreatePartnerApp(t.Context(), &sagemakersdk.CreatePartnerAppInput{
		Name:                          aws.String("full-fields-app"),
		Type:                          smtypes.PartnerAppTypeComet,
		AuthType:                      smtypes.PartnerAppAuthTypeIam,
		ExecutionRoleArn:              aws.String("arn:aws:iam::000000000000:role/partner"),
		Tier:                          aws.String("small"),
		KmsKeyId:                      aws.String("arn:aws:kms:us-east-1:000000000000:key/abc"),
		EnableAutoMinorVersionUpgrade: aws.Bool(true),
		EnableIamSessionBasedIdentity: aws.Bool(true),
		MaintenanceConfig: &smtypes.PartnerAppMaintenanceConfig{
			MaintenanceWindowStart: aws.String("TUE:03:30"),
		},
		ApplicationConfig: &smtypes.PartnerAppConfig{
			AdminUsers: []string{"alice"},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribePartnerApp(t.Context(), &sagemakersdk.DescribePartnerAppInput{
		Arn: create.Arn,
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(desc.EnableAutoMinorVersionUpgrade))
	assert.True(t, aws.ToBool(desc.EnableIamSessionBasedIdentity))
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", aws.ToString(desc.KmsKeyId))
	require.NotNil(t, desc.MaintenanceConfig)
	assert.Equal(t, "TUE:03:30", aws.ToString(desc.MaintenanceConfig.MaintenanceWindowStart))
	require.NotNil(t, desc.ApplicationConfig)
	assert.Equal(t, []string{"alice"}, desc.ApplicationConfig.AdminUsers)
	assert.Contains(t, aws.ToString(desc.BaseUrl), "full-fields-app")
}

// TestHandler_UpdatePartnerApp_Tags_RealClient asserts UpdatePartnerAppInput's
// Tags -- previously entirely discarded -- now apply and are reachable
// through ListTags.
func TestHandler_UpdatePartnerApp_Tags_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	create, err := client.CreatePartnerApp(t.Context(), &sagemakersdk.CreatePartnerAppInput{
		Name:             aws.String("tags-app"),
		Type:             smtypes.PartnerAppTypeComet,
		AuthType:         smtypes.PartnerAppAuthTypeIam,
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/partner"),
		Tier:             aws.String("small"),
	})
	require.NoError(t, err)

	_, err = client.UpdatePartnerApp(t.Context(), &sagemakersdk.UpdatePartnerAppInput{
		Arn:  create.Arn,
		Tags: []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{ResourceArn: create.Arn})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.Tags[0].Value))
}

// TestHandler_ListPartnerApps_MaxResults_RealClient asserts
// ListPartnerAppsInput.MaxResults -- previously entirely absent, only
// NextToken was ever read -- now caps the page and returns a token.
func TestHandler_ListPartnerApps_MaxResults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, n := range []string{"papp-a", "papp-b", "papp-c"} {
		_, err := client.CreatePartnerApp(t.Context(), &sagemakersdk.CreatePartnerAppInput{
			Name:             aws.String(n),
			Type:             smtypes.PartnerAppTypeComet,
			AuthType:         smtypes.PartnerAppAuthTypeIam,
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/partner"),
			Tier:             aws.String("small"),
		})
		require.NoError(t, err)
	}

	out, err := client.ListPartnerApps(t.Context(), &sagemakersdk.ListPartnerAppsInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Summaries, 1)
	assert.NotEmpty(t, aws.ToString(out.NextToken))
}
