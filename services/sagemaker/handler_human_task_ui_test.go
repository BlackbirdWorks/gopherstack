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

func TestHandler_CreateHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
		"UiTemplate":      map[string]any{"Content": "<html></html>"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["HumanTaskUiArn"], "my-ui")
}

func TestHandler_CreateHumanTaskUI_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	tests := map[string]map[string]any{
		"missing name":             {"UiTemplate": map[string]any{"Content": "<html></html>"}},
		"missing ui template":      {"HumanTaskUiName": "ui-req"},
		"missing template content": {"HumanTaskUiName": "ui-req", "UiTemplate": map[string]any{}},
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateHumanTaskUi", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DescribeHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "ui-1",
		"UiTemplate":      map[string]any{"Content": "<html></html>"},
	})
	rec := doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ui-1", resp["HumanTaskUiName"])
}

func TestHandler_DescribeHumanTaskUI_UiTemplateContentSha256_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateHumanTaskUi(t.Context(), &sagemakersdk.CreateHumanTaskUiInput{
		HumanTaskUiName: aws.String("ui-sha"),
		UiTemplate:      &smtypes.UiTemplate{Content: aws.String("<html>hello</html>")},
	})
	require.NoError(t, err)

	out, err := client.DescribeHumanTaskUi(t.Context(), &sagemakersdk.DescribeHumanTaskUiInput{
		HumanTaskUiName: aws.String("ui-sha"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.UiTemplate)
	assert.NotEmpty(t, aws.ToString(out.UiTemplate.ContentSha256))
}

func TestHandler_DeleteHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "ui-del",
		"UiTemplate":      map[string]any{"Content": "<html></html>"},
	})
	rec := doSageMakerRequest(t, h, "DeleteHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListHumanTaskUIs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["HumanTaskUiSummaries"])

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
		"UiTemplate":      map[string]any{"Content": "<html></html>"},
	})

	rec = doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	uis := resp["HumanTaskUiSummaries"].([]any)
	assert.Len(t, uis, 1)
	u := uis[0].(map[string]any)
	assert.Equal(t, "my-ui", u["HumanTaskUiName"])
}

func TestHandler_ListHumanTaskUIs_SortOrder_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"alpha-ui", "beta-ui"} {
		_, err := client.CreateHumanTaskUi(t.Context(), &sagemakersdk.CreateHumanTaskUiInput{
			HumanTaskUiName: aws.String(name),
			UiTemplate:      &smtypes.UiTemplate{Content: aws.String("<html></html>")},
		})
		require.NoError(t, err)
	}

	out, err := client.ListHumanTaskUis(t.Context(), &sagemakersdk.ListHumanTaskUisInput{
		SortOrder: smtypes.SortOrderDescending,
	})
	require.NoError(t, err)
	require.Len(t, out.HumanTaskUiSummaries, 2)
	assert.Equal(t, "beta-ui", aws.ToString(out.HumanTaskUiSummaries[0].HumanTaskUiName))
	assert.Equal(t, "alpha-ui", aws.ToString(out.HumanTaskUiSummaries[1].HumanTaskUiName))
}
