package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("del-dp", "mysql")

	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderArn": "del-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.DataProviderCount())
}

func TestModifyDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("mod-dp", "mysql")

	rec := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderArn": "mod-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestDeleteDataProvider_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderArn": "arn:aws:dms:us-east-1:123:data-provider:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundFault", body["__type"])
}
