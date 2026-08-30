package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaExtensions_StateLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("start returns extension ID", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":         dirID,
			"Description":         "Add custom attrs",
			"SchemaExtensionBody": "dn: CN=foo,DC=corp,DC=example,DC=com",
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		extID, ok := body["SchemaExtensionId"].(string)
		require.True(t, ok)
		assert.NotEmpty(t, extID)
	})

	t.Run("list shows extension after start", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		startRec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":         dirID,
			"Description":         "My extension",
			"SchemaExtensionBody": "dn: CN=foo",
		})
		startBody := respBody(t, startRec)
		extID := startBody["SchemaExtensionId"].(string)

		listRec := doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": dirID})
		body := respBody(t, listRec)
		exts, _ := body["SchemaExtensionsInfo"].([]any)
		require.Len(t, exts, 1)
		ext := exts[0].(map[string]any)
		assert.Equal(t, extID, ext["SchemaExtensionId"])
		assert.Equal(t, "Completed", ext["SchemaExtensionStatus"])
	})

	t.Run("cancel sets status to CancelInProgress", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		startRec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":         dirID,
			"Description":         "cancelable",
			"SchemaExtensionBody": "dn: CN=foo",
		})
		startBody := respBody(t, startRec)
		extID := startBody["SchemaExtensionId"].(string)

		cancelRec := doRequest(t, h, "CancelSchemaExtension", map[string]any{
			"DirectoryId":       dirID,
			"SchemaExtensionId": extID,
		})
		assert.Equal(t, http.StatusOK, cancelRec.Code)

		listRec := doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": dirID})
		body := respBody(t, listRec)
		exts, _ := body["SchemaExtensionsInfo"].([]any)
		require.Len(t, exts, 1)
		ext := exts[0].(map[string]any)
		assert.Equal(t, "CancelInProgress", ext["SchemaExtensionStatus"])
	})

	t.Run("CreateSnapshotBeforeSchemaExtension takes an Auto snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":                         dirID,
			"Description":                         "add attr",
			"SchemaExtensionBody":                 "dn: CN=foo",
			"CreateSnapshotBeforeSchemaExtension": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		listRec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dirID})
		require.Equal(t, http.StatusOK, listRec.Code)
		body := respBody(t, listRec)
		snapshots, _ := body["Snapshots"].([]any)
		require.Len(t, snapshots, 1, "CreateSnapshotBeforeSchemaExtension=true must take a snapshot")
		assert.Equal(t, "Auto", snapshots[0].(map[string]any)["Type"])
	})

	t.Run("CreateSnapshotBeforeSchemaExtension=false takes no snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":                         dirID,
			"Description":                         "add attr",
			"SchemaExtensionBody":                 "dn: CN=foo",
			"CreateSnapshotBeforeSchemaExtension": false,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		listRec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dirID})
		require.Equal(t, http.StatusOK, listRec.Code)
		body := respBody(t, listRec)
		snapshots, _ := body["Snapshots"].([]any)
		assert.Empty(t, snapshots)
	})

	t.Run("start on unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":         "d-0000000000",
			"Description":         "test",
			"SchemaExtensionBody": "dn: CN=foo",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestSchemaExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start list cancel cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Start
			rec1 := doRequest(t, h, "StartSchemaExtension", map[string]any{
				"DirectoryId":                         dirID,
				"Description":                         "test extension",
				"LdifContent":                         "dn: cn=test",
				"CreateSnapshotBeforeSchemaExtension": false,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			extID, _ := r1["SchemaExtensionId"].(string)
			assert.NotEmpty(t, extID)

			// List
			rec2 := doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			exts, _ := r2["SchemaExtensionsInfo"].([]any)
			assert.Len(t, exts, 1)

			// Cancel
			rec3 := doRequest(t, h, "CancelSchemaExtension", map[string]any{
				"DirectoryId":       dirID,
				"SchemaExtensionId": extID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}
