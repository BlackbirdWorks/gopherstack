package appconfig_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestExtensionParameterDynamicViaSDKClient proves types.Parameter.Dynamic
// (appconfig@v1.48.4 deserializers.go's Dynamic case, shared by
// Create/UpdateExtensionInput and Get/CreateExtensionOutput) is no longer
// silently discarded on input and never emitted on output.
func TestExtensionParameterDynamicViaSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	createOut, err := client.CreateExtension(t.Context(), &appconfigsdk.CreateExtensionInput{
		Name: aws.String("dynamic-param-ext"),
		Actions: map[string][]types.Action{
			"ON_DEPLOYMENT_START": {{Name: aws.String("act"), Uri: aws.String("arn:aws:sns:us-east-1:123456789012:t")}},
		},
		Parameters: map[string]types.Parameter{
			"myParam": {Dynamic: true, Required: false},
		},
	})
	require.NoError(t, err)
	require.Contains(t, createOut.Parameters, "myParam")
	assert.True(t, createOut.Parameters["myParam"].Dynamic)

	getOut, err := client.GetExtension(t.Context(), &appconfigsdk.GetExtensionInput{
		ExtensionIdentifier: createOut.Id,
	})
	require.NoError(t, err)
	require.Contains(t, getOut.Parameters, "myParam")
	assert.True(t, getOut.Parameters["myParam"].Dynamic)
}

func TestHandler_Extension_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name       string
		method     string
		path       string
		wantField  string
		wantValue  string
		body       []byte
		wantStatus int
	}{
		{
			name:       "list extensions empty",
			method:     http.MethodGet,
			path:       "/extensions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "create extension",
			method:     http.MethodPost,
			path:       "/extensions",
			body:       []byte(`{"Name":"my-ext","Description":"test extension"}`),
			wantStatus: http.StatusCreated,
			wantField:  "Name",
			wantValue:  "my-ext",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantValue, resp[tt.wantField])
			}
		})
	}
}

func TestHandler_Extension_GetDeleteByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"ext-to-delete"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))
	assert.NotEmpty(t, ext.ID)
	assert.Equal(t, "ext-to-delete", ext.Name)

	// Get by ID.
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by name.
	rec = doRequest(t, h, http.MethodGet, "/extensions/ext-to-delete", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete by ID.
	rec = doRequest(t, h, http.MethodDelete, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should now return 404.
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_Extension_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "get not found", method: http.MethodGet, path: "/extensions/nonexistent"},
		{name: "delete not found", method: http.MethodDelete, path: "/extensions/nonexistent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_Extension_ListPaginated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		body := []byte(`{"Name":"ext-` + strconv.Itoa(i) + `"}`)
		rec := doRequest(t, h, http.MethodPost, "/extensions", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodGet, "/extensions?max_results=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

func TestHandler_ExtensionAssociation_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an extension first.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"assoc-ext"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	// Create an application to use as resource.
	rec = doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"assoc-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// List associations empty.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Create association.
	resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID
	body := []byte(
		`{"ExtensionIdentifier":"` + ext.ID + `","ResourceIdentifier":"` + resourceID + `"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/extensionassociations", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var assoc appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))
	assert.NotEmpty(t, assoc.ID)
	assert.Equal(t, ext.Arn, assoc.ExtensionArn)

	// Get association.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List associations shows one.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, ok := listResp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)

	// Delete association.
	rec = doRequest(t, h, http.MethodDelete, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should now return 404.
	rec = doRequest(t, h, http.MethodGet, "/extensionassociations/"+assoc.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ExtensionAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{
			name:   "get not found",
			method: http.MethodGet,
			path:   "/extensionassociations/nonexistent",
		},
		{
			name:   "delete not found",
			method: http.MethodDelete,
			path:   "/extensionassociations/nonexistent",
		},
		{
			name:   "create with missing extension",
			method: http.MethodPost,
			path:   "/extensionassociations",
			body: []byte(`{"ExtensionIdentifier":"missing-ext",` +
				`"ResourceIdentifier":"arn:aws:appconfig:us-east-1:123456789012:application/abc"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_UpdateExtension(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension.
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/extensions",
		[]byte(`{"Name":"update-ext","Description":"original"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))
	assert.Equal(t, int32(1), ext.VersionNumber)

	// Update extension.
	rec = doRequest(t, h, http.MethodPatch, "/extensions/"+ext.ID,
		[]byte(`{"Description":"updated"}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "updated", updated.Description)
	assert.Equal(t, int32(2), updated.VersionNumber)
}

// TestHandler_UpdateExtension_OmittedDescriptionPreserved verifies that
// updating an extension's Actions without including Description leaves the
// existing description unchanged, matching real UpdateExtensionInput's
// optional *string Description member.
func TestHandler_UpdateExtension_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/extensions",
		[]byte(`{"Name":"update-ext2","Description":"keep-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	// Update Actions only; Description is omitted.
	rec = doRequest(t, h, http.MethodPatch, "/extensions/"+ext.ID,
		[]byte(`{"Actions":{"ON_DEPLOYMENT_START":[{"Name":"a","Uri":"lambda:1"}]}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must not be cleared")
}

func TestHandler_UpdateExtension_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/extensions/nonexistent",
		[]byte(`{"Description":"updated"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateExtensionAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create extension and association.
	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"update-assoc-ext"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/abc"
	assocBody := []byte(
		`{"ExtensionIdentifier":"` + ext.ID + `","ResourceIdentifier":"` + resourceID + `"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/extensionassociations", assocBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var assoc appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))

	// Update the association parameters.
	rec = doRequest(t, h, http.MethodPatch, "/extensionassociations/"+assoc.ID,
		[]byte(`{"Parameters":{"key":"value"}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ExtensionAssociation
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "value", updated.Parameters["key"])
}

func TestHandler_UpdateExtensionAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/extensionassociations/nonexistent",
		[]byte(`{"Parameters":{}}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateExtension_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "missing name returns 400",
			body:       []byte(`{"Description":"no name"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty name returns 400",
			body:       []byte(`{"Name":""}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate name returns 409",
			body:       []byte(`{"Name":"duplicate-ext"}`),
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusConflict {
				// Pre-create extension to force duplicate.
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/extensions",
					[]byte(`{"Name":"duplicate-ext"}`),
				)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/extensions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateExtensionAssociation_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "missing extension identifier returns 400",
			body: []byte(
				`{"ResourceIdentifier":"arn:aws:appconfig:us-east-1:123456789012:application/abc"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing resource identifier returns 400",
			body:       []byte(`{"ExtensionIdentifier":"my-ext"}`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/extensionassociations", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Extension_VersionQueryParams verifies the real wire-shape fix:
// GetExtension binds an optional "version_number" query param and
// DeleteExtension binds an optional "version" query param (see
// awsRestjson1_serializeOpHttpBindingsGetExtensionInput/
// ...DeleteExtensionInput), so an UpdateExtension-created new version must
// be independently addressable and independently deletable rather than the
// single mutable record this backend used before.
func TestHandler_Extension_VersionQueryParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/extensions",
		[]byte(`{"Name":"query-ver-ext","Description":"v1"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var v1 appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v1))

	rec = doRequest(t, h, http.MethodPatch, "/extensions/"+v1.ID,
		[]byte(`{"Description":"v2"}`))
	require.Equal(t, http.StatusOK, rec.Code)

	// Get version 1 explicitly: must still return the original description.
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+v1.ID+"?version_number=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var gotV1 appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gotV1))
	assert.Equal(t, "v1", gotV1.Description)
	assert.Equal(t, int32(1), gotV1.VersionNumber)

	// Get without a version: must return the highest (v2).
	rec = doRequest(t, h, http.MethodGet, "/extensions/"+v1.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var gotLatest appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gotLatest))
	assert.Equal(t, int32(2), gotLatest.VersionNumber)

	// Delete version 1 explicitly via the "version" query param; version 2
	// must remain gettable.
	rec = doRequest(t, h, http.MethodDelete, "/extensions/"+v1.ID+"?version=1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/extensions/"+v1.ID+"?version_number=1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/extensions/"+v1.ID+"?version_number=2", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_DeleteExtension_ConflictWhenAssociated verifies DeleteExtension
// returns 400 Bad Request for a version still referenced by an
// ExtensionAssociation, matching real AWS's requirement to remove
// associations before deleting the extension version they use. DeleteExtension
// models only BadRequestException, InternalServerException and
// ResourceNotFoundException (appconfig@v1.48.4 deserializers.go:2369) -- it
// does not model ConflictException.
func TestHandler_DeleteExtension_ConflictWhenAssociated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/extensions", []byte(`{"Name":"conflict-del-ext"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var ext appconfig.Extension
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))

	assocBody := []byte(
		`{"ExtensionIdentifier":"` + ext.ID +
			`","ResourceIdentifier":"arn:aws:appconfig:us-east-1:123456789012:application/app-1"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/extensionassociations", assocBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/extensions/"+ext.ID, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListExtensions_NameFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two extensions.
	for _, name := range []string{"alpha-ext", "beta-ext"} {
		body := []byte(`{"Name":"` + name + `"}`)
		rec := doRequest(t, h, http.MethodPost, "/extensions", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// Filter by name.
	rec := doRequest(t, h, http.MethodGet, "/extensions?name=alpha-ext", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)

	// Filter by non-matching name.
	rec = doRequest(t, h, http.MethodGet, "/extensions?name=nonexistent", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok = resp["Items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

// TestHandler_CreateExtension_TagsAppliedInline verifies that Tags sent
// inline on CreateExtensionInput are visible via ListTagsForResource
// immediately after creation -- previously CreateExtension's handler never
// bound or forwarded the Tags field at all, so tags set at create time
// silently vanished (bd gopherstack-lcan).
func TestHandler_CreateExtension_TagsAppliedInline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "tags_applied_at_create",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "no_tags_is_not_an_error",
			tags: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body, err := json.Marshal(map[string]any{
				"Name": "tagged-ext-" + tt.name,
				"Tags": tt.tags,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/extensions", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var ext appconfig.Extension
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ext))
			require.NotEmpty(t, ext.Arn)

			tagsRec := doRequest(t, h, http.MethodGet, "/tags/"+ext.Arn, nil)
			require.Equal(t, http.StatusOK, tagsRec.Code)

			var tagsResp struct {
				Tags map[string]string `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))

			if len(tt.tags) == 0 {
				assert.Empty(t, tagsResp.Tags)
			} else {
				assert.Equal(t, tt.tags, tagsResp.Tags)
			}
		})
	}
}

// TestHandler_CreateExtensionAssociation_TagsAppliedInline verifies that
// Tags sent inline on CreateExtensionAssociationInput are visible via
// ListTagsForResource immediately after creation -- previously
// CreateExtensionAssociation's handler never bound or forwarded the Tags
// field at all, so tags set at create time silently vanished (bd
// gopherstack-lcan).
func TestHandler_CreateExtensionAssociation_TagsAppliedInline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "tags_applied_at_create",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "no_tags_is_not_an_error",
			tags: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			extRec := doRequest(t, h, http.MethodPost, "/extensions",
				[]byte(`{"Name":"tagged-assoc-ext-`+tt.name+`"}`))
			require.Equal(t, http.StatusCreated, extRec.Code)

			var ext appconfig.Extension
			require.NoError(t, json.Unmarshal(extRec.Body.Bytes(), &ext))

			resourceID := "arn:aws:appconfig:us-east-1:123456789012:application/tagged-assoc-" + tt.name
			body, err := json.Marshal(map[string]any{
				"ExtensionIdentifier": ext.ID,
				"ResourceIdentifier":  resourceID,
				"Tags":                tt.tags,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/extensionassociations", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var assoc appconfig.ExtensionAssociation
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))
			require.NotEmpty(t, assoc.Arn)

			tagsRec := doRequest(t, h, http.MethodGet, "/tags/"+assoc.Arn, nil)
			require.Equal(t, http.StatusOK, tagsRec.Code)

			var tagsResp struct {
				Tags map[string]string `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))

			if len(tt.tags) == 0 {
				assert.Empty(t, tagsResp.Tags)
			} else {
				assert.Equal(t, tt.tags, tagsResp.Tags)
			}
		})
	}
}
