package directoryservice_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

func newTestHandler(t *testing.T) *directoryservice.Handler {
	t.Helper()

	return directoryservice.NewHandler(directoryservice.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *directoryservice.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "DirectoryService_20150416."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func TestDirectoryService_CreateDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "creates Simple AD and returns DirectoryId",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				id, ok := resp["DirectoryId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDirectory", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDirectoryService_CreateMicrosoftAD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "creates MicrosoftAD and returns DirectoryId",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Standard",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				id, ok := resp["DirectoryId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateMicrosoftAD", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDirectoryService_DeleteDirectory(t *testing.T) {
	t.Parallel()

	t.Run("deletes existing directory", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		rec := doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("unknown DirectoryId returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing DirectoryId returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteDirectory", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestDirectoryService_DescribeDirectories(t *testing.T) {
	t.Parallel()

	t.Run("lists all directories when no IDs given", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "a.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "b.example.com", "Password": "Admin1234!", "Size": "Large",
		})

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		dirs, ok := resp["DirectoryDescriptions"].([]any)
		require.True(t, ok)
		assert.Len(t, dirs, 2)
	})

	t.Run("returns specific directory by ID", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{
			"DirectoryIds": []string{dirID},
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		dirs, ok := resp["DirectoryDescriptions"].([]any)
		require.True(t, ok)
		require.Len(t, dirs, 1)
		dir := dirs[0].(map[string]any)
		assert.Equal(t, dirID, dir["DirectoryId"])
		assert.Equal(t, "corp.example.com", dir["Name"])
		assert.Equal(t, "SimpleAD", dir["Type"])
		assert.Equal(t, "Active", dir["Stage"])
	})

	t.Run("unknown DirectoryId returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DescribeDirectories", map[string]any{
			"DirectoryIds": []string{"d-0000000000"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty backend returns empty list", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DescribeDirectories", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		dirs, ok := resp["DirectoryDescriptions"].([]any)
		require.True(t, ok)
		assert.Empty(t, dirs)
	})
}

func TestDirectoryService_CreateAlias(t *testing.T) {
	t.Parallel()

	t.Run("creates alias for existing directory", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		rec := doRequest(t, h, "CreateAlias", map[string]any{
			"DirectoryId": dirID,
			"Alias":       "myalias",
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, dirID, resp["DirectoryId"])
		assert.Equal(t, "myalias", resp["Alias"])
	})

	t.Run("unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "CreateAlias", map[string]any{
			"DirectoryId": "d-0000000000",
			"Alias":       "myalias",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestDirectoryService_SSO(t *testing.T) {
	t.Parallel()

	createDir := func(h *directoryservice.Handler) string {
		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var resp map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		return resp["DirectoryId"].(string)
	}

	t.Run("enable SSO succeeds", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		rec := doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("disable SSO succeeds", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": dirID})
		rec := doRequest(t, h, "DisableSso", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("EnableSso unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DisableSso unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DisableSso", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestDirectoryService_GetDirectoryLimits(t *testing.T) {
	t.Parallel()

	t.Run("returns limits with zero counts on empty backend", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		limits, ok := resp["DirectoryLimits"].(map[string]any)
		require.True(t, ok)
		assert.EqualValues(t, 0, limits["CloudOnlyDirectoriesCurrentCount"])
		assert.False(t, limits["CloudOnlyDirectoriesLimitReached"].(bool))
	})

	t.Run("counts increment after creation", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		rec := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		limits := resp["DirectoryLimits"].(map[string]any)
		assert.EqualValues(t, 1, limits["CloudOnlyDirectoriesCurrentCount"])
	})
}

func TestDirectoryService_Snapshots(t *testing.T) {
	t.Parallel()

	createDir := func(h *directoryservice.Handler) string {
		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var resp map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		return resp["DirectoryId"].(string)
	}

	t.Run("CreateSnapshot returns SnapshotId", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		rec := doRequest(t, h, "CreateSnapshot", map[string]any{
			"DirectoryId": dirID,
			"Name":        "my-snapshot",
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		snapID, ok := resp["SnapshotId"].(string)
		require.True(t, ok)
		assert.NotEmpty(t, snapID)
	})

	t.Run("CreateSnapshot unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DeleteSnapshot removes snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
		var snapResp map[string]any
		require.NoError(t, json.Unmarshal(snapRec.Body.Bytes(), &snapResp))
		snapID := snapResp["SnapshotId"].(string)

		rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotId": snapID})
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify it's gone.
		descRec := doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotIds": []string{snapID}})
		assert.Equal(t, http.StatusOK, descRec.Code)
		var descResp map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
		snaps := descResp["Snapshots"].([]any)
		assert.Empty(t, snaps)
	})

	t.Run("DeleteSnapshot unknown returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotId": "s-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DescribeSnapshots filters by directory", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID1 := createDir(h)
		dirID2 := func() string {
			rec := doRequest(t, h, "CreateDirectory", map[string]any{
				"Name": "other.example.com", "Password": "Admin1234!", "Size": "Small",
			})
			var resp map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)

			return resp["DirectoryId"].(string)
		}()

		doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID1})
		doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID2})

		rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dirID1})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		snaps := resp["Snapshots"].([]any)
		assert.Len(t, snaps, 1)
	})

	t.Run("GetSnapshotLimits returns limits", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})

		rec := doRequest(t, h, "GetSnapshotLimits", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		limits := resp["SnapshotLimits"].(map[string]any)
		assert.EqualValues(t, 1, limits["ManualSnapshotsCurrentCount"])
	})

	t.Run("RestoreFromSnapshot succeeds on existing snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
		var snapResp map[string]any
		require.NoError(t, json.Unmarshal(snapRec.Body.Bytes(), &snapResp))
		snapID := snapResp["SnapshotId"].(string)

		rec := doRequest(t, h, "RestoreFromSnapshot", map[string]any{"SnapshotId": snapID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("RestoreFromSnapshot unknown returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "RestoreFromSnapshot", map[string]any{"SnapshotId": "s-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestDirectoryService_Tags(t *testing.T) {
	t.Parallel()

	createDir := func(h *directoryservice.Handler) string {
		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var resp map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		return resp["DirectoryId"].(string)
	}

	t.Run("AddTagsToResource and ListTagsForResource round-trip", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)

		addRec := doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "test"}},
		})
		assert.Equal(t, http.StatusOK, addRec.Code)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		assert.Equal(t, http.StatusOK, listRec.Code)

		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags, ok := listResp["Tags"].([]any)
		require.True(t, ok)
		require.Len(t, tags, 1)
		tag := tags[0].(map[string]any)
		assert.Equal(t, "env", tag["Key"])
		assert.Equal(t, "test", tag["Value"])
	})

	t.Run("RemoveTagsFromResource removes specified keys", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "env", "Value": "test"},
				{"Key": "team", "Value": "ops"},
			},
		})

		removeRec := doRequest(t, h, "RemoveTagsFromResource", map[string]any{
			"ResourceId": dirID,
			"TagKeys":    []string{"env"},
		})
		assert.Equal(t, http.StatusOK, removeRec.Code)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags := listResp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "team", tags[0].(map[string]any)["Key"])
	})

	t.Run("AddTagsToResource unknown resource returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": "d-0000000000",
			"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("tags from CreateDirectory are preserved", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name":     "corp.example.com",
			"Password": "Admin1234!",
			"Size":     "Small",
			"Tags":     []map[string]any{{"Key": "created", "Value": "yes"}},
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags := listResp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "created", tags[0].(map[string]any)["Key"])
	})
}

func TestDirectoryService_MicrosoftAD_DescribeReturnsType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
		"Name":     "corp.example.com",
		"Password": "Admin1234!",
		"Edition":  "Enterprise",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	dirID := createResp["DirectoryId"].(string)

	rec := doRequest(t, h, "DescribeDirectories", map[string]any{
		"DirectoryIds": []string{dirID},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["DirectoryDescriptions"].([]any)
	require.Len(t, dirs, 1)
	dir := dirs[0].(map[string]any)
	assert.Equal(t, "MicrosoftAD", dir["Type"])
	assert.Equal(t, "Enterprise", dir["Edition"])
}

func TestDirectoryService_DeleteDirectoryRemovesSnapshots(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateDirectory", map[string]any{
		"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	dirID := createResp["DirectoryId"].(string)

	doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
	doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

	descRec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dirID})
	assert.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	snaps := descResp["Snapshots"].([]any)
	assert.Empty(t, snaps)
}
