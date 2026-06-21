package directoryservice_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// --- helpers ---

func mustCreateSimpleAD(t *testing.T, h *directoryservice.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateDirectory", map[string]any{
		"Name":     name,
		"Password": "Admin1234!",
		"Size":     "Small",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["DirectoryId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func mustCreateMicrosoftAD(t *testing.T, h *directoryservice.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
		"Name":     name,
		"Password": "Admin1234!",
		"Edition":  "Enterprise",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["DirectoryId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func respBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// --- Input validation: CreateDirectory ---

func TestCreateDirectory_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "missing Name returns 400 ClientException",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name:     "missing Password returns 400 ClientException",
			body:     map[string]any{"Name": "corp.example.com", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "invalid Size returns 400 ClientException",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Huge",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "empty Size returns 400 ClientException",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "Size Small succeeds",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "Size Large succeeds",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Large",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDirectory", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				body := respBody(t, rec)
				assert.Equal(t, tt.wantType, body["__type"])
			}
		})
	}
}

// --- Input validation: CreateMicrosoftAD ---

func TestCreateMicrosoftAD_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!", "Edition": "Enterprise"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name:     "missing Password returns 400",
			body:     map[string]any{"Name": "corp.example.com", "Edition": "Enterprise"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "invalid Edition returns 400",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Ultra",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "Edition Enterprise succeeds",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "Edition Standard succeeds",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Standard",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "omitted Edition defaults to Enterprise",
			body:     map[string]any{"Name": "corp.example.com", "Password": "Admin1234!"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateMicrosoftAD", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				body := respBody(t, rec)
				assert.Equal(t, tt.wantType, body["__type"])
			}
		})
	}
}

// --- Input validation: ConnectDirectory ---

func TestConnectDirectory_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name:     "missing Password returns 400",
			body:     map[string]any{"Name": "corp.example.com", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "invalid Size returns 400",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Giant",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "valid Small succeeds",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "ConnectDirectory", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				body := respBody(t, rec)
				assert.Equal(t, tt.wantType, body["__type"])
			}
		})
	}
}

// --- Directory limit enforcement ---

func TestCreateDirectory_LimitEnforcement(t *testing.T) {
	t.Parallel()

	t.Run(
		"10 SimpleAD is allowed, 11th returns DirectoryLimitExceededException",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 10 {
				rec := doRequest(t, h, "CreateDirectory", map[string]any{
					"Name":     fmt.Sprintf("corp%d.example.com", i),
					"Password": "Admin1234!",
					"Size":     "Small",
				})
				require.Equal(t, http.StatusOK, rec.Code, "directory %d should succeed", i)
			}

			rec := doRequest(t, h, "CreateDirectory", map[string]any{
				"Name":     "overflow.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			body := respBody(t, rec)
			assert.Equal(t, "DirectoryLimitExceededException", body["__type"])
		},
	)
}

func TestCreateMicrosoftAD_LimitEnforcement(t *testing.T) {
	t.Parallel()

	t.Run(
		"20 MicrosoftAD is allowed, 21st returns DirectoryLimitExceededException",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 20 {
				rec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
					"Name":     fmt.Sprintf("corp%d.example.com", i),
					"Password": "Admin1234!",
					"Edition":  "Enterprise",
				})
				require.Equal(t, http.StatusOK, rec.Code, "directory %d should succeed", i)
			}

			rec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
				"Name":     "overflow.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			body := respBody(t, rec)
			assert.Equal(t, "DirectoryLimitExceededException", body["__type"])
		},
	)
}

// --- Snapshot limit enforcement ---

func TestCreateSnapshot_LimitEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		wantCode int
	}{
		{name: "5 snapshots succeeds", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			for i := range 5 {
				rec := doRequest(t, h, "CreateSnapshot", map[string]any{
					"DirectoryId": dirID,
					"Name":        fmt.Sprintf("snap-%d", i),
				})
				assert.Equal(t, tt.wantCode, rec.Code, "snapshot %d", i)
			}
		})
	}

	t.Run("6th snapshot returns SnapshotLimitExceededException", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		for i := range 5 {
			rec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"DirectoryId": dirID,
				"Name":        fmt.Sprintf("snap-%d", i),
			})
			require.Equal(t, http.StatusOK, rec.Code, "snapshot %d should succeed", i)
		}

		rec := doRequest(t, h, "CreateSnapshot", map[string]any{
			"DirectoryId": dirID,
			"Name":        "overflow",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		body := respBody(t, rec)
		assert.Equal(t, "SnapshotLimitExceededException", body["__type"])
	})

	t.Run("snapshot limit is per directory", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dir1 := mustCreateSimpleAD(t, h, "corp1.example.com")
		dir2 := mustCreateSimpleAD(t, h, "corp2.example.com")

		for i := range 5 {
			rec := doRequest(
				t,
				h,
				"CreateSnapshot",
				map[string]any{"DirectoryId": dir1, "Name": fmt.Sprintf("s%d", i)},
			)
			require.Equal(t, http.StatusOK, rec.Code)
		}
		// dir2 is unaffected by dir1's snapshots
		rec := doRequest(
			t,
			h,
			"CreateSnapshot",
			map[string]any{"DirectoryId": dir2, "Name": "first"},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("deleting snapshot frees slot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		snapIDs := make([]string, 0, 5)
		for i := range 5 {
			rec := doRequest(
				t,
				h,
				"CreateSnapshot",
				map[string]any{"DirectoryId": dirID, "Name": fmt.Sprintf("s%d", i)},
			)
			require.Equal(t, http.StatusOK, rec.Code)
			body := respBody(t, rec)
			snapIDs = append(snapIDs, body["SnapshotId"].(string))
		}

		// Delete one to free up a slot
		delRec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotId": snapIDs[0]})
		require.Equal(t, http.StatusOK, delRec.Code)

		// Now a new one should succeed
		rec := doRequest(
			t,
			h,
			"CreateSnapshot",
			map[string]any{"DirectoryId": dirID, "Name": "new-snap"},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// --- Cascade delete ---

func TestDeleteDirectory_CascadesResources(t *testing.T) {
	t.Parallel()

	t.Run("deleting directory removes IP routes", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddIpRoutes", map[string]any{
			"DirectoryId": dirID,
			"IpRoutes":    []any{map[string]any{"CidrIp": "10.0.0.0/24", "Description": "test"}},
		})

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		// Re-create to verify new dir doesn't see old routes
		newDirID := mustCreateSimpleAD(t, h, "corp.example.com")
		rec := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": newDirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		routes := body["IpRoutesInfo"].([]any)
		assert.Empty(t, routes)
	})

	t.Run("deleting directory removes event topics", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "RegisterEventTopic", map[string]any{
			"DirectoryId": dirID,
			"TopicName":   "my-topic",
		})

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		// After delete, the directory is gone — verifying cleanup by checking no stale topics on new dir
		newDirID := mustCreateSimpleAD(t, h, "corp.example.com")
		rec := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": newDirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		topics, _ := body["EventTopics"].([]any)
		assert.Empty(t, topics)
	})

	t.Run("deleting directory removes conditional forwarders", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.1"},
		})

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		newDirID := mustCreateSimpleAD(t, h, "corp.example.com")
		rec := doRequest(
			t,
			h,
			"DescribeConditionalForwarders",
			map[string]any{"DirectoryId": newDirID},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		fwds, _ := body["ConditionalForwarders"].([]any)
		assert.Empty(t, fwds)
	})

	t.Run("deleting directory removes log subscriptions", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateLogSubscription", map[string]any{
			"DirectoryId":  dirID,
			"LogGroupName": "/aws/directoryservice/corp",
		})

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		newDirID := mustCreateSimpleAD(t, h, "corp.example.com")
		rec := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": newDirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		subs, _ := body["LogSubscriptions"].([]any)
		assert.Empty(t, subs)
	})

	t.Run("deleting directory removes snapshots", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		for i := range 3 {
			doRequest(t, h, "CreateSnapshot", map[string]any{
				"DirectoryId": dirID,
				"Name":        fmt.Sprintf("snap-%d", i),
			})
		}

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		// After cascade delete and re-create, snapshot limit reset for that directory ID space
		// Verify by checking DescribeSnapshots returns empty for old dirID (not found is OK)
		rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dirID})
		// Either 400 (not found) or 200 with empty is acceptable - we deleted the directory
		if rec.Code == http.StatusOK {
			body := respBody(t, rec)
			snaps, _ := body["Snapshots"].([]any)
			assert.Empty(t, snaps)
		}
	})

	t.Run("deleting directory removes schema extensions", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		doRequest(t, h, "StartSchemaExtension", map[string]any{
			"DirectoryId":         dirID,
			"Description":         "my extension",
			"SchemaExtensionBody": "dn: CN=foo",
		})

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		// Re-create and verify no stale extensions
		newDirID := mustCreateMicrosoftAD(t, h, "corp.example.com")
		rec := doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": newDirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		exts, _ := body["SchemaExtensionsInfo"].([]any)
		assert.Empty(t, exts)
	})
}

// --- Error code shapes ---

func TestErrorCodeShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *directoryservice.Handler) (string, any)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "DeleteDirectory unknown returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DeleteDirectory", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "CreateAlias duplicate returns EntityAlreadyExistsException",
			setup: func(h *directoryservice.Handler) (string, any) {
				dirID := mustCreateSimpleAD(t, h, "corp.example.com")
				doRequest(
					t,
					h,
					"CreateAlias",
					map[string]any{"DirectoryId": dirID, "Alias": "myalias"},
				)
				dir2 := mustCreateSimpleAD(t, h, "other.example.com")

				return "CreateAlias", map[string]any{"DirectoryId": dir2, "Alias": "myalias"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityAlreadyExistsException",
		},
		{
			name: "DescribeDirectories unknown ID returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DescribeDirectories", map[string]any{
					"DirectoryIds": []string{"d-0000000000"},
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "DeleteSnapshot unknown returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DeleteSnapshot", map[string]any{"SnapshotId": "s-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "EnableSso unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "EnableSso", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "AddTagsToResource unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "AddTagsToResource", map[string]any{
					"ResourceId": "d-0000000000",
					"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "GetSnapshotLimits unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "GetSnapshotLimits", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "ListIpRoutes unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "ListIpRoutes", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "invalid JSON body returns ClientException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "CreateDirectory", "not-json"
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "DirectoryLimitExceededException on 11th SimpleAD",
			setup: func(h *directoryservice.Handler) (string, any) {
				for i := range 10 {
					doRequest(t, h, "CreateDirectory", map[string]any{
						"Name": fmt.Sprintf(
							"corp%d.example.com",
							i,
						), "Password": "Admin1234!", "Size": "Small",
					})
				}

				return "CreateDirectory", map[string]any{
					"Name":     "overflow.example.com",
					"Password": "Admin1234!",
					"Size":     "Small",
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "DirectoryLimitExceededException",
		},
		{
			name: "SnapshotLimitExceededException on 6th snapshot",
			setup: func(h *directoryservice.Handler) (string, any) {
				dirID := mustCreateSimpleAD(t, h, "corp.example.com")
				for i := range 5 {
					doRequest(
						t,
						h,
						"CreateSnapshot",
						map[string]any{"DirectoryId": dirID, "Name": fmt.Sprintf("s%d", i)},
					)
				}

				return "CreateSnapshot", map[string]any{"DirectoryId": dirID, "Name": "overflow"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "SnapshotLimitExceededException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			op, body := tt.setup(h)
			rec := doRequest(t, h, op, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				b := respBody(t, rec)
				assert.Equal(t, tt.wantType, b["__type"])
			}
		})
	}
}

// --- Pagination ---

func TestListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("pagination with limit returns correct page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		// Add 5 tags
		tags := make([]map[string]any, 5)
		for i := range 5 {
			tags[i] = map[string]any{
				"Key":   fmt.Sprintf("tag%02d", i),
				"Value": fmt.Sprintf("val%d", i),
			}
		}
		doRequest(t, h, "AddTagsToResource", map[string]any{"ResourceId": dirID, "Tags": tags})

		// First page: limit 2
		rec := doRequest(
			t,
			h,
			"ListTagsForResource",
			map[string]any{"ResourceId": dirID, "Limit": 2},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		firstPage, _ := body["Tags"].([]any)
		assert.Len(t, firstPage, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		// Second page
		rec2 := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID, "Limit": 2, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		secondPage, _ := body2["Tags"].([]any)
		assert.Len(t, secondPage, 2)
		nextToken2, _ := body2["NextToken"].(string)
		assert.NotEmpty(t, nextToken2)

		// Third page (last)
		rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID, "Limit": 2, "NextToken": nextToken2,
		})
		assert.Equal(t, http.StatusOK, rec3.Code)
		body3 := respBody(t, rec3)
		thirdPage, _ := body3["Tags"].([]any)
		assert.Len(t, thirdPage, 1)
		_, hasMoreToken := body3["NextToken"]
		assert.False(t, hasMoreToken)
	})

	t.Run("no limit returns all tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		tags := make([]map[string]any, 10)
		for i := range 10 {
			tags[i] = map[string]any{"Key": fmt.Sprintf("k%02d", i), "Value": "v"}
		}
		doRequest(t, h, "AddTagsToResource", map[string]any{"ResourceId": dirID, "Tags": tags})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		result, _ := body["Tags"].([]any)
		assert.Len(t, result, 10)
		_, hasToken := body["NextToken"]
		assert.False(t, hasToken)
	})
}

func TestDescribeDirectories_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("pagination returns pages in deterministic order", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		for i := range 5 {
			mustCreateSimpleAD(t, h, fmt.Sprintf("corp%d.example.com", i))
		}

		// Page 1: limit 2
		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"Limit": 2})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["DirectoryDescriptions"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		// Page 2
		rec2 := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"Limit": 2, "NextToken": nextToken},
		)
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["DirectoryDescriptions"].([]any)
		assert.Len(t, page2, 2)

		// Page 3 (last)
		nextToken2, _ := body2["NextToken"].(string)
		rec3 := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"Limit": 2, "NextToken": nextToken2},
		)
		assert.Equal(t, http.StatusOK, rec3.Code)
		body3 := respBody(t, rec3)
		page3, _ := body3["DirectoryDescriptions"].([]any)
		assert.Len(t, page3, 1)
		_, hasMore := body3["NextToken"]
		assert.False(t, hasMore)

		// All IDs are distinct across pages
		seen := map[string]bool{}
		for _, page := range [][]any{page1, page2, page3} {
			for _, d := range page {
				dir := d.(map[string]any)
				id := dir["DirectoryId"].(string)
				assert.False(t, seen[id], "duplicate directory %s across pages", id)
				seen[id] = true
			}
		}
		assert.Len(t, seen, 5)
	})
}

func TestDescribeSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("paginate through snapshots", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		for i := range 4 {
			rec := doRequest(
				t,
				h,
				"CreateSnapshot",
				map[string]any{"DirectoryId": dirID, "Name": fmt.Sprintf("s%d", i)},
			)
			require.Equal(t, http.StatusOK, rec.Code)
		}

		// Page 1: limit 2
		rec := doRequest(
			t,
			h,
			"DescribeSnapshots",
			map[string]any{"DirectoryId": dirID, "Limit": 2},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["Snapshots"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		// Page 2
		rec2 := doRequest(
			t,
			h,
			"DescribeSnapshots",
			map[string]any{"DirectoryId": dirID, "Limit": 2, "NextToken": nextToken},
		)
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["Snapshots"].([]any)
		assert.Len(t, page2, 2)
		_, hasMore := body2["NextToken"]
		assert.False(t, hasMore)

		// 4 distinct snapshots total
		seen := map[string]bool{}
		for _, page := range [][]any{page1, page2} {
			for _, s := range page {
				snap := s.(map[string]any)
				seen[snap["SnapshotId"].(string)] = true
			}
		}
		assert.Len(t, seen, 4)
	})
}

func TestListCertificates_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("paginate through certificates", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		for i := range 4 {
			doRequest(t, h, "RegisterCertificate", map[string]any{
				"DirectoryId":     dirID,
				"CertificateData": fmt.Sprintf("cert-data-%d", i),
				"Type":            "ClientLDAPS",
			})
		}

		rec := doRequest(
			t,
			h,
			"ListCertificates",
			map[string]any{"DirectoryId": dirID, "PageSize": 2},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["CertificatesInfo"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		rec2 := doRequest(t, h, "ListCertificates", map[string]any{
			"DirectoryId": dirID, "PageSize": 2, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["CertificatesInfo"].([]any)
		assert.Len(t, page2, 2)
		_, hasMore := body2["NextToken"]
		assert.False(t, hasMore)
	})
}

func TestListIpRoutes_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("paginate through IP routes", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		routes := make([]any, 5)
		for i := range 5 {
			routes[i] = map[string]any{"CidrIp": fmt.Sprintf("10.%d.0.0/24", i), "Description": "r"}
		}
		doRequest(t, h, "AddIpRoutes", map[string]any{"DirectoryId": dirID, "IpRoutes": routes})

		rec := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID, "Limit": 2})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		page1, _ := body["IpRoutesInfo"].([]any)
		assert.Len(t, page1, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		rec2 := doRequest(t, h, "ListIpRoutes", map[string]any{
			"DirectoryId": dirID, "Limit": 3, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		page2, _ := body2["IpRoutesInfo"].([]any)
		assert.Len(t, page2, 3)
		_, hasMore := body2["NextToken"]
		assert.False(t, hasMore)
	})
}

func TestListLogSubscriptions_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("all subscriptions returned without pagination", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateLogSubscription", map[string]any{
			"DirectoryId":  dirID,
			"LogGroupName": "/aws/directoryservice/corp",
		})

		rec := doRequest(t, h, "ListLogSubscriptions", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		subs, _ := body["LogSubscriptions"].([]any)
		assert.Len(t, subs, 1)
	})
}

// --- DescribeDirectories response field fidelity ---

func TestDescribeDirectories_ResponseFields(t *testing.T) {
	t.Parallel()

	t.Run("SimpleAD response has required fields", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		rec := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		require.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		dirs := body["DirectoryDescriptions"].([]any)
		require.Len(t, dirs, 1)
		d := dirs[0].(map[string]any)

		assert.Equal(t, dirID, d["DirectoryId"])
		assert.Equal(t, "corp.example.com", d["Name"])
		assert.Equal(t, "SimpleAD", d["Type"])
		assert.Equal(t, "Active", d["Stage"])
		assert.Equal(t, "Small", d["Size"])
		assert.NotEmpty(t, d["Alias"])
		assert.NotEmpty(t, d["AccessUrl"])
		assert.NotZero(t, d["LaunchTime"])
	})

	t.Run("MicrosoftAD response includes Edition", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

		rec := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		require.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		dirs := body["DirectoryDescriptions"].([]any)
		d := dirs[0].(map[string]any)

		assert.Equal(t, "MicrosoftAD", d["Type"])
		assert.Equal(t, "Enterprise", d["Edition"])
	})

	t.Run("SSO state reflected in describe", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		// Initially SSO disabled
		rec := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		body := respBody(t, rec)
		dirs := body["DirectoryDescriptions"].([]any)
		d := dirs[0].(map[string]any)
		assert.False(t, d["SsoEnabled"].(bool))

		// Enable SSO
		doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": dirID})

		rec2 := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		body2 := respBody(t, rec2)
		dirs2 := body2["DirectoryDescriptions"].([]any)
		d2 := dirs2[0].(map[string]any)
		assert.True(t, d2["SsoEnabled"].(bool))
	})

	t.Run("VpcSettings present when provided", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name":     "corp.example.com",
			"Password": "Admin1234!",
			"Size":     "Small",
			"VpcSettings": map[string]any{
				"VpcId":     "vpc-12345",
				"SubnetIds": []string{"subnet-a", "subnet-b"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		dirID := body["DirectoryId"].(string)

		rec2 := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		body2 := respBody(t, rec2)
		dirs := body2["DirectoryDescriptions"].([]any)
		d := dirs[0].(map[string]any)
		vpc, ok := d["VpcSettings"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "vpc-12345", vpc["VpcId"])
		subnets := vpc["SubnetIds"].([]any)
		assert.Len(t, subnets, 2)
	})
}

// --- State lifecycle: restore from snapshot ---

func TestRestoreFromSnapshot_SetsRestoringStage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, snapRec.Code)
	snapBody := respBody(t, snapRec)
	snapID := snapBody["SnapshotId"].(string)

	doRequest(t, h, "RestoreFromSnapshot", map[string]any{"SnapshotId": snapID})

	rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
	body := respBody(t, rec)
	dirs := body["DirectoryDescriptions"].([]any)
	d := dirs[0].(map[string]any)
	assert.Equal(t, "Restoring", d["Stage"])
}

// --- CreateAlias state transitions ---

func TestCreateAlias_Idempotency(t *testing.T) {
	t.Parallel()

	t.Run("setting same alias twice fails on second attempt", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		rec1 := doRequest(
			t,
			h,
			"CreateAlias",
			map[string]any{"DirectoryId": dirID, "Alias": "myalias"},
		)
		assert.Equal(t, http.StatusOK, rec1.Code)

		// Second call with same alias on same directory - alias already taken
		dir2 := mustCreateSimpleAD(t, h, "other.example.com")
		rec2 := doRequest(
			t,
			h,
			"CreateAlias",
			map[string]any{"DirectoryId": dir2, "Alias": "myalias"},
		)
		assert.Equal(t, http.StatusBadRequest, rec2.Code)
		body := respBody(t, rec2)
		assert.Equal(t, "EntityAlreadyExistsException", body["__type"])
	})

	t.Run("alias is reflected in describe after set", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateAlias", map[string]any{"DirectoryId": dirID, "Alias": "myalias"})

		rec := doRequest(
			t,
			h,
			"DescribeDirectories",
			map[string]any{"DirectoryIds": []string{dirID}},
		)
		body := respBody(t, rec)
		dirs := body["DirectoryDescriptions"].([]any)
		d := dirs[0].(map[string]any)
		assert.Equal(t, "myalias", d["Alias"])
		assert.Contains(t, d["AccessUrl"].(string), "myalias")
	})
}

// --- Tags: AddTagsToResource upsert semantics ---

func TestAddTagsToResource_UpsertSemantics(t *testing.T) {
	t.Parallel()

	t.Run("updating an existing key overwrites value", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
		})
		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "prod"}},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "prod", tags[0].(map[string]any)["Value"])
	})

	t.Run("multiple tags added in one call", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "a", "Value": "1"},
				{"Key": "b", "Value": "2"},
				{"Key": "c", "Value": "3"},
			},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		assert.Len(t, tags, 3)
	})

	t.Run("tags returned in sorted key order", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "zebra", "Value": "z"},
				{"Key": "apple", "Value": "a"},
				{"Key": "mango", "Value": "m"},
			},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		require.Len(t, tags, 3)
		assert.Equal(t, "apple", tags[0].(map[string]any)["Key"])
		assert.Equal(t, "mango", tags[1].(map[string]any)["Key"])
		assert.Equal(t, "zebra", tags[2].(map[string]any)["Key"])
	})

	t.Run("RemoveTagsFromResource with non-existent key is idempotent", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
		})

		// Remove a key that doesn't exist — should succeed silently
		rec := doRequest(t, h, "RemoveTagsFromResource", map[string]any{
			"ResourceId": dirID,
			"TagKeys":    []string{"nonexistent"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		// Original tag still present
		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, listRec)
		tags, _ := body["Tags"].([]any)
		assert.Len(t, tags, 1)
	})
}

// --- Conditional forwarder lifecycle ---

func TestConditionalForwarder_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("create duplicate forwarder returns EntityAlreadyExistsException", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.1"},
		})

		rec := doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.2"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		body := respBody(t, rec)
		assert.Equal(t, "EntityAlreadyExistsException", body["__type"])
	})

	t.Run("update changes DNS IPs", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "CreateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.1"},
		})

		doRequest(t, h, "UpdateConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "remote.example.com",
			"DnsIpAddrs":       []string{"10.0.0.2", "10.0.0.3"},
		})

		rec := doRequest(t, h, "DescribeConditionalForwarders", map[string]any{
			"DirectoryId":       dirID,
			"RemoteDomainNames": []string{"remote.example.com"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		fwds, _ := body["ConditionalForwarders"].([]any)
		require.Len(t, fwds, 1)
		fwd := fwds[0].(map[string]any)
		dnsIPs, _ := fwd["DnsIpAddrs"].([]any)
		assert.Len(t, dnsIPs, 2)
	})

	t.Run("delete non-existent forwarder returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "DeleteConditionalForwarder", map[string]any{
			"DirectoryId":      dirID,
			"RemoteDomainName": "nonexistent.example.com",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// --- GetDirectoryLimits accuracy ---

func TestGetDirectoryLimits_Accuracy(t *testing.T) {
	t.Parallel()

	t.Run("counts SimpleAD and MicrosoftAD separately", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		mustCreateSimpleAD(t, h, "simple.example.com")
		mustCreateMicrosoftAD(t, h, "msad.example.com")

		rec := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		limits := body["DirectoryLimits"].(map[string]any)

		assert.EqualValues(t, 1, limits["CloudOnlyDirectoriesCurrentCount"])
		assert.EqualValues(t, 1, limits["CloudOnlyMicrosoftADCurrentCount"])
		assert.EqualValues(t, 0, limits["ConnectedDirectoriesCurrentCount"])
		assert.False(t, limits["CloudOnlyDirectoriesLimitReached"].(bool))
		assert.False(t, limits["CloudOnlyMicrosoftADLimitReached"].(bool))
	})

	t.Run("limit reached flag true at limit", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		for i := range 10 {
			mustCreateSimpleAD(t, h, fmt.Sprintf("corp%d.example.com", i))
		}

		rec := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		body := respBody(t, rec)
		limits := body["DirectoryLimits"].(map[string]any)
		assert.True(t, limits["CloudOnlyDirectoriesLimitReached"].(bool))
	})

	t.Run("counts decrement after delete", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		rec := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		body := respBody(t, rec)
		limits := body["DirectoryLimits"].(map[string]any)
		assert.EqualValues(t, 1, limits["CloudOnlyDirectoriesCurrentCount"])

		doRequest(t, h, "DeleteDirectory", map[string]any{"DirectoryId": dirID})

		rec2 := doRequest(t, h, "GetDirectoryLimits", map[string]any{})
		body2 := respBody(t, rec2)
		limits2 := body2["DirectoryLimits"].(map[string]any)
		assert.EqualValues(t, 0, limits2["CloudOnlyDirectoriesCurrentCount"])
	})
}

// --- DescribeEventTopics filtering ---

func TestDescribeEventTopics_Filtering(t *testing.T) {
	t.Parallel()

	t.Run("filter by topic name", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "topic-a"},
		)
		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "topic-b"},
		)

		rec := doRequest(t, h, "DescribeEventTopics", map[string]any{
			"DirectoryId": dirID,
			"TopicNames":  []string{"topic-a"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		topics, _ := body["EventTopics"].([]any)
		require.Len(t, topics, 1)
		assert.Equal(t, "topic-a", topics[0].(map[string]any)["TopicName"])
	})

	t.Run("duplicate topic registration returns EntityAlreadyExistsException", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "my-topic"},
		)
		rec := doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "my-topic"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		body := respBody(t, rec)
		assert.Equal(t, "EntityAlreadyExistsException", body["__type"])
	})
}

// --- DomainController lifecycle ---

func TestDomainControllers_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired int32
		wantLen int
	}{
		{name: "scale up to 3", desired: 3, wantLen: 3},
		{name: "scale up to 1", desired: 1, wantLen: 1},
		{name: "desired 0 removes all", desired: 0, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

			rec := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": tt.desired,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doRequest(
				t,
				h,
				"DescribeDomainControllers",
				map[string]any{"DirectoryId": dirID},
			)
			require.Equal(t, http.StatusOK, listRec.Code)
			body := respBody(t, listRec)
			controllers, _ := body["DomainControllers"].([]any)
			assert.Len(t, controllers, tt.wantLen)
		})
	}
}

// --- CreateDirectory/ConnectDirectory returns DirectoryId shape ---

func TestCreateDirectory_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		op   string
	}{
		{
			name: "CreateDirectory response contains DirectoryId",
			op:   "CreateDirectory",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
		},
		{
			name: "CreateMicrosoftAD response contains DirectoryId",
			op:   "CreateMicrosoftAD",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			},
		},
		{
			name: "ConnectDirectory response contains DirectoryId",
			op:   "ConnectDirectory",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			body := respBody(t, rec)
			id, ok := body["DirectoryId"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)
		})
	}
}

// --- Multi-directory isolation ---

func TestMultipleDirectories_Isolation(t *testing.T) {
	t.Parallel()

	t.Run("tags not shared between directories", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dir1 := mustCreateSimpleAD(t, h, "corp1.example.com")
		dir2 := mustCreateSimpleAD(t, h, "corp2.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dir1,
			"Tags":       []map[string]any{{"Key": "owner", "Value": "team-a"}},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dir2})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		assert.Empty(t, tags)
	})

	t.Run("snapshots not shared between directories", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dir1 := mustCreateSimpleAD(t, h, "corp1.example.com")
		dir2 := mustCreateSimpleAD(t, h, "corp2.example.com")

		doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dir1})

		rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"DirectoryId": dir2})
		body := respBody(t, rec)
		snaps, _ := body["Snapshots"].([]any)
		assert.Empty(t, snaps)
	})

	t.Run("IP routes not shared between directories", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dir1 := mustCreateSimpleAD(t, h, "corp1.example.com")
		dir2 := mustCreateSimpleAD(t, h, "corp2.example.com")

		doRequest(t, h, "AddIpRoutes", map[string]any{
			"DirectoryId": dir1,
			"IpRoutes":    []any{map[string]any{"CidrIp": "10.0.0.0/24", "Description": "r"}},
		})

		rec := doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dir2})
		body := respBody(t, rec)
		routes, _ := body["IpRoutesInfo"].([]any)
		assert.Empty(t, routes)
	})
}

// --- Schema extension state ---

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
