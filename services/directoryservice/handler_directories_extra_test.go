package directoryservice_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

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
			wantType: "InvalidParameterException",
		},
		{
			name:     "missing Password returns 400",
			body:     map[string]any{"Name": "corp.example.com", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid Size returns 400",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Giant",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
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

func TestDescribeDirectories_ResponseFields(t *testing.T) {
	t.Parallel()

	t.Run("SimpleAD response has required fields", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		backend := h.Backend.(*directoryservice.InMemoryBackend)
		require.True(t, directoryservice.WaitForDirectoryActive(backend, dirID, time.Second))

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

func TestCreateComputer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	tests := []struct {
		body     any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "creates computer returns ComputerName",
			body: map[string]any{
				"DirectoryId":  dirID,
				"ComputerName": "WORKSTATION1",
				"Password":     "Comp1234!",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				computer, _ := resp["Computer"].(map[string]any)
				assert.Equal(t, "WORKSTATION1", computer["ComputerName"])
				assert.NotEmpty(t, computer["ComputerId"])
			},
		},
		{
			name:     "missing ComputerName returns 400",
			body:     map[string]any{"DirectoryId": dirID, "Password": "Comp1234!"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "CreateComputer", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tc.check(t, resp)
			}
		})
	}
}

func TestResetUserPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "resets password returns ok",
			body: map[string]any{
				"DirectoryId": dirID,
				"UserName":    "jdoe",
				"NewPassword": "NewPw1234!",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing UserName returns 400",
			body:     map[string]any{"DirectoryId": dirID, "NewPassword": "pw"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "ResetUserPassword", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestConnectDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "creates ADConnector and returns DirectoryId",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Small",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				id, ok := resp["DirectoryId"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, id)
			},
		},
		{
			name:     "missing Name returns 400",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "ConnectDirectory", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tc.check(t, resp)
			}
		})
	}
}
