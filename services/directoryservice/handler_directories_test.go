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
		backend := h.Backend.(*directoryservice.InMemoryBackend)
		require.True(t, directoryservice.WaitForDirectoryActive(backend, dirID, time.Second))

		rec = doRequest(t, h, "DescribeDirectories", map[string]any{
			"DirectoryIds": []string{dirID},
		})
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		dirs = resp["DirectoryDescriptions"].([]any)
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

	t.Run("DnsIpAddrs and StageLastUpdatedDateTime are present and advance with stage", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "stage.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp := respBody(t, rec)
		dirs, _ := resp["DirectoryDescriptions"].([]any)
		require.Len(t, dirs, 1)
		dir := dirs[0].(map[string]any)

		dnsIPs, ok := dir["DnsIpAddrs"].([]any)
		require.True(t, ok, "DnsIpAddrs must be present on the wire")
		assert.NotEmpty(t, dnsIPs)
		requestedUpdated := dir["StageLastUpdatedDateTime"]
		assert.NotEmpty(t, requestedUpdated)

		backend := h.Backend.(*directoryservice.InMemoryBackend)
		require.True(t, directoryservice.WaitForDirectoryActive(backend, dirID, time.Second))

		rec2 := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp2 := respBody(t, rec2)
		dirs2, _ := resp2["DirectoryDescriptions"].([]any)
		dir2 := dirs2[0].(map[string]any)
		assert.Equal(t, "Active", dir2["Stage"])
		activeUpdated := dir2["StageLastUpdatedDateTime"]
		assert.NotEmpty(t, activeUpdated)
		assert.NotEqual(
			t, requestedUpdated, activeUpdated,
			"StageLastUpdatedDateTime must advance when the stage transitions",
		)
	})

	t.Run("VpcSettings SecurityGroupId is populated on DescribeDirectories", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name":     "vpc-secgroup.example.com",
			"Password": "Admin1234!",
			"Size":     "Small",
			"VpcSettings": map[string]any{
				"VpcId":     "vpc-12345678",
				"SubnetIds": []string{"subnet-1111", "subnet-2222"},
			},
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp := respBody(t, rec)
		dirs, ok := resp["DirectoryDescriptions"].([]any)
		require.True(t, ok)
		require.Len(t, dirs, 1)
		dir := dirs[0].(map[string]any)

		vpcSettings, ok := dir["VpcSettings"].(map[string]any)
		require.True(t, ok, "VpcSettings must be present in DescribeDirectories output")
		sgID, ok := vpcSettings["SecurityGroupId"].(string)
		require.True(t, ok, "SecurityGroupId must be a string on the wire")
		assert.NotEmpty(t, sgID)
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

	t.Run("NetworkType, DnsIpv6Addrs, DesiredNumberOfDomainControllers and RegionsInfo round-trip for "+
		"MicrosoftAD", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
			"Name":        "msad-fields.example.com",
			"Password":    "Admin1234!",
			"Edition":     "Enterprise",
			"NetworkType": "Dual-stack",
		})
		createResp := respBody(t, createRec)
		dirID, ok := createResp["DirectoryId"].(string)
		require.True(t, ok)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp := respBody(t, rec)
		dirs, _ := resp["DirectoryDescriptions"].([]any)
		require.Len(t, dirs, 1)
		dir := dirs[0].(map[string]any)

		assert.Equal(t, "Dual-stack", dir["NetworkType"])

		dnsV6, ok := dir["DnsIpv6Addrs"].([]any)
		require.True(t, ok, "DnsIpv6Addrs must be present on the wire")
		assert.NotEmpty(t, dnsV6, "a Dual-stack directory must report IPv6 DNS addresses")

		desired, ok := dir["DesiredNumberOfDomainControllers"]
		require.True(t, ok, "MicrosoftAD directories must report DesiredNumberOfDomainControllers")
		assert.InEpsilon(t, float64(2), desired, 0)

		regionsInfo, ok := dir["RegionsInfo"].(map[string]any)
		require.True(t, ok, "MicrosoftAD directories must report RegionsInfo")
		assert.Equal(t, "us-east-1", regionsInfo["PrimaryRegion"])
		assert.Empty(t, regionsInfo["AdditionalRegions"])

		// AddRegion must surface in RegionsInfo.AdditionalRegions.
		addRec := doRequest(t, h, "AddRegion", map[string]any{
			"DirectoryId": dirID,
			"RegionName":  "us-west-2",
			"VPCSettings": map[string]any{
				"VpcId":     "vpc-region2",
				"SubnetIds": []string{"subnet-r1", "subnet-r2"},
			},
		})
		require.Equal(t, http.StatusOK, addRec.Code)

		rec2 := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp2 := respBody(t, rec2)
		dirs2, _ := resp2["DirectoryDescriptions"].([]any)
		dir2 := dirs2[0].(map[string]any)
		regionsInfo2 := dir2["RegionsInfo"].(map[string]any)
		additional, ok := regionsInfo2["AdditionalRegions"].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{"us-west-2"}, additional)
	})

	t.Run("RadiusSettings and RadiusStatus round-trip after EnableRadius", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "radius.example.com")

		enableRec := doRequest(t, h, "EnableRadius", map[string]any{
			"DirectoryId": dirID,
			"RadiusSettings": map[string]any{
				"AuthenticationProtocol": "PAP",
				"RadiusServers":          []string{"10.2.2.2"},
				"SharedSecret":           "s3cr3t",
				"RadiusPort":             1812,
				"RadiusRetries":          3,
				"RadiusTimeout":          5,
			},
		})
		require.Equal(t, http.StatusOK, enableRec.Code)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp := respBody(t, rec)
		dirs, _ := resp["DirectoryDescriptions"].([]any)
		dir := dirs[0].(map[string]any)

		assert.Equal(t, "Completed", dir["RadiusStatus"])
		radiusSettings, ok := dir["RadiusSettings"].(map[string]any)
		require.True(t, ok, "RadiusSettings must be present on the wire once enabled")
		assert.Equal(t, "PAP", radiusSettings["AuthenticationProtocol"])
		assert.Equal(t, []any{"10.2.2.2"}, radiusSettings["RadiusServers"])
	})

	t.Run("ConnectSettings and customer DnsIpAddrs round-trip for ConnectDirectory", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		createRec := doRequest(t, h, "ConnectDirectory", map[string]any{
			"Name":     "connector.example.com",
			"Password": "Admin1234!",
			"Size":     "Small",
			"ConnectSettings": map[string]any{
				"CustomerUserName": "Admin",
				"VpcId":            "vpc-connector",
				"SubnetIds":        []string{"subnet-c1", "subnet-c2"},
				"CustomerDnsIps":   []string{"192.0.2.10", "192.0.2.11"},
			},
		})
		createResp := respBody(t, createRec)
		dirID, ok := createResp["DirectoryId"].(string)
		require.True(t, ok)

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{dirID}})
		resp := respBody(t, rec)
		dirs, _ := resp["DirectoryDescriptions"].([]any)
		dir := dirs[0].(map[string]any)

		// AD Connector's DnsIpAddrs must echo the customer's own DNS IPs,
		// not a synthesized Directory Service-managed address.
		assert.Equal(t, []any{"192.0.2.10", "192.0.2.11"}, dir["DnsIpAddrs"])

		connectSettings, ok := dir["ConnectSettings"].(map[string]any)
		require.True(t, ok, "ConnectSettings must be present for an AD Connector directory")
		assert.Equal(t, "Admin", connectSettings["CustomerUserName"])
		assert.Equal(t, "vpc-connector", connectSettings["VpcId"])
		assert.Equal(t, []any{"subnet-c1", "subnet-c2"}, connectSettings["SubnetIds"])
		connectIPs, ok := connectSettings["ConnectIps"].([]any)
		require.True(t, ok)
		assert.NotEmpty(t, connectIPs, "ConnectIps (the AD Connector's own IPs) must be populated")
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

func TestCreateDirectory_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "missing Name returns 400 InvalidParameterException",
			body:     map[string]any{"Password": "Admin1234!", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name:     "missing Password returns 400 InvalidParameterException",
			body:     map[string]any{"Name": "corp.example.com", "Size": "Small"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid Size returns 400 InvalidParameterException",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "Huge",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "empty Size returns 400 InvalidParameterException",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Size":     "",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
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
			wantType: "InvalidParameterException",
		},
		{
			name:     "missing Password returns 400",
			body:     map[string]any{"Name": "corp.example.com", "Edition": "Enterprise"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "invalid Edition returns 400",
			body: map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Ultra",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
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

func TestGetDirectoryLimits_ReflectsCounts(t *testing.T) {
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
				"ConnectSettings": map[string]any{
					"CustomerUserName": "Admin",
					"VpcId":            "vpc-12345678",
					"SubnetIds":        []string{"subnet-11111111", "subnet-22222222"},
				},
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

// TestDirectoryCreationLifecycle verifies the Requested → Creating → Active stage transitions.
func TestDirectoryCreationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(h *directoryservice.Handler) string
		name   string
	}{
		{
			name:   "SimpleAD",
			create: func(h *directoryservice.Handler) string { return mustCreateSimpleAD(t, h, "corp.example.com") },
		},
		{
			name:   "MicrosoftAD",
			create: func(h *directoryservice.Handler) string { return mustCreateMicrosoftAD(t, h, "corp.example.com") },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			backend := h.Backend.(*directoryservice.InMemoryBackend)
			dirID := tc.create(h)

			// Immediately after creation the stage must not yet be Active.
			initial := directoryservice.DirectoryStageForTest(backend, dirID)
			assert.Equal(t, "Requested", initial, "directory should start as Requested")

			// Eventually reaches Active.
			ok := directoryservice.WaitForDirectoryActive(backend, dirID, 2*time.Second)
			assert.True(t, ok, "directory should reach Active within 2s")

			final := directoryservice.DirectoryStageForTest(backend, dirID)
			assert.Equal(t, "Active", final)
		})
	}
}
