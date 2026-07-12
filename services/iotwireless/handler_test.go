package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func newTestHandlerHTTP() *iotwireless.Handler {
	bk := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doIoTWRequest(t *testing.T, h *iotwireless.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateGetListDeleteWirelessDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceName string
		devType    string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			deviceName: "my-device",
			devType:    "LoRaWAN",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.deviceName + `","Type":"` + tt.devType + `","DestinationName":"d1"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.deviceName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			devices, ok := listResp["WirelessDeviceList"].([]any)
			require.True(t, ok)
			assert.Len(t, devices, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_GetWirelessDevice_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "no_such_device", id: "does-not-exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+tt.id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_CreateGetListDeleteServiceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantStatus  int
	}{
		{
			name:        "full_lifecycle",
			profileName: "my-profile",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.profileName + `"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.profileName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			profiles, ok := listResp["ServiceProfileList"].([]any)
			require.True(t, ok)
			assert.Len(t, profiles, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_CreateGetListDeleteDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		destName   string
		expression string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			destName:   "my-dest",
			expression: "my-iot-rule",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.destName + `","Expression":"` + tt.expression +
				`","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::000000000000:role/r"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/destinations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.destName, createResp["Name"])

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.destName, getResp["Name"])
			assert.Equal(t, tt.expression, getResp["Expression"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			dests, ok := listResp["DestinationList"].([]any)
			require.True(t, ok)
			assert.Len(t, dests, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_ListTagsTagUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		addTags    string
		wantKey    string
		wantValue  string
		wantGone   string
		removeTags []string
	}{
		{
			name:      "tag_and_list",
			addTags:   `{"Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"platform"}]}`,
			wantKey:   "env",
			wantValue: "prod",
		},
		{
			name:       "tag_then_untag",
			addTags:    `{"Tags":[{"Key":"env","Value":"staging"},{"Key":"team","Value":"infra"}]}`,
			removeTags: []string{"team"},
			wantKey:    "env",
			wantValue:  "staging",
			wantGone:   "team",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create a service profile to get a real ARN.
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", `{"Name":"tag-test-profile"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			arn := createResp["Arn"].(string)
			require.NotEmpty(t, arn)

			// Real AWS binds all three tag ops to the bare "/tags" path with
			// the resource ARN as the "resourceArn" query parameter (never a
			// path segment).
			encodedARN := url.QueryEscape(arn)

			// TagResource
			rec = doIoTWRequest(t, h, http.MethodPost, "/tags?resourceArn="+encodedARN, tt.addTags)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Untag if specified.
			if len(tt.removeTags) > 0 {
				var queryStr strings.Builder
				queryStr.WriteString("resourceArn=" + encodedARN)
				for _, k := range tt.removeTags {
					queryStr.WriteString("&tagKeys=" + k)
				}

				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, "/tags?"+queryStr.String(), http.NoBody)
				recDel := httptest.NewRecorder()
				c := e.NewContext(req, recDel)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusNoContent, recDel.Code)
			}

			// ListTags
			rec = doIoTWRequest(t, h, http.MethodGet, "/tags?resourceArn="+encodedARN, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
			tagList, ok := tagsResp["Tags"].([]any)
			require.True(t, ok)

			got := make(map[string]string, len(tagList))

			for _, kv := range tagList {
				m, kvOK := kv.(map[string]any)
				require.True(t, kvOK)
				got[m["Key"].(string)] = m["Value"].(string)
			}

			assert.Equal(t, tt.wantValue, got[tt.wantKey])

			if tt.wantGone != "" {
				_, present := got[tt.wantGone]
				assert.False(t, present, "tag %q should be removed", tt.wantGone)
			}
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "unknown_base", path: "/unknown-resource"},
		{name: "root", path: "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_CreateGetListDeleteWirelessGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		gatewayName string
		description string
		wantStatus  int
	}{
		{
			name:        "full_lifecycle",
			gatewayName: "my-gateway",
			description: "test gateway",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "gateway_with_empty_description",
			gatewayName: "bare-gateway",
			description: "",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.gatewayName + `","Description":"` + tt.description + `"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.gatewayName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			gateways, ok := listResp["WirelessGatewayList"].([]any)
			require.True(t, ok)
			assert.Len(t, gateways, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFunc func(*testing.T, *iotwireless.Handler)
		name      string
	}{
		{
			name: "name_is_IoTWireless",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, "IoTWireless", h.Name())
			},
		},
		{
			name: "chaos_service_name_is_iotwireless",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, "iotwireless", h.ChaosServiceName())
			},
		},
		{
			name: "supported_operations_not_empty",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.NotEmpty(t, h.GetSupportedOperations())
			},
		},
		{
			name: "chaos_operations_matches_supported",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
			},
		},
		{
			name: "chaos_regions_contains_default_region",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, []string{testRegion}, h.ChaosRegions())
			},
		},
		{
			name: "match_priority_is_86",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, 86, h.MatchPriority())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			tt.checkFunc(t, h)
		})
	}
}

func TestHandler_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "get_wireless_gateway_not_found",
			method:     http.MethodGet,
			path:       "/wireless-gateways/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_wireless_gateway_not_found",
			method:     http.MethodDelete,
			path:       "/wireless-gateways/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_service_profile_not_found",
			method:     http.MethodDelete,
			path:       "/service-profiles/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_wireless_device_not_found",
			method:     http.MethodDelete,
			path:       "/wireless-devices/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_destination_not_found",
			method:     http.MethodDelete,
			path:       "/destinations/no-such-name",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unsupported_method_on_collection",
			method:     http.MethodPut,
			path:       "/service-profiles",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Populate a device so the snapshot is non-trivial.
	body := `{"Name":"persist-dev","Type":"LoRaWAN","DestinationName":"d","Description":"desc"}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Snapshot via the handler.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap, "Snapshot must return non-nil data")

	// Fresh handler with the same backend type.
	bk2 := iotwireless.NewInMemoryBackend()
	h2 := iotwireless.NewHandler(bk2)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	// Restore into h2.
	require.NoError(t, h2.Restore(t.Context(), snap))

	// The device must be visible through h2.
	devices := bk2.ListWirelessDevices(testAccountID, testRegion)
	assert.Len(t, devices, 1)
	assert.Equal(t, "persist-dev", devices[0].Name)
}

func TestHandler_CreateDeviceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantStatus  int
	}{
		{
			name:        "create_device_profile",
			profileName: "my-device-profile",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "create_with_empty_name",
			profileName: "",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"Name":"` + tt.profileName + `"}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/device-profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Id"])
			assert.NotEmpty(t, resp["Arn"])
		})
	}
}

func TestHandler_CreateFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskName    string
		description string
		wantStatus  int
	}{
		{
			name:        "create_fuota_task",
			taskName:    "my-fuota-task",
			description: "firmware update task",
			wantStatus:  http.StatusCreated,
		},
		{
			name:       "create_without_description",
			taskName:   "bare-fuota-task",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"Name":"` + tt.taskName + `","Description":"` + tt.description +
				`","FirmwareUpdateImage":"s3://bucket/fw.bin","FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Id"])
			assert.NotEmpty(t, resp["Arn"])
		})
	}
}

func TestHandler_AssociateAwsAccountWithPartnerAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		partnerAccountID string
		wantStatus       int
	}{
		{
			name:             "associate_partner_account",
			partnerAccountID: "partner-123",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "idempotent_reassociation",
			partnerAccountID: "partner-456",
			wantStatus:       http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			// Real AWS binds this op to POST /partner-accounts (no path
			// parameter): the partner account ID is Sidewalk.AmazonId in the
			// body, and Tags is a []Tag{Key,Value} list.
			body := `{"Sidewalk":{"AmazonId":"` + tt.partnerAccountID + `"},"Tags":[{"Key":"env","Value":"prod"}]}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/partner-accounts", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Arn"])
			sidewalk, ok := resp["Sidewalk"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.partnerAccountID, sidewalk["AmazonId"])
		})
	}
}

func TestHandler_AssociateMulticastGroupWithFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fuotaTaskID      string
		multicastGroupID string
		wantStatus       int
	}{
		{
			name:             "associate_multicast_group",
			fuotaTaskID:      "fuota-task-001",
			multicastGroupID: "multicast-group-001",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"MulticastGroupId":"` + tt.multicastGroupID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+tt.fuotaTaskID+"/multicast-groups", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessDeviceWithFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fuotaTaskID      string
		wirelessDeviceID string
		wantStatus       int
	}{
		{
			name:             "associate_wireless_device",
			fuotaTaskID:      "fuota-task-002",
			wirelessDeviceID: "dev-001",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"WirelessDeviceId":"` + tt.wirelessDeviceID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+tt.fuotaTaskID+"/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessDeviceWithMulticastGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		multicastGroupID string
		wirelessDeviceID string
		wantStatus       int
	}{
		{
			name:             "associate_wireless_device",
			multicastGroupID: "multicast-group-002",
			wirelessDeviceID: "dev-002",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"WirelessDeviceId":"` + tt.wirelessDeviceID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut,
				"/multicast-groups/"+tt.multicastGroupID+"/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessDeviceWithThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		thingArn   string
		createDev  bool
		wantStatus int
	}{
		{
			name:       "associate_existing_device",
			thingArn:   "arn:aws:iot:us-east-1:000000000000:thing/my-thing",
			createDev:  true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "device_not_found",
			thingArn:   "arn:aws:iot:us-east-1:000000000000:thing/other-thing",
			createDev:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			devID := "no-such-device"

			if tt.createDev {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
					`{"Name":"dev-thing","Type":"LoRaWAN","DestinationName":"d"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				devID = createResp["Id"].(string)
			}

			body := `{"ThingArn":"` + tt.thingArn + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-devices/"+devID+"/thing", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessGatewayWithCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		iotCertificateID string
		createGateway    bool
		wantStatus       int
	}{
		{
			name:             "associate_existing_gateway",
			iotCertificateID: "cert-abc123",
			createGateway:    true,
			wantStatus:       http.StatusOK,
		},
		{
			name:             "gateway_not_found",
			iotCertificateID: "cert-xyz789",
			createGateway:    false,
			wantStatus:       http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			gwID := "no-such-gateway"

			if tt.createGateway {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
					`{"Name":"gw-cert","Description":"cert gw"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				gwID = createResp["Id"].(string)
			}

			body := `{"IotCertificateId":"` + tt.iotCertificateID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/certificate", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["IotCertificateArn"])
			}
		})
	}
}

func TestHandler_AssociateWirelessGatewayWithThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		thingArn      string
		createGateway bool
		wantStatus    int
	}{
		{
			name:          "associate_existing_gateway",
			thingArn:      "arn:aws:iot:us-east-1:000000000000:thing/gw-thing",
			createGateway: true,
			wantStatus:    http.StatusNoContent,
		},
		{
			name:          "gateway_not_found",
			thingArn:      "arn:aws:iot:us-east-1:000000000000:thing/other-thing",
			createGateway: false,
			wantStatus:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			gwID := "no-such-gateway"

			if tt.createGateway {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
					`{"Name":"gw-thing","Description":"thing gw"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				gwID = createResp["Id"].(string)
			}

			body := `{"ThingArn":"` + tt.thingArn + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/thing", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CancelMulticastGroupSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		multicastGroupID string
		wantStatus       int
	}{
		{
			name:             "cancel_existing_session",
			multicastGroupID: "multicast-group-session-01",
			wantStatus:       http.StatusNoContent,
		},
		{
			name:             "cancel_nonexistent_session_is_idempotent",
			multicastGroupID: "nonexistent-group",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodDelete, "/multicast-groups/"+tt.multicastGroupID+"/session", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AssociateAwsAccountWithPartnerAccount",
		"AssociateMulticastGroupWithFuotaTask",
		"AssociateWirelessDeviceWithFuotaTask",
		"AssociateWirelessDeviceWithMulticastGroup",
		"AssociateWirelessDeviceWithThing",
		"AssociateWirelessGatewayWithCertificate",
		"AssociateWirelessGatewayWithThing",
		"CancelMulticastGroupSession",
		"CreateDeviceProfile",
		"CreateFuotaTask",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "GetSupportedOperations should contain %q", op)
	}
}
