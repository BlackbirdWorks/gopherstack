package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestSignalMap_StatusIsLegalEnumMember drives CreateSignalMap and
// StartUpdateSignalMap through the real aws-sdk-go-v2 client.
// CreateSignalMapOutput.Status/StartUpdateSignalMapOutput.Status are
// types.SignalMapStatus (CREATE_IN_PROGRESS/CREATE_COMPLETE/CREATE_FAILED/
// UPDATE_IN_PROGRESS/UPDATE_COMPLETE/UPDATE_REVERTED/UPDATE_FAILED/READY/
// NOT_READY -- medialive@v1.101.4 types/enums.go); the backend previously
// set the bare string "SUCCEEDED" on both create and update, which is not a
// member of SignalMapStatus, so a real client's waiter for a signal map
// would never match any case and poll until timeout.
func TestSignalMap_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("my-signal-map"),
		DiscoveryEntryPointArn: aws.String("arn:aws:medialive:us-east-1:000000000000:input:1234567"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalMapStatusCreateComplete, created.Status)

	updated, err := client.StartUpdateSignalMap(ctx, &medialivesdk.StartUpdateSignalMapInput{
		Identifier:  created.Id,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalMapStatusUpdateComplete, updated.Status)
}

// TestSignalMap_MonitorDeploymentStatusIsLegalEnumMember covers the same
// SignalMapMonitorDeploymentStatus bug on StartMonitorDeployment/
// StartDeleteMonitorDeployment. Unlike Status, the real
// StartMonitorDeploymentOutput/StartDeleteMonitorDeploymentOutput nest their
// status under a "monitorDeployment" object (types.MonitorDeployment.Status
// -- medialive@v1.101.4 types/types.go:5679), but this backend's handler
// emits a flat top-level "monitorDeploymentStatus" key instead -- a
// pre-existing, unrelated wire-shape bug (not fixed here; flagged
// separately) that stops the real SDK client from decoding
// MonitorDeployment at all. This test therefore drives the raw HTTP route
// (same as the rest of this package's non-SDK tests) rather than the SDK's
// decoded field, and still compares against the typed enum constant's wire
// value, not a bare literal.
func TestSignalMap_MonitorDeploymentStatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name":                   "my-signal-map-2",
		"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:input:1234567",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)
	var createOut struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	deployRec := doRequest(t, h, http.MethodPost, "/prod/signal-maps/"+createOut.ID+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, deployRec.Code)
	var deployOut struct {
		MonitorDeploymentStatus string `json:"monitorDeploymentStatus"`
	}
	require.NoError(t, json.Unmarshal(deployRec.Body.Bytes(), &deployOut))
	assert.Equal(t,
		string(types.SignalMapMonitorDeploymentStatusDeploymentComplete),
		deployOut.MonitorDeploymentStatus)

	deleteRec := doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+createOut.ID+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, deleteRec.Code)
	var deleteOut struct {
		MonitorDeploymentStatus string `json:"monitorDeploymentStatus"`
	}
	require.NoError(t, json.Unmarshal(deleteRec.Body.Bytes(), &deleteOut))
	assert.Equal(t,
		string(types.SignalMapMonitorDeploymentStatusDeleteComplete),
		deleteOut.MonitorDeploymentStatus)
}
