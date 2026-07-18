package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventStream_PutAndGet_Fields(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-app")

	destArn := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	roleArn := "arn:aws:iam::123456789012:role/PinpointKinesisRole"

	putRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{
			"DestinationStreamArn": destArn,
			"RoleArn":              roleArn,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var es map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &es))

	assert.Equal(t, destArn, es["DestinationStreamArn"])
	assert.Equal(t, roleArn, es["RoleArn"])
	assert.Equal(t, appID, es["ApplicationId"])
	assert.NotEmpty(t, es["LastModifiedDate"], "LastModifiedDate must be set")
}

func TestEventStream_Replace(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-replace-app")

	stream1 := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-1"
	stream2 := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-2"
	roleArn := "arn:aws:iam::123456789012:role/Role"

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{"DestinationStreamArn": stream1, "RoleArn": roleArn})

	putRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{"DestinationStreamArn": stream2, "RoleArn": roleArn})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var es map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &es))

	assert.Equal(t, stream2, es["DestinationStreamArn"], "second PUT must replace first")
}

func TestEventStream_Delete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "evtstream-del-app")

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/eventstream",
		map[string]any{
			"DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
			"RoleArn":              "arn:aws:iam::123456789012:role/R",
		})

	delRec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/eventstream", nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/eventstream", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestEventStream_AppIsolation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appA := createTestApp(t, h, "evtstream-app-a")
	appB := createTestApp(t, h, "evtstream-app-b")

	streamA := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-a"
	streamB := "arn:aws:kinesis:us-east-1:123456789012:stream/stream-b"

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appA+"/eventstream",
		map[string]any{"DestinationStreamArn": streamA, "RoleArn": "arn:aws:iam::123:role/r"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appB+"/eventstream",
		map[string]any{"DestinationStreamArn": streamB, "RoleArn": "arn:aws:iam::123:role/r"})

	getA := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appA+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getA.Code)
	var esA map[string]any
	require.NoError(t, json.Unmarshal(getA.Body.Bytes(), &esA))
	assert.Equal(t, streamA, esA["DestinationStreamArn"])

	getB := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appB+"/eventstream", nil)
	require.Equal(t, http.StatusOK, getB.Code)
	var esB map[string]any
	require.NoError(t, json.Unmarshal(getB.Body.Bytes(), &esB))
	assert.Equal(t, streamB, esB["DestinationStreamArn"])
}

// ──────────────────────────────────────────────────
// Channel settings — credential flags per type
// ──────────────────────────────────────────────────

func TestPinpoint_EventStream(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "event-stream-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID),
		map[string]any{
			"DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			"RoleArn":              "arn:aws:iam::123456789012:role/PinpointKinesisRole",
		})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
