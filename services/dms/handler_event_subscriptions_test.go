package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("del-es", "arn:aws:sns:us-east-1:123:topic")

	rec := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.EventSubscriptionCount())

	rec2 := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestModifyEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("mod-es", "arn:aws:sns:us-east-1:123:topic")

	enabled := true
	rec := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "mod-es",
		"Enabled":          enabled,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestCreateEventSubscription_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	body := map[string]any{
		"SubscriptionName": "dup-sub",
		"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
	}

	rec1 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusConflict, rec2.Code)

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
	assert.Equal(t, "ResourceAlreadyExistsFault", errBody["__type"])
}

func TestDescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a replication instance to generate a creation event.
	rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "test-ri",
		"ReplicationInstanceClass":      "dms.t3.micro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create an endpoint to generate another event.
	rec = doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEvents must now return at least these two events.
	rec = doDMS(t, h, "DescribeEvents", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	events, ok := body["Events"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(events), 1, "at least endpoint creation event expected")

	e0, ok := events[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, e0["Message"])
	assert.NotEmpty(t, e0["Date"])
}
