package ocinotifications_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/ons"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	created, err := e.ctl.CreateTopic(ctx, ons.CreateTopicRequest{
		Name: new("orders"), CompartmentId: new(testCompartment), Description: new("order events"),
		FreeformTags: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.NotNil(t, created.Etag)
	assert.Equal(t, ons.NotificationTopicLifecycleStateCreating, created.LifecycleState)
	assert.Equal(t, e.base, *created.ApiEndpoint)
	assert.Contains(t, *created.TopicId, "ocid1.onstopic.")
	assert.NotEmpty(t, *created.ShortTopicId)

	_, err = e.ctl.CreateTopic(ctx, ons.CreateTopicRequest{
		Name: new("orders"), CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.dp.PublishMessage(ctx, ons.PublishMessageRequest{
		TopicId: created.TopicId, Body: new("x"),
	})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	e.settle()

	got, err := e.ctl.GetTopic(ctx, ons.GetTopicRequest{TopicId: created.TopicId})
	require.NoError(t, err)
	assert.Equal(t, ons.NotificationTopicLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "order events", *got.Description)
	assert.Equal(t, "v", got.FreeformTags["k"])

	_, err = e.ctl.UpdateTopic(ctx, ons.UpdateTopicRequest{
		TopicId: created.TopicId, IfMatch: new("stale"),
		Description: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.ctl.UpdateTopic(ctx, ons.UpdateTopicRequest{
		TopicId: created.TopicId, IfMatch: got.Etag,
		Description: new("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", *upd.Description)
	assert.NotEqual(t, *got.Etag, *upd.Etag)

	list, err := e.ctl.ListTopics(ctx, ons.ListTopicsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "orders", *list.Items[0].Name)

	sub := e.subscribe(t, got.NotificationTopic, "PAGERDUTY", "integration-key")

	_, err = e.ctl.ChangeTopicCompartment(ctx, ons.ChangeTopicCompartmentRequest{
		TopicId:                       created.TopicId,
		ChangeTopicCompartmentDetails: ons.ChangeCompartmentDetails{CompartmentId: new("ocid1.compartment.oc1..other")},
	})
	require.NoError(t, err)

	moved, err := e.ctl.ListTopics(ctx, ons.ListTopicsRequest{CompartmentId: new("ocid1.compartment.oc1..other")})
	require.NoError(t, err)
	assert.Len(t, moved.Items, 1)

	_, err = e.ctl.DeleteTopic(ctx, ons.DeleteTopicRequest{TopicId: created.TopicId})
	require.NoError(t, err)

	del, err := e.ctl.GetTopic(ctx, ons.GetTopicRequest{TopicId: created.TopicId})
	require.NoError(t, err)
	assert.Equal(t, ons.NotificationTopicLifecycleStateDeleting, del.LifecycleState)

	e.settle()

	_, err = e.ctl.GetTopic(ctx, ons.GetTopicRequest{TopicId: created.TopicId})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.dp.GetSubscription(ctx, ons.GetSubscriptionRequest{SubscriptionId: sub.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "subscriptions go with the topic")
}

func TestCreateTopicValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		det  ons.CreateTopicDetails
		code string
	}{
		{"blank name", ons.CreateTopicDetails{Name: new(" "), CompartmentId: new(testCompartment)}, "MissingParameter"},
		{
			"long name",
			ons.CreateTopicDetails{Name: new(strings.Repeat("n", 257)), CompartmentId: new(testCompartment)},
			"InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.ctl.CreateTopic(t.Context(), ons.CreateTopicRequest{CreateTopicDetails: tt.det})
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestListTopicsFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	for _, n := range []string{"t1", "t2", "t3"} {
		e.mkTopic(t, n)
	}

	tests := []struct {
		req  ons.ListTopicsRequest
		name string
		want int
	}{
		{ons.ListTopicsRequest{}, "all", 3},
		{ons.ListTopicsRequest{Name: new("t2")}, "by name", 1},
		{ons.ListTopicsRequest{LifecycleState: ons.NotificationTopicSummaryLifecycleStateCreating}, "by state", 0},
		{ons.ListTopicsRequest{Limit: new(2)}, "limit", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := tt.req
			r.CompartmentId = new(testCompartment)

			resp, err := e.ctl.ListTopics(t.Context(), r)
			require.NoError(t, err)
			assert.Len(t, resp.Items, tt.want)

			if tt.name == "limit" {
				require.NotNil(t, resp.OpcNextPage)
			}
		})
	}

	_, err := e.ctl.ListTopics(t.Context(), ons.ListTopicsRequest{CompartmentId: new("")})
	require.Error(t, err)
}

func TestAuthEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})

	status, body := rawDo(t, http.MethodGet, e.base+"/20181201/topics?compartmentId="+testCompartment, "")
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")

	_, err := e.ctl.ListTopics(t.Context(), ons.ListTopicsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{
			"missing topic",
			http.MethodGet,
			"/20181201/topics/ocid1.onstopic.oc1..nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"bad json", http.MethodPost, "/20181201/topics", "{", "CannotParseRequest", 400},
		{"unknown route", http.MethodGet, "/20181201/nothing", "", "NotFound", 404},
		{"missing subscription", http.MethodGet, "/20181201/subscriptions/nope", "", "NotAuthorizedOrNotFound", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
		})
	}
}
