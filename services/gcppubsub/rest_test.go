package gcppubsub_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create topic", Method: http.MethodPut, Path: "/v1/projects/p/topics/top", Body: `{}`, Status: 200,
			Contains: []string{`"name":"projects/p/topics/top"`},
		},
		{
			Name: "create subscription", Method: http.MethodPut, Path: "/v1/projects/p/subscriptions/sub",
			Body: `{"topic":"projects/p/topics/top","ackDeadlineSeconds":10}`, Status: 200,
			Contains: []string{`"ackDeadlineSeconds":10`},
		},
		{
			Name: "publish", Method: http.MethodPost, Path: "/v1/projects/p/topics/top:publish",
			Body: `{"messages":[{"data":"aGk="}]}`, Status: 200, Contains: []string{`"messageIds"`},
		},
		{
			Name: "pull", Method: http.MethodPost, Path: "/v1/projects/p/subscriptions/sub:pull",
			Body: `{"maxMessages":1}`, Status: 200, Contains: []string{`"data":"aGk="`},
		},
		{
			Name: "get missing topic", Method: http.MethodGet, Path: "/v1/projects/p/topics/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
