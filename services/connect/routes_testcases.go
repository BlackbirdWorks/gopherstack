package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func testCaseRoutes() []restroute.Route {
	const (
		evalByID = "/contact-evaluations/{InstanceId}/{EvaluationId}"
		one      = "/test-cases/{InstanceId}/{TestCaseId}"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: "/test-cases/{InstanceId}", Op: "CreateTestCase"},
		{Method: http.MethodGet, Path: one, Op: "DescribeTestCase"},
		{Method: http.MethodPost, Path: one, Op: "UpdateTestCase"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteTestCase"},
		{Method: http.MethodGet, Path: "/test-cases-summary/{InstanceId}", Op: "ListTestCases"},
		{Method: http.MethodPut, Path: "/contact-evaluations/{InstanceId}", Op: "StartContactEvaluation"},
		{Method: http.MethodGet, Path: "/contact-evaluations/{InstanceId}", Op: "ListContactEvaluations"},
		{
			Method: http.MethodGet,
			Path:   evalByID,
			Op:     "DescribeContactEvaluation",
		},
		{
			Method: http.MethodPost,
			Path:   evalByID,
			Op:     "UpdateContactEvaluation",
		},
		{
			Method: http.MethodDelete,
			Path:   evalByID,
			Op:     "DeleteContactEvaluation",
		},
		{
			Method: http.MethodPost,
			Path:   "/contact-evaluations/{InstanceId}/{EvaluationId}/submit",
			Op:     "SubmitContactEvaluation",
		},
	}
}
