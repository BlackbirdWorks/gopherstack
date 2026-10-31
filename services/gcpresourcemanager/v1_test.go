package gcpresourcemanager_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

const v1Create = `{"projectId":"v1-proj","name":"V1 Project","labels":{"team":"a"},` +
	`"parent":{"type":"organization","id":"123456789012"}}`

func TestV1ProjectFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.addr, []resttest.Step{
		{
			Name: "create", Method: "POST", Path: "/v1/projects", Body: v1Create, Status: 200,
			Contains: []string{`"done":true`, `"name":"operations/cp.`},
		},
		{
			Name: "get", Method: "GET", Path: "/v1/projects/v1-proj", Status: 200,
			Contains: []string{
				`"projectId":"v1-proj"`, `"lifecycleState":"ACTIVE"`, `"projectNumber":"`, `"name":"V1 Project"`,
				`"parent":{"type":"organization","id":"123456789012"}`, `"labels":{"team":"a"}`, `"createTime":"`,
			},
		},
		{
			Name: "duplicate", Method: "POST", Path: "/v1/projects", Body: v1Create, Status: 409,
			Contains: []string{`"status":"ALREADY_EXISTS"`},
		},
		{
			Name: "bad id", Method: "POST", Path: "/v1/projects", Body: `{"projectId":"Bad_ID"}`, Status: 400,
			Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "update", Method: "PUT", Path: "/v1/projects/v1-proj", Status: 200,
			Body:     `{"projectId":"v1-proj","name":"Renamed","labels":{"team":"b"},"parent":{"type":"organization","id":"123456789012"}}`,
			Contains: []string{`"name":"Renamed"`, `"labels":{"team":"b"}`},
		},
		{
			Name: "update project id is immutable", Method: "PUT", Path: "/v1/projects/v1-proj", Status: 400,
			Body: `{"projectId":"other-id","name":"Renamed"}`,
		},
		{
			Name: "list", Method: "GET", Path: "/v1/projects?filter=id:v1-proj", Status: 200,
			Contains: []string{`"projects":[`, `"projectId":"v1-proj"`},
		},
		{
			Name: "list empty", Method: "GET", Path: "/v1/projects?filter=id:nothing-here", Status: 200,
			Contains: []string{`{}`},
		},
		{
			Name: "ancestry", Method: "POST", Path: "/v1/projects/v1-proj:getAncestry", Status: 200,
			Contains: []string{
				`{"resourceId":{"type":"project","id":"v1-proj"}}`, `{"resourceId":{"type":"organization","id":"123456789012"}}`,
			},
		},
		{
			Name: "get iam", Method: "POST", Path: "/v1/projects/v1-proj:getIamPolicy",
			Body: `{"options":{"requestedPolicyVersion":3}}`, Status: 200, Contains: []string{`"etag"`},
		},
		{
			Name: "set iam", Method: "POST", Path: "/v1/projects/v1-proj:setIamPolicy", Status: 200,
			Body:     `{"policy":{"bindings":[{"role":"roles/viewer","members":["user:a@example.com"]}]}}`,
			Contains: []string{`"role":"roles/viewer"`, `"user:a@example.com"`},
		},
		{
			Name: "set iam stale etag", Method: "POST", Path: "/v1/projects/v1-proj:setIamPolicy", Status: 409,
			Body:     `{"policy":{"etag":"AAAAAAAAAAA=","bindings":[{"role":"roles/viewer","members":["user:b@example.com"]}]}}`,
			Contains: []string{`"status":"ABORTED"`},
		},
		{
			Name: "test iam", Method: "POST", Path: "/v1/projects/v1-proj:testIamPermissions", Status: 200,
			Body: `{"permissions":["compute.instances.get"]}`, Contains: []string{`"compute.instances.get"`},
		},
		{
			Name: "billing info unset", Method: "GET", Path: "/v1/projects/v1-proj/billingInfo", Status: 200,
			Contains: []string{`"name":"projects/v1-proj/billingInfo"`, `"projectId":"v1-proj"`, `"billingEnabled":false`},
		},
		{
			Name: "billing info set", Method: "PUT", Path: "/v1/projects/v1-proj/billingInfo", Status: 200,
			Body:     `{"billingAccountName":"billingAccounts/0X0X0X-0X0X0X-0X0X0X"}`,
			Contains: []string{`"billingAccountName":"billingAccounts/0X0X0X-0X0X0X-0X0X0X"`, `"billingEnabled":true`},
		},
		{
			Name: "billing info bad account", Method: "PUT", Path: "/v1/projects/v1-proj/billingInfo", Status: 400,
			Body: `{"billingAccountName":"nope"}`,
		},
		{Name: "billing info unknown", Method: "GET", Path: "/v1/projects/nope-nope/billingInfo", Status: 403},
		{Name: "delete", Method: "DELETE", Path: "/v1/projects/v1-proj", Status: 200, Contains: []string{`{}`}},
		{
			Name: "get deleted", Method: "GET", Path: "/v1/projects/v1-proj", Status: 200,
			Contains: []string{`"lifecycleState":"DELETE_REQUESTED"`},
		},
		{Name: "undelete", Method: "POST", Path: "/v1/projects/v1-proj:undelete", Status: 200, Contains: []string{`{}`}},
		{
			Name: "get restored", Method: "GET", Path: "/v1/projects/v1-proj", Status: 200,
			Contains: []string{`"lifecycleState":"ACTIVE"`},
		},
		{
			Name: "unknown project", Method: "GET", Path: "/v1/projects/nope-nope", Status: 403,
			Contains: []string{`"status":"PERMISSION_DENIED"`, `IAM_PERMISSION_DENIED`},
		},
		{Name: "unknown route", Method: "GET", Path: "/v1/projects/v1-proj:bogus", Status: 404},
	})
}

func TestV1OrganizationFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.addr, []resttest.Step{
		{
			Name: "search", Method: "POST", Path: "/v1/organizations:search", Body: `{"filter":"domain:gopherstack.local"}`,
			Status:   200,
			Contains: []string{`"organizations":[`, `"name":"organizations/123456789012"`, `"displayName":"gopherstack.local"`, `"lifecycleState":"ACTIVE"`},
		},
		{
			Name: "search no match", Method: "POST", Path: "/v1/organizations:search", Body: `{"filter":"domain:other.example"}`,
			Status: 200, Contains: []string{`{}`},
		},
		{
			Name: "get", Method: "GET", Path: "/v1/organizations/123456789012", Status: 200,
			Contains: []string{`"displayName":"gopherstack.local"`, `"owner":{"directoryCustomerId":"C123456789012"}`},
		},
		{Name: "get unknown", Method: "GET", Path: "/v1/organizations/1", Status: 403},
		{
			Name: "set iam", Method: "POST", Path: "/v1/organizations/123456789012:setIamPolicy", Status: 200,
			Body:     `{"policy":{"bindings":[{"role":"roles/resourcemanager.organizationViewer","members":["user:a@example.com"]}]}}`,
			Contains: []string{`organizationViewer`},
		},
		{
			Name: "get iam", Method: "POST", Path: "/v1/organizations/123456789012:getIamPolicy", Body: `{}`, Status: 200,
			Contains: []string{`organizationViewer`},
		},
	})
}

func TestServiceUsageRESTIsNotShadowedByV1(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	resttest.Run(t, e.addr, []resttest.Step{
		{
			Name: "list services", Method: "GET", Path: "/v1/projects/gopherstack-test/services?filter=state:ENABLED", Status: 200,
			Contains: []string{`"services":[`, `"state":"ENABLED"`},
		},
		{
			Name: "enable", Method: "POST", Path: "/v1/projects/gopherstack-test/services/run.googleapis.com:enable", Status: 200,
			Contains: []string{`"done":true`, `"name":"operations/acat.`},
		},
		{
			Name: "batch enable", Method: "POST", Path: "/v1/projects/gopherstack-test/services:batchEnable", Status: 200,
			Body: `{"serviceIds":["dns.googleapis.com"]}`, Contains: []string{`"done":true`},
		},
		{
			Name: "disable", Method: "POST", Path: "/v1/projects/gopherstack-test/services/run.googleapis.com:disable", Status: 200,
			Body: `{"disableDependentServices":true}`, Contains: []string{`"done":true`},
		},
		{
			Name: "batch get", Method: "GET", Path: "/v1/projects/gopherstack-test/services:batchGet?names=projects/gopherstack-test/services/run.googleapis.com",
			Status: 200, Contains: []string{`"state":"DISABLED"`},
		},
	})
}
