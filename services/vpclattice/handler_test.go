package vpclattice_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func newTestHandler(t *testing.T) *vpclattice.Handler {
	t.Helper()
	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")

	return vpclattice.NewHandler(backend)
}

func doRequest(t *testing.T, h *vpclattice.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		buf = bytes.NewReader(data)
	} else {
		buf = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(buf.Len())
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func parseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestService_CRUD tests create/get/update/delete/list for services.
func TestService_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     map[string]any
		wantCode int
		check    func(t *testing.T, resp map[string]any)
	}{
		{
			name:     "create missing name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create with name returns 201",
			body:     map[string]any{"name": "my-svc"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(t, resp["arn"], "arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-")
				assert.Equal(t, "my-svc", resp["name"])
				assert.Equal(t, "ACTIVE", resp["status"])
				assert.NotEmpty(t, resp["id"])
			},
		},
		{
			name:     "create duplicate name returns 409",
			body:     map[string]any{"name": "dup-svc"},
			wantCode: http.StatusCreated,
		},
	}

	h := newTestHandler(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodPost, "/services", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

func TestService_DuplicateName(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "dup"})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "dup"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestService_GetUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create
	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)

	// get by id
	rec = doRequest(t, h, http.MethodGet, "/services/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "svc1", got["name"])

	// get not found
	rec = doRequest(t, h, http.MethodGet, "/services/svc-notexist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// update
	rec = doRequest(t, h, http.MethodPatch, "/services/"+id, map[string]any{"authType": "AWS_IAM"})
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, "AWS_IAM", updated["authType"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/services", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/services/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, vpclattice.ServiceCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/services/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestServiceNetwork_CRUD tests service networks.
func TestServiceNetwork_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create
	rec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	assert.Contains(t, created["arn"], "arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-")

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworks/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "sn1", got["name"])

	// update
	rec = doRequest(t, h, http.MethodPatch, "/servicenetworks/"+id, map[string]any{"authType": "AWS_IAM"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworks", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)
	assert.Equal(t, 1, vpclattice.ServiceNetworkCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworks/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, 0, vpclattice.ServiceNetworkCount(h.Backend.(*vpclattice.InMemoryBackend)))
}

// TestSNSA_CRUD tests service network service associations.
func TestSNSA_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create prerequisite resources
	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "net1"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc1"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create association
	rec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assocID, _ := assoc["id"].(string)
	require.NotEmpty(t, assocID)
	assert.Equal(t, "ACTIVE", assoc["status"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// duplicate association returns conflict
	rec = doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkserviceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSNVA_CRUD tests service network VPC associations.
func TestSNVA_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service network
	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "net2"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	// create
	rec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"vpcIdentifier":            "vpc-1234567890",
		"securityGroupIds":         []string{"sg-abcdef01"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assocID, _ := assoc["id"].(string)
	assert.NotEmpty(t, assocID)
	assert.Equal(t, "ACTIVE", assoc["status"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update security groups
	rec = doRequest(t, h, http.MethodPatch, "/servicenetworkvpcassociations/"+assocID, map[string]any{
		"securityGroupIds": []string{"sg-new"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkvpcassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListener_CRUD tests listeners.
func TestListener_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-l"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create listener
	rec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "my-listener",
		"protocol": "HTTP",
		"port":     80,
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	l := parseBody(t, rec)
	listenerID, _ := l["id"].(string)
	require.NotEmpty(t, listenerID)
	assert.Equal(t, "my-listener", l["name"])
	assert.Equal(t, float64(80), l["port"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update
	rec = doRequest(t, h, http.MethodPatch, "/services/"+svcID+"/listeners/"+listenerID, map[string]any{
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 200},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, vpclattice.ListenerCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// list
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/services/"+svcID+"/listeners/"+listenerID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, 0, vpclattice.ListenerCount(h.Backend.(*vpclattice.InMemoryBackend)))
}

// TestRule_CRUD tests listener rules.
func TestRule_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// setup
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-r"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	recL := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "l1",
		"protocol": "HTTP",
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, recL.Code)
	listenerID, _ := parseBody(t, recL)["id"].(string)

	// create target group for forward rule
	recTG := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg1",
		"type": "INSTANCE",
		"config": map[string]any{
			"protocol": "HTTP",
			"port":     80,
			"vpcIdentifier": "vpc-123",
		},
	})
	require.Equal(t, http.StatusCreated, recTG.Code)
	tgID, _ := parseBody(t, recTG)["id"].(string)

	// create rule with forward action
	rec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners/"+listenerID+"/rules", map[string]any{
		"name":     "rule1",
		"priority": 10,
		"action": map[string]any{
			"forward": map[string]any{
				"targetGroups": []any{
					map[string]any{"targetGroupIdentifier": tgID, "weight": 100},
				},
			},
		},
		"match": map[string]any{
			"httpMatch": map[string]any{
				"method": "GET",
				"path": map[string]any{
					"match": map[string]any{"exact": "/api"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	rule := parseBody(t, rec)
	ruleID, _ := rule["id"].(string)
	require.NotEmpty(t, ruleID)
	assert.Equal(t, float64(10), rule["priority"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list (includes default rule)
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 2) // default + created

	// update
	rec = doRequest(t, h, http.MethodPatch, "/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID, map[string]any{
		"priority": 20,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, float64(20), updated["priority"])

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// list now has only default rule
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules", nil)
	list = parseBody(t, rec)
	items, _ = list["items"].([]any)
	assert.Len(t, items, 1)
}

// TestBatchUpdateRule tests batch rule updates.
func TestBatchUpdateRule(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// setup
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-bu"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	recL := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "l-bu",
		"protocol": "HTTP",
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, recL.Code)
	listenerID, _ := parseBody(t, recL)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners/"+listenerID+"/rules", map[string]any{
		"name":     "r1",
		"priority": 10,
		"action":   map[string]any{"fixedResponse": map[string]any{"statusCode": 200}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	ruleID, _ := parseBody(t, rec)["id"].(string)

	// batch update
	rec = doRequest(t, h, http.MethodPatch, "/services/"+svcID+"/listeners/"+listenerID+"/rules", map[string]any{
		"rules": []any{
			map[string]any{"ruleIdentifier": ruleID, "priority": 50},
			map[string]any{"ruleIdentifier": "rule-notexist", "priority": 99},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	successful, _ := resp["successful"].([]any)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	assert.Len(t, successful, 1)
	assert.Len(t, unsuccessful, 1)
}

// TestTargetGroup_CRUD tests target groups.
func TestTargetGroup_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	tests := []struct {
		name     string
		body     map[string]any
		wantCode int
		check    func(t *testing.T, resp map[string]any)
	}{
		{
			name:     "create missing name returns 400",
			body:     map[string]any{"type": "INSTANCE"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create instance target group returns 201",
			body: map[string]any{
				"name": "tg-inst",
				"type": "INSTANCE",
				"config": map[string]any{
					"protocol": "HTTP",
					"port":     8080,
					"vpcIdentifier": "vpc-abc",
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(t, resp["arn"], ":targetgroup/tg-")
				assert.Equal(t, "ACTIVE", resp["status"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h2 := newTestHandler(t)
			rec := doRequest(t, h2, http.MethodPost, "/targetgroups", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}

	// full CRUD test
	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-full",
		"type": "IP",
		"config": map[string]any{
			"protocol": "HTTPS",
			"port":     443,
			"vpcIdentifier": "vpc-xyz",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tg := parseBody(t, rec)
	tgID, _ := tg["id"].(string)
	require.NotEmpty(t, tgID)
	assert.Equal(t, 1, vpclattice.TargetGroupCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// get
	rec = doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/targetgroups", nil)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// update health check
	rec = doRequest(t, h, http.MethodPatch, "/targetgroups/"+tgID, map[string]any{
		"healthCheck": map[string]any{
			"enabled":  true,
			"protocol": "HTTP",
			"path":     "/health",
			"port":     8080,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/targetgroups/"+tgID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, vpclattice.TargetGroupCount(h.Backend.(*vpclattice.InMemoryBackend)))
}

// TestTargets tests register/deregister/list targets.
func TestTargets(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-targets",
		"type": "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// register
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.2", "port": 80},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	assert.Empty(t, unsuccessful)

	// list
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 2)

	// deregister one
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// list after deregister
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", map[string]any{})
	list = parseBody(t, rec)
	items, _ = list["items"].([]any)
	assert.Len(t, items, 1)
}

// TestAccessLogSubscription_CRUD tests access log subscriptions.
func TestAccessLogSubscription_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service for resource
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-als"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create ALS
	destArn := "arn:aws:logs:us-east-1:000000000000:log-group:/vpc-lattice/test"
	rec := doRequest(t, h, http.MethodPost, "/accesslogsubscriptions", map[string]any{
		"resourceIdentifier": svcID,
		"destinationArn":     destArn,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	als := parseBody(t, rec)
	alsID, _ := als["id"].(string)
	require.NotEmpty(t, alsID)
	assert.Equal(t, destArn, als["destinationArn"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/accesslogsubscriptions/"+alsID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update
	newDest := "arn:aws:logs:us-east-1:000000000000:log-group:/vpc-lattice/new"
	rec = doRequest(t, h, http.MethodPatch, "/accesslogsubscriptions/"+alsID, map[string]any{
		"destinationArn": newDest,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, newDest, updated["destinationArn"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/accesslogsubscriptions", nil)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/accesslogsubscriptions/"+alsID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestAuthPolicy tests put/get/delete auth policy.
func TestAuthPolicy(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-auth"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	// put
	rec := doRequest(t, h, http.MethodPut, "/authpolicy/"+svcID, map[string]any{"policy": policy})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestResourcePolicy tests put/get/delete resource policy.
func TestResourcePolicy(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	resArn := "arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-abc123"
	policy := `{"Version":"2012-10-17","Statement":[]}`

	// put
	rec := doRequest(t, h, http.MethodPut, "/resourcepolicy/"+resArn, map[string]any{"policy": policy})
	assert.Equal(t, http.StatusOK, rec.Code)

	// get
	rec = doRequest(t, h, http.MethodGet, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestTagging tests tagging operations.
func TestTagging(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create a service to tag
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{
		"name": "svc-tag",
		"tags": map[string]any{"env": "dev"},
	})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcData := parseBody(t, recSvc)
	svcArn, _ := svcData["arn"].(string)

	// list tags
	rec := doRequest(t, h, http.MethodGet, "/tags/"+svcArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	tagsResp := parseBody(t, rec)
	tags, _ := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "dev", tags["env"])

	// tag resource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+svcArn, map[string]any{
		"tags": map[string]any{"team": "platform"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// verify tag was added
	rec = doRequest(t, h, http.MethodGet, "/tags/"+svcArn, nil)
	tagsResp = parseBody(t, rec)
	tags, _ = tagsResp["tags"].(map[string]any)
	assert.Equal(t, "dev", tags["env"])
	assert.Equal(t, "platform", tags["team"])
}

// TestARNFormat verifies ARN formats for all resource types.
func TestARNFormat(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "arn-svc"})
	require.Equal(t, http.StatusCreated, rec.Code)
	svc := parseBody(t, rec)
	assert.Regexp(t, `^arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-[a-f0-9]+$`, svc["arn"])

	rec = doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "arn-sn"})
	require.Equal(t, http.StatusCreated, rec.Code)
	sn := parseBody(t, rec)
	assert.Regexp(t, `^arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-[a-f0-9]+$`, sn["arn"])

	rec = doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "arn-tg",
		"type": "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tg := parseBody(t, rec)
	assert.Regexp(t, `^arn:aws:vpc-lattice:us-east-1:000000000000:targetgroup/tg-[a-f0-9]+$`, tg["arn"])
}

// TestNotFound verifies 404 on get/update/delete of nonexistent resources.
func TestNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"get service", http.MethodGet, "/services/svc-notexist"},
		{"get servicenetwork", http.MethodGet, "/servicenetworks/sn-notexist"},
		{"get targetgroup", http.MethodGet, "/targetgroups/tg-notexist"},
		{"get snsa", http.MethodGet, "/servicenetworkserviceassociations/snsa-notexist"},
		{"get snva", http.MethodGet, "/servicenetworkvpcassociations/snva-notexist"},
		{"get als", http.MethodGet, "/accesslogsubscriptions/als-notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
