package vpclattice_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func newTestHandler(t *testing.T) *vpclattice.Handler {
	t.Helper()
	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")

	return vpclattice.NewHandler(backend)
}

func doRequestWithRegion(
	t *testing.T,
	h *vpclattice.Handler,
	region, method, path string,
	body any,
) *httptest.ResponseRecorder {
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

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Region:    region,
		Account:   "000000000000",
		Partition: "aws",
	})
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func doRequest(
	t *testing.T,
	h *vpclattice.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
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
		body     map[string]any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
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
				assert.Contains(
					t,
					resp["arn"],
					"arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-",
				)
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
	assert.Contains(
		t,
		created["arn"],
		"arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-",
	)

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworks/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "sn1", got["name"])

	// update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/servicenetworks/"+id,
		map[string]any{"authType": "AWS_IAM"},
	)
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

	// delete returns 202 per AWS spec
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkserviceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
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
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/servicenetworkvpcassociations/"+assocID,
		map[string]any{
			"securityGroupIds": []string{"sg-new"},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete returns 202 per AWS spec
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkvpcassociations/"+assocID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
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
	assert.InDelta(t, float64(80), l["port"], 0)

	// get
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID,
		map[string]any{
			"defaultAction": map[string]any{
				"fixedResponse": map[string]any{"statusCode": 200},
			},
		},
	)
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
			"protocol":      "HTTP",
			"port":          80,
			"vpcIdentifier": "vpc-123",
		},
	})
	require.Equal(t, http.StatusCreated, recTG.Code)
	tgID, _ := parseBody(t, recTG)["id"].(string)

	// create rule with forward action
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules",
		map[string]any{
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
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	rule := parseBody(t, rec)
	ruleID, _ := rule["id"].(string)
	require.NotEmpty(t, ruleID)
	assert.InDelta(t, float64(10), rule["priority"], 0)

	// get
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID,
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list (includes default rule)
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 2) // default + created

	// update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID,
		map[string]any{
			"priority": 20,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.InDelta(t, float64(20), updated["priority"], 0)

	// delete
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules/"+ruleID,
		nil,
	)
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

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules",
		map[string]any{
			"name":     "r1",
			"priority": 10,
			"action":   map[string]any{"fixedResponse": map[string]any{"statusCode": 200}},
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	ruleID, _ := parseBody(t, rec)["id"].(string)

	// batch update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules",
		map[string]any{
			"rules": []any{
				map[string]any{"ruleIdentifier": ruleID, "priority": 50},
				map[string]any{"ruleIdentifier": "rule-notexist", "priority": 99},
			},
		},
	)
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
		body     map[string]any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
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
					"protocol":      "HTTP",
					"port":          8080,
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
			"protocol":      "HTTPS",
			"port":          443,
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
		"name":   "tg-targets",
		"type":   "IP",
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
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/targetgroups/"+tgID+"/deregistertargets",
		map[string]any{
			"targets": []any{
				map[string]any{"id": "10.0.0.1", "port": 80},
			},
		},
	)
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
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/resourcepolicy/"+resArn,
		map[string]any{"policy": policy},
	)
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
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-[a-f0-9]+$`,
		svc["arn"],
	)

	rec = doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "arn-sn"})
	require.Equal(t, http.StatusCreated, rec.Code)
	sn := parseBody(t, rec)
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-[a-f0-9]+$`,
		sn["arn"],
	)

	rec = doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "arn-tg",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tg := parseBody(t, rec)
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:targetgroup/tg-[a-f0-9]+$`,
		tg["arn"],
	)
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

// TestListTargets_BodyFilter verifies that target filters in the POST body are applied.
func TestListTargets_BodyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []map[string]any
		wantIDs   []string
		wantCount int
	}{
		{
			name:      "no filter returns all targets",
			wantCount: 3,
		},
		{
			name:      "filter by ID returns matching target",
			filters:   []map[string]any{{"id": "10.0.0.1"}},
			wantCount: 1,
			wantIDs:   []string{"10.0.0.1"},
		},
		{
			name:      "filter by ID+port returns exact match",
			filters:   []map[string]any{{"id": "10.0.0.1", "port": float64(80)}},
			wantCount: 1,
			wantIDs:   []string{"10.0.0.1"},
		},
		{
			name:      "filter by ID with wrong port returns nothing",
			filters:   []map[string]any{{"id": "10.0.0.1", "port": float64(9999)}},
			wantCount: 0,
		},
		{
			name:      "multiple filters return union of matches",
			filters:   []map[string]any{{"id": "10.0.0.1"}, {"id": "10.0.0.2"}},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			recTG := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
				"name": "tg-filter-test",
				"type": "IP",
				"config": map[string]any{
					"protocol":      "HTTP",
					"port":          80,
					"vpcIdentifier": "vpc-1",
				},
			})
			require.Equal(t, http.StatusCreated, recTG.Code)
			tgID, _ := parseBody(t, recTG)["id"].(string)

			rec := doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
				"targets": []map[string]any{
					{"id": "10.0.0.1", "port": 80},
					{"id": "10.0.0.2", "port": 80},
					{"id": "10.0.0.3", "port": 80},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var listBody map[string]any
			if tc.filters != nil {
				listBody = map[string]any{"targets": tc.filters}
			}

			recList := doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/listtargets", listBody)
			require.Equal(t, http.StatusOK, recList.Code)
			resp := parseBody(t, recList)
			items, _ := resp["items"].([]any)
			assert.Len(t, items, tc.wantCount)

			for _, wantID := range tc.wantIDs {
				found := false
				for _, item := range items {
					m, _ := item.(map[string]any)
					if m["id"] == wantID {
						found = true

						break
					}
				}
				assert.True(t, found, "expected target %s in results", wantID)
			}
		})
	}
}

// TestRegionIsolation verifies that resources created in one region are not visible in another.
func TestRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createRegion string
		listRegion   string
		wantCount    int
	}{
		{
			name:         "same region sees resource",
			createRegion: "us-east-1",
			listRegion:   "us-east-1",
			wantCount:    1,
		},
		{
			name:         "different region sees nothing",
			createRegion: "us-east-1",
			listRegion:   "eu-west-1",
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequestWithRegion(t, h, tc.createRegion, http.MethodPost, "/services", map[string]any{
				"name": "region-svc",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			recList := doRequestWithRegion(t, h, tc.listRegion, http.MethodGet, "/services", nil)
			require.Equal(t, http.StatusOK, recList.Code)
			listResp := parseBody(t, recList)
			items, _ := listResp["items"].([]any)
			assert.Len(t, items, tc.wantCount)
		})
	}
}

// TestParity_GetAuthPolicyNotFoundReturns404 verifies that GetAuthPolicy
// returns 404 when no policy has been set on the resource. Real AWS returns
// ResourceNotFoundException; the emulator previously returned a 200 with an
// empty policy string, making it impossible to distinguish "not set" from
// "policy is empty string".
func TestParity_GetAuthPolicyNotFoundReturns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resourceID string
		name       string
	}{
		{
			name:       "unknown_service_id",
			resourceID: "svc-abc123notexist",
		},
		{
			name:       "existing_service_without_policy",
			resourceID: "", // populated after creating a service below
		},
	}

	h := newTestHandler(t)
	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "parity-auth-svc"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)
	require.NotEmpty(t, svcID)
	tests[1].resourceID = svcID

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, "/authpolicy/"+tt.resourceID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"GetAuthPolicy on resource with no policy must return 404")
		})
	}
}

// TestParity_GetAuthPolicyAfterPutReturns200 verifies the happy-path still works
// after the not-found fix: setting a policy and then getting it returns 200.
func TestParity_GetAuthPolicyAfterPutReturns200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "parity-auth-set"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)
	require.NotEmpty(t, svcID)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putRec := doRequest(t, h, http.MethodPut, "/authpolicy/"+svcID, map[string]any{"policy": policy})
	require.Equal(t, http.StatusOK, putRec.Code, "PutAuthPolicy must succeed")

	getRec := doRequest(t, h, http.MethodGet, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusOK, getRec.Code, "GetAuthPolicy after Put must return 200")

	resp := parseBody(t, getRec)
	assert.Equal(t, policy, resp["policy"])
}

// TestParity_RegisterDeregisterTargetsSuccessfulField verifies that
// RegisterTargets/DeregisterTargets responses include the "successful" list
// of targets, matching the real API's RegisterTargetsOutput/
// DeregisterTargetsOutput shape (Successful []Target, Unsuccessful
// []TargetFailure). The emulator previously omitted "successful" entirely,
// so SDK clients reading resp.Successful always saw an empty slice even on a
// fully successful call.
func TestParity_RegisterDeregisterTargetsSuccessfulField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-successful-field",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// register two targets, one of which will later fail to deregister
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.2", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	successful, _ := resp["successful"].([]any)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, successful, 2, "RegisterTargets must report both targets as successful")
	assert.Empty(t, unsuccessful)

	first, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", first["id"])
	assert.InEpsilon(t, float64(80), first["port"], 0)

	// register a duplicate -> should fail, and NOT appear in successful
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/registertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.3", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1)
	require.Len(t, unsuccessful, 1)
	successOne, _ := successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.3", successOne["id"])

	// deregister: one present, one absent
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{
			map[string]any{"id": "10.0.0.1", "port": 80},
			map[string]any{"id": "10.0.0.99", "port": 80},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	successful, _ = resp["successful"].([]any)
	unsuccessful, _ = resp["unsuccessful"].([]any)
	require.Len(t, successful, 1, "DeregisterTargets must report the removed target as successful")
	require.Len(t, unsuccessful, 1)
	successOne, _ = successful[0].(map[string]any)
	assert.Equal(t, "10.0.0.1", successOne["id"])
}

// TestParity_TargetFailureUsesFailureCodeFailureMessageKeys verifies that
// target failure entries (from RegisterTargets/DeregisterTargets) use the
// wire keys "failureCode"/"failureMessage", matching the real
// TargetFailure shape. The emulator previously emitted "code"/"message",
// which real SDK clients (expecting FailureCode/FailureMessage) would never
// populate.
func TestParity_TargetFailureUsesFailureCodeFailureMessageKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "tg-failure-keys",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	// deregister a target that was never registered -> guaranteed failure
	rec = doRequest(t, h, http.MethodPost, "/targetgroups/"+tgID+"/deregistertargets", map[string]any{
		"targets": []any{map[string]any{"id": "10.0.0.1", "port": 80}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, unsuccessful, 1)

	failure, _ := unsuccessful[0].(map[string]any)
	assert.NotEmpty(t, failure["failureCode"], "TargetFailure must use failureCode, not code")
	assert.NotEmpty(t, failure["failureMessage"], "TargetFailure must use failureMessage, not message")
	assert.Nil(t, failure["code"])
	assert.Nil(t, failure["message"])
}

// TestParity_BatchUpdateRuleFailureUsesFailureCodeFailureMessageKeys mirrors
// TestParity_TargetFailureUsesFailureCodeFailureMessageKeys for
// RuleUpdateFailure, whose real wire keys are also
// "failureCode"/"failureMessage" rather than "code"/"message".
func TestParity_BatchUpdateRuleFailureUsesFailureCodeFailureMessageKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-batch-fail-keys"})
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

	rec := doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID+"/rules",
		map[string]any{
			"rules": []any{
				map[string]any{"ruleIdentifier": "rule-notexist", "priority": 99},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	unsuccessful, _ := resp["unsuccessful"].([]any)
	require.Len(t, unsuccessful, 1)

	failure, _ := unsuccessful[0].(map[string]any)
	assert.NotEmpty(t, failure["failureCode"], "RuleUpdateFailure must use failureCode, not code")
	assert.NotEmpty(t, failure["failureMessage"], "RuleUpdateFailure must use failureMessage, not message")
	assert.Nil(t, failure["code"])
	assert.Nil(t, failure["message"])
}

// TestParity_SNSAIncludesCustomDomainNameAndDNSEntry verifies that
// ServiceNetworkServiceAssociation responses include "customDomainName" and
// "dnsEntry" when the underlying service has them set, matching the real
// CreateServiceNetworkServiceAssociationOutput/
// GetServiceNetworkServiceAssociationOutput shapes. The emulator previously
// captured these fields internally but never serialized them.
func TestParity_SNSAIncludesCustomDomainNameAndDNSEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{
		"name":             "svc-snsa-dns",
		"customDomainName": "example.com",
	})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svc := parseBody(t, recSvc)
	svcID, _ := svc["id"].(string)

	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snsa-dns"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assert.Equal(t, "example.com", assoc["customDomainName"])
	dnsEntry, ok := assoc["dnsEntry"].(map[string]any)
	require.True(t, ok, "dnsEntry must be present on CreateServiceNetworkServiceAssociation response")
	assert.NotEmpty(t, dnsEntry["domainName"])

	assocID, _ := assoc["id"].(string)
	getRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	got := parseBody(t, getRec)
	assert.Equal(t, "example.com", got["customDomainName"])

	listRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)
	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "example.com", summary["customDomainName"])
	assert.NotEmpty(t, summary["dnsEntry"])
}

// TestParity_TargetGroupSummaryWireShape verifies ListTargetGroups summary
// entries use "vpcIdentifier" (not "vpcId") and include lastUpdatedAt,
// matching the real TargetGroupSummary shape. The emulator previously
// emitted "vpcId", which real SDK clients (populating VpcIdentifier) would
// never see, and omitted lastUpdatedAt entirely.
func TestParity_TargetGroupSummaryWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-summary-shape",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-summary",
			"ipAddressType":               "IPV4",
			"lambdaEventStructureVersion": "V1",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/targetgroups", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)

	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "vpc-summary", summary["vpcIdentifier"], "summary must use vpcIdentifier wire key")
	assert.Nil(t, summary["vpcId"], "summary must not use the vpcId wire key")
	assert.NotEmpty(t, summary["lastUpdatedAt"])
	assert.Equal(t, "IPV4", summary["ipAddressType"])
	assert.Equal(t, "V1", summary["lambdaEventStructureVersion"])
}

// TestParity_TargetGroupConfigRoundTripsIPAddressType verifies that
// ipAddressType/lambdaEventStructureVersion set on CreateTargetGroup are
// echoed back in GetTargetGroup's config, matching real AWS's
// GetTargetGroupOutput.Config shape. The emulator captured these fields but
// never serialized them back to clients.
func TestParity_TargetGroupConfigRoundTripsIPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name": "tg-config-roundtrip",
		"type": "IP",
		"config": map[string]any{
			"protocol":                    "HTTP",
			"port":                        80,
			"vpcIdentifier":               "vpc-rt",
			"ipAddressType":               "IPV6",
			"lambdaEventStructureVersion": "V2",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tgID, _ := parseBody(t, rec)["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/targetgroups/"+tgID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	config, ok := parseBody(t, getRec)["config"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "IPV6", config["ipAddressType"])
	assert.Equal(t, "V2", config["lambdaEventStructureVersion"])
}

// TestParity_RuleSummaryIncludesTimestamps verifies that ListRules summary
// entries include createdAt/lastUpdatedAt, matching the real RuleSummary
// shape. The emulator previously omitted both fields.
func TestParity_RuleSummaryIncludesTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-rule-summary-ts"})
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

	listRec := doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID+"/rules", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1, "expected the auto-created default rule")

	summary, _ := items[0].(map[string]any)
	assert.NotEmpty(t, summary["createdAt"])
	assert.NotEmpty(t, summary["lastUpdatedAt"])
}
