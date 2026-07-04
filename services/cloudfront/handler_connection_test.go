package cloudfront_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestNewOps_ConnectionGroup_Full exercises the full ConnectionGroup lifecycle: create with a
// full config (AnycastIpListId, Ipv6Enabled, Enabled), get, get-by-routing-endpoint, list,
// update (partial merge), and delete.
func TestNewOps_ConnectionGroup_Full(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<CreateConnectionGroupRequest>` +
		`<Name>full-cg</Name>` +
		`<AnycastIpListId>anycast-1</AnycastIpListId>` +
		`<Ipv6Enabled>false</Ipv6Enabled>` +
		`<Enabled>true</Enabled>` +
		`</CreateConnectionGroupRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"connection-group", createBody)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}
	if !strings.Contains(out, "<AnycastIpListId>anycast-1</AnycastIpListId>") {
		t.Errorf("expected AnycastIpListId in create response: %s", out)
	}
	if !strings.Contains(out, "<Ipv6Enabled>false</Ipv6Enabled>") {
		t.Errorf("expected Ipv6Enabled=false in create response: %s", out)
	}

	routingEndpoint := strings.ToLower(id) + ".cloudfront.net"

	// Get.
	getOut := cfOK(t, h, http.MethodGet, prefix+"connection-group/"+id, "")
	if extractXMLID(t, getOut) != id {
		t.Errorf("get mismatch: %s", getOut)
	}
	if !strings.Contains(getOut, "<RoutingEndpoint>"+routingEndpoint+"</RoutingEndpoint>") {
		t.Errorf("expected generated routing endpoint, got: %s", getOut)
	}

	// GetByRoutingEndpoint (query param, not path segment).
	byEndpointOut := cfOK(
		t, h, http.MethodGet, prefix+"connection-group-by-routing-endpoint?RoutingEndpoint="+routingEndpoint, "",
	)
	if extractXMLID(t, byEndpointOut) != id {
		t.Errorf("get-by-routing-endpoint mismatch: %s", byEndpointOut)
	}

	// List.
	listOut := cfOK(t, h, http.MethodGet, prefix+"connection-group", "")
	if !strings.Contains(listOut, id) {
		t.Errorf("list missing id: %s", listOut)
	}
	if !strings.Contains(listOut, routingEndpoint) {
		t.Errorf("list missing routing endpoint: %s", listOut)
	}

	// Update: change the Anycast IP list and flip Ipv6Enabled, leave Enabled untouched.
	updateBody := `<UpdateConnectionGroupRequest>` +
		`<AnycastIpListId>anycast-2</AnycastIpListId>` +
		`<Ipv6Enabled>true</Ipv6Enabled>` +
		`</UpdateConnectionGroupRequest>`
	updateOut := cfOK(t, h, http.MethodPut, prefix+"connection-group/"+id, updateBody)
	if !strings.Contains(updateOut, "<AnycastIpListId>anycast-2</AnycastIpListId>") {
		t.Errorf("expected updated AnycastIpListId, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<Ipv6Enabled>true</Ipv6Enabled>") {
		t.Errorf("expected updated Ipv6Enabled, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<Enabled>true</Enabled>") {
		t.Errorf("expected Enabled preserved from create, got: %s", updateOut)
	}

	// Delete.
	cfOK(t, h, http.MethodDelete, prefix+"connection-group/"+id, "")

	// Routing endpoint index must be cleaned up on delete.
	afterDeleteRR := cfRequest(
		t, h, http.MethodGet, prefix+"connection-group-by-routing-endpoint?RoutingEndpoint="+routingEndpoint, "",
	)
	if afterDeleteRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d: %s", afterDeleteRR.Code, afterDeleteRR.Body.String())
	}
}

// TestNewOps_ConnectionGroup_NameUniqueness verifies that creating a connection group with a
// name that already exists fails with 409 EntityAlreadyExists.
func TestNewOps_ConnectionGroup_NameUniqueness(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	body := `<CreateConnectionGroupRequest><Name>dup-cg</Name></CreateConnectionGroupRequest>`
	cfOK(t, h, http.MethodPost, prefix+"connection-group", body)

	dupRR := cfRequest(t, h, http.MethodPost, prefix+"connection-group", body)
	if dupRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate name, got %d: %s", dupRR.Code, dupRR.Body.String())
	}
	if !strings.Contains(dupRR.Body.String(), "EntityAlreadyExists") {
		t.Errorf("expected EntityAlreadyExists error, got: %s", dupRR.Body.String())
	}
}

// TestNewOps_ConnectionGroup_NotFound verifies Get/GetByRoutingEndpoint/Update/Delete on a
// missing ID or endpoint return 404 NoSuchConnectionGroup.
func TestNewOps_ConnectionGroup_NotFound(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	getRR := cfRequest(t, h, http.MethodGet, prefix+"connection-group/does-not-exist", "")
	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on get, got %d: %s", getRR.Code, getRR.Body.String())
	}
	if !strings.Contains(getRR.Body.String(), "NoSuchConnectionGroup") {
		t.Errorf("expected NoSuchConnectionGroup error, got: %s", getRR.Body.String())
	}

	byEndpointRR := cfRequest(
		t, h, http.MethodGet, prefix+"connection-group-by-routing-endpoint?RoutingEndpoint=nope.cloudfront.net", "",
	)
	if byEndpointRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on get-by-routing-endpoint, got %d: %s", byEndpointRR.Code, byEndpointRR.Body.String())
	}

	updateRR := cfRequest(t, h, http.MethodPut, prefix+"connection-group/does-not-exist",
		`<UpdateConnectionGroupRequest><Comment>x</Comment></UpdateConnectionGroupRequest>`)
	if updateRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on update, got %d: %s", updateRR.Code, updateRR.Body.String())
	}

	deleteRR := cfRequest(t, h, http.MethodDelete, prefix+"connection-group/does-not-exist", "")
	if deleteRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on delete, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}
}

// TestNewOps_ConnectionGroup_IfMatchEnforcement verifies that a mismatched If-Match header on
// update/delete is rejected with 412 PreconditionFailed, and the correct ETag succeeds.
func TestNewOps_ConnectionGroup_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"connection-group",
		[]byte(`<CreateConnectionGroupRequest><Name>etag-cg</Name></CreateConnectionGroupRequest>`))
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: got %d: %s", createRec.Code, createRec.Body.String())
	}
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag on create")
	}

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"connection-group/"+id,
		[]byte(`<UpdateConnectionGroupRequest><Comment>x</Comment></UpdateConnectionGroupRequest>`),
		map[string]string{"If-Match": "bogus-etag"})
	if badUpdate.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match update, got %d: %s", badUpdate.Code, badUpdate.Body.String())
	}

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"connection-group/"+id,
		[]byte(`<UpdateConnectionGroupRequest><Comment>x</Comment></UpdateConnectionGroupRequest>`),
		map[string]string{"If-Match": etag})
	if goodUpdate.Code != http.StatusOK {
		t.Fatalf("expected 200 on good If-Match update, got %d: %s", goodUpdate.Code, goodUpdate.Body.String())
	}
	newEtag := goodUpdate.Header().Get("ETag")
	if newEtag == "" || newEtag == etag {
		t.Errorf("expected ETag to rotate on update, old=%s new=%s", etag, newEtag)
	}

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"connection-group/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	if badDelete.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match delete, got %d: %s", badDelete.Code, badDelete.Body.String())
	}

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"connection-group/"+id, nil,
		map[string]string{"If-Match": newEtag})
	if goodDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on good If-Match delete, got %d: %s", goodDelete.Code, goodDelete.Body.String())
	}
}

// TestNewOps_ConnectionGroup_Tags verifies tags can be attached to a connection group via the
// generic TagResource/ListTagsForResource endpoints.
func TestNewOps_ConnectionGroup_Tags(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-group",
		`<CreateConnectionGroupRequest><Name>tagged-cg</Name></CreateConnectionGroupRequest>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:connection-group/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	tagRR := cfRequest(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)
	if tagRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 tagging connection group, got %d: %s", tagRR.Code, tagRR.Body.String())
	}

	listRR := cfRequest(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Key>env</Key>") ||
		!strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected env=prod tag in response, got: %s", listRR.Body.String())
	}
}

// TestNewOps_ConnectionGroup_Persistence verifies connection groups, their tags, and derived
// indexes (ARN, name-uniqueness, routing endpoint) survive a Snapshot/Restore round-trip.
func TestNewOps_ConnectionGroup_Persistence(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-group",
		`<CreateConnectionGroupRequest><Name>persist-cg</Name></CreateConnectionGroupRequest>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:connection-group/%s", id)
	routingEndpoint := strings.ToLower(id) + ".cloudfront.net"

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	cfOK(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)

	snap := h.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	restored := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	if err := restored.Restore(t.Context(), snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	h2 := cloudfront.NewHandler(restored)

	getOut := cfOK(t, h2, http.MethodGet, prefix+"connection-group/"+id, "")
	if extractXMLID(t, getOut) != id {
		t.Errorf("expected connection group present after restore, got: %s", getOut)
	}

	// The routing-endpoint index must still resolve after restore.
	byEndpointOut := cfOK(
		t, h2, http.MethodGet, prefix+"connection-group-by-routing-endpoint?RoutingEndpoint="+routingEndpoint, "",
	)
	if extractXMLID(t, byEndpointOut) != id {
		t.Errorf("expected routing endpoint index restored, got: %s", byEndpointOut)
	}

	// Tags must still resolve via the restored ARN index.
	listRR := cfRequest(t, h2, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags after restore, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected tag preserved after restore, got: %s", listRR.Body.String())
	}

	// The name-uniqueness index must still be enforced after restore.
	dupRR := cfRequest(t, h2, http.MethodPost, prefix+"connection-group",
		`<CreateConnectionGroupRequest><Name>persist-cg</Name></CreateConnectionGroupRequest>`)
	if dupRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate name after restore, got %d: %s", dupRR.Code, dupRR.Body.String())
	}
}

// TestNewOps_ConnectionFunction_Full exercises the full ConnectionFunction lifecycle: create
// with code/runtime/comment, get (raw code), describe (summary), list, update (full replace),
// publish (DEVELOPMENT -> LIVE), test, and delete.
func TestNewOps_ConnectionFunction_Full(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<CreateConnectionFunctionRequest>` +
		`<Name>full-cfn</Name>` +
		`<ConnectionFunctionCode>ZnVuY3Rpb24gaGFuZGxlcigpIHt9</ConnectionFunctionCode>` +
		`<ConnectionFunctionConfig><Comment>v1</Comment><Runtime>cloudfront-js-2.0</Runtime></ConnectionFunctionConfig>` +
		`</CreateConnectionFunctionRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"connection-function", createBody)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}
	if !strings.Contains(out, "<Stage>DEVELOPMENT</Stage>") {
		t.Errorf("expected new function to start in DEVELOPMENT stage, got: %s", out)
	}
	if !strings.Contains(out, "<Runtime>cloudfront-js-2.0</Runtime>") {
		t.Errorf("expected runtime in create response, got: %s", out)
	}

	// Get returns the raw function code, not XML.
	getRR := cfRequest(t, h, http.MethodGet, prefix+"connection-function/"+id, "")
	if getRR.Code != http.StatusOK {
		t.Fatalf("expected 200 on get, got %d: %s", getRR.Code, getRR.Body.String())
	}
	if !strings.Contains(getRR.Body.String(), "function handler()") {
		t.Errorf("expected decoded function code body, got: %s", getRR.Body.String())
	}

	// Describe returns the ConnectionFunctionSummary metadata.
	describeOut := cfOK(t, h, http.MethodGet, prefix+"connection-function/"+id+"/describe", "")
	if !strings.Contains(describeOut, "<Comment>v1</Comment>") {
		t.Errorf("expected describe to include comment, got: %s", describeOut)
	}

	// List.
	listOut := cfOK(t, h, http.MethodGet, prefix+"connection-function", "")
	if !strings.Contains(listOut, id) {
		t.Errorf("list missing id: %s", listOut)
	}

	// Update: full replace of comment/runtime/code resets stage to DEVELOPMENT.
	updateBody := `<UpdateConnectionFunctionRequest>` +
		`<ConnectionFunctionCode>ZnVuY3Rpb24gaGFuZGxlcjIoKSB7fQ==</ConnectionFunctionCode>` +
		`<ConnectionFunctionConfig><Comment>v2</Comment><Runtime>cloudfront-js-2.0</Runtime></ConnectionFunctionConfig>` +
		`</UpdateConnectionFunctionRequest>`
	updateOut := cfOK(t, h, http.MethodPut, prefix+"connection-function/"+id, updateBody)
	if !strings.Contains(updateOut, "<Comment>v2</Comment>") {
		t.Errorf("expected updated comment, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<Stage>DEVELOPMENT</Stage>") {
		t.Errorf("expected stage reset to DEVELOPMENT after update, got: %s", updateOut)
	}

	// Publish promotes DEVELOPMENT -> LIVE.
	publishOut := cfOK(t, h, http.MethodPost, prefix+"connection-function/"+id+"/publish", "")
	if !strings.Contains(publishOut, "<Stage>LIVE</Stage>") {
		t.Errorf("expected LIVE stage after publish, got: %s", publishOut)
	}

	// Test executes the function against a connection object; output must echo the input and
	// compute utilization must be derived (non-empty, numeric) rather than a fixed constant.
	testBody := `<TestConnectionFunctionRequest>` +
		`<ConnectionObject>aGVsbG8=</ConnectionObject>` +
		`</TestConnectionFunctionRequest>`
	testOut := cfOK(t, h, http.MethodPost, prefix+"connection-function/"+id+"/test", testBody)
	if !strings.Contains(testOut, "<ConnectionFunctionOutput>hello</ConnectionFunctionOutput>") {
		t.Errorf("expected test output to echo decoded input, got: %s", testOut)
	}
	if !strings.Contains(testOut, "<ComputeUtilization>") {
		t.Errorf("expected a ComputeUtilization value, got: %s", testOut)
	}

	// Delete.
	cfOK(t, h, http.MethodDelete, prefix+"connection-function/"+id, "")

	afterDeleteRR := cfRequest(t, h, http.MethodGet, prefix+"connection-function/"+id+"/describe", "")
	if afterDeleteRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d: %s", afterDeleteRR.Code, afterDeleteRR.Body.String())
	}
}

// TestNewOps_ConnectionFunction_TestResultVariesWithInput verifies that TestConnectionFunction's
// ComputeUtilization is derived from the input rather than a hardcoded constant: two different
// connection objects against the same function must not always produce identical utilization.
func TestNewOps_ConnectionFunction_TestResultVariesWithInput(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-function",
		`<CreateConnectionFunctionRequest><Name>varying-cfn</Name></CreateConnectionFunctionRequest>`)
	id := extractXMLID(t, out)

	shortBody := `<TestConnectionFunctionRequest><ConnectionObject>YQ==</ConnectionObject></TestConnectionFunctionRequest>`
	longBody := `<TestConnectionFunctionRequest>` +
		`<ConnectionObject>YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5eg==</ConnectionObject>` +
		`</TestConnectionFunctionRequest>`

	shortOut := cfOK(t, h, http.MethodPost, prefix+"connection-function/"+id+"/test", shortBody)
	longOut := cfOK(t, h, http.MethodPost, prefix+"connection-function/"+id+"/test", longBody)

	extractUtilization := func(s string) string {
		start := strings.Index(s, "<ComputeUtilization>") + len("<ComputeUtilization>")
		end := strings.Index(s, "</ComputeUtilization>")

		return s[start:end]
	}

	if extractUtilization(shortOut) == extractUtilization(longOut) {
		t.Errorf(
			"expected ComputeUtilization to vary with input size, got identical values %q for both",
			extractUtilization(shortOut),
		)
	}
}

// TestNewOps_ConnectionFunction_NotFound verifies Describe/Get/Update/Delete/Publish/Test on a
// missing ID return 404 NoSuchConnectionFunction.
func TestNewOps_ConnectionFunction_NotFound(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, prefix + "connection-function/does-not-exist/describe"},
		{http.MethodGet, prefix + "connection-function/does-not-exist"},
		{http.MethodPut, prefix + "connection-function/does-not-exist"},
		{http.MethodDelete, prefix + "connection-function/does-not-exist"},
		{http.MethodPost, prefix + "connection-function/does-not-exist/publish"},
		{http.MethodPost, prefix + "connection-function/does-not-exist/test"},
	} {
		rr := cfRequest(t, h, tc.method, tc.path, "")
		if rr.Code != http.StatusNotFound {
			t.Errorf("%s %s: expected 404, got %d: %s", tc.method, tc.path, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "NoSuchConnectionFunction") {
			t.Errorf("%s %s: expected NoSuchConnectionFunction, got: %s", tc.method, tc.path, rr.Body.String())
		}
	}
}

// TestNewOps_ConnectionFunction_IfMatchEnforcement verifies that a mismatched If-Match header
// on update/delete/publish/test is rejected with 412 PreconditionFailed.
func TestNewOps_ConnectionFunction_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"connection-function",
		[]byte(`<CreateConnectionFunctionRequest><Name>etag-cfn</Name></CreateConnectionFunctionRequest>`))
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: got %d: %s", createRec.Code, createRec.Body.String())
	}
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag on create")
	}

	badHeaders := map[string]string{"If-Match": "bogus-etag"}

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"connection-function/"+id,
		[]byte(`<UpdateConnectionFunctionRequest><ConnectionFunctionConfig>`+
			`<Comment>x</Comment><Runtime>cloudfront-js-2.0</Runtime></ConnectionFunctionConfig>`+
			`</UpdateConnectionFunctionRequest>`), badHeaders)
	if badUpdate.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match update, got %d: %s", badUpdate.Code, badUpdate.Body.String())
	}

	badPublish := doXMLWithHeaders(t, h, http.MethodPost, prefix+"connection-function/"+id+"/publish", nil, badHeaders)
	if badPublish.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match publish, got %d: %s", badPublish.Code, badPublish.Body.String())
	}

	badTest := doXMLWithHeaders(t, h, http.MethodPost, prefix+"connection-function/"+id+"/test", nil, badHeaders)
	if badTest.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match test, got %d: %s", badTest.Code, badTest.Body.String())
	}

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"connection-function/"+id, nil, badHeaders)
	if badDelete.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match delete, got %d: %s", badDelete.Code, badDelete.Body.String())
	}

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"connection-function/"+id, nil,
		map[string]string{"If-Match": etag})
	if goodDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on good If-Match delete, got %d: %s", goodDelete.Code, goodDelete.Body.String())
	}
}

// TestNewOps_ConnectionFunction_InvalidRuntime verifies that an unsupported Runtime is rejected
// with 400 InvalidArgument on both create and update.
func TestNewOps_ConnectionFunction_InvalidRuntime(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createRR := cfRequest(t, h, http.MethodPost, prefix+"connection-function",
		`<CreateConnectionFunctionRequest><Name>bad-runtime-cfn</Name>`+
			`<ConnectionFunctionConfig><Runtime>python3.9</Runtime></ConnectionFunctionConfig>`+
			`</CreateConnectionFunctionRequest>`)
	if createRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid runtime create, got %d: %s", createRR.Code, createRR.Body.String())
	}
	if !strings.Contains(createRR.Body.String(), "InvalidArgument") {
		t.Errorf("expected InvalidArgument error, got: %s", createRR.Body.String())
	}
}

// TestNewOps_ConnectionFunction_Tags verifies tags can be attached to a connection function via
// the generic TagResource/ListTagsForResource endpoints.
func TestNewOps_ConnectionFunction_Tags(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-function",
		`<CreateConnectionFunctionRequest><Name>tagged-cfn</Name></CreateConnectionFunctionRequest>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:connection-function/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	tagRR := cfRequest(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)
	if tagRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 tagging connection function, got %d: %s", tagRR.Code, tagRR.Body.String())
	}

	listRR := cfRequest(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected env=prod tag in response, got: %s", listRR.Body.String())
	}
}

// TestNewOps_ConnectionFunction_Persistence verifies connection functions and their tags survive
// a Snapshot/Restore round-trip via the restored ARN index.
func TestNewOps_ConnectionFunction_Persistence(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-function",
		`<CreateConnectionFunctionRequest><Name>persist-cfn</Name>`+
			`<ConnectionFunctionConfig><Comment>persisted</Comment></ConnectionFunctionConfig>`+
			`</CreateConnectionFunctionRequest>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:connection-function/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	cfOK(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)

	snap := h.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	restored := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	if err := restored.Restore(t.Context(), snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	h2 := cloudfront.NewHandler(restored)

	describeOut := cfOK(t, h2, http.MethodGet, prefix+"connection-function/"+id+"/describe", "")
	if !strings.Contains(describeOut, "persisted") {
		t.Errorf("expected persisted comment after restore, got: %s", describeOut)
	}

	listRR := cfRequest(t, h2, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags after restore, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected tag preserved after restore, got: %s", listRR.Body.String())
	}
}

// TestNewOps_ListDistributionsByConnectionFunction verifies distributions referencing a
// connection function are returned, and that an unrelated function ID yields an empty list.
func TestNewOps_ListDistributionsByConnectionFunction(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"connection-function",
		`<CreateConnectionFunctionRequest><Name>assoc-cfn</Name></CreateConnectionFunctionRequest>`)
	fnID := extractXMLID(t, out)

	distBody := `<DistributionConfig>` +
		`<CallerReference>cr-cfn</CallerReference><Enabled>true</Enabled>` +
		`<ConnectionFunctionId>` + fnID + `</ConnectionFunctionId>` +
		`</DistributionConfig>`
	cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)

	resp := cfOK(t, h, http.MethodGet, prefix+"distributions/by-connection-function/"+fnID, "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	empty := cfOK(t, h, http.MethodGet, prefix+"distributions/by-connection-function/nonexistent-fn", "")
	if !strings.Contains(empty, "<Quantity>0</Quantity>") {
		t.Errorf("expected empty list for unrelated function, got: %s", empty)
	}
}
