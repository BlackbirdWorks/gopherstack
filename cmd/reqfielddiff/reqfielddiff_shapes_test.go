package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func shapeOp(name string, fields ...string) sdkOp {
	op := sdkOp{Name: name}
	for _, f := range fields {
		op.Fields = append(op.Fields, mustField(f, "", false))
	}

	return op
}

func TestResolveOp_FalsePositiveShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		src         string
		op          sdkOp
		wantMissing []string
	}{
		{
			name: "wrapop signature handler behind path-keyed table",
			op:   shapeOp("CreateQuotaShare", "QuotaShareName", "State", "Ghost"),
			src: `package fixture
type Handler struct{}
type createQuotaShareInput struct {
	QuotaShareName string ` + "`json:\"quotaShareName\"`" + `
	State          string ` + "`json:\"state\"`" + `
}
var routes = map[string]service.JSONOpFunc{"/v1/createquotashare": service.WrapOp(h.handleCreateQuotaShare)}
func (h *Handler) handleCreateQuotaShare(
	ctx context.Context,
	in *createQuotaShareInput,
) (*out, error) {
	return nil, nil
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "non-context first param is not a request signature",
			op:   shapeOp("CreateThing", "Name", "State"),
			src: `package fixture
type Handler struct{}
type thingModel struct{ Name string; State string }
func (h *Handler) handleCreateThing(c *echo.Context, m *thingModel) error { return nil }`,
			wantMissing: []string{"Name", "State"},
		},
		{
			name: "embedded struct fields are promoted",
			op:   shapeOp("CreateProject", "Name", "SourceVersion", "TimeoutInMinutes", "Ghost"),
			src: `package fixture
type Handler struct{}
type baseFields struct{ TimeoutInMinutes int ` + "`json:\"timeoutInMinutes\"`" + ` }
type projectConfigFields struct {
	*baseFields
	SourceVersion string ` + "`json:\"sourceVersion\"`" + `
}
type createProjectInput struct {
	projectConfigFields
	Name string ` + "`json:\"name\"`" + `
}
func (h *Handler) handleCreateProject(ctx context.Context, in *createProjectInput) (*out, error) { return nil, nil }`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "tagged embedded struct is a named field not promoted",
			op:   shapeOp("CreateProject", "Name", "SourceVersion"),
			src: `package fixture
type Handler struct{}
type inner struct{ SourceVersion string }
type createProjectInput struct {
	inner ` + "`json:\"inner\"`" + `
	Name string ` + "`json:\"name\"`" + `
}
func (h *Handler) handleCreateProject(ctx context.Context, in *createProjectInput) (*out, error) { return nil, nil }`,
			wantMissing: []string{"SourceVersion"},
		},
		{
			name: "unrelated switch does not override map dispatch",
			op:   shapeOp("DescribeEndpoints", "MaxRecords", "Ghost"),
			src: `package fixture
type Handler struct{}
type describeEndpointsInput struct{ MaxRecords *int32 ` + "`json:\"MaxRecords\"`" + ` }
const opDescribeEndpoints = "DescribeEndpoints"
func (h *Handler) ops() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{opDescribeEndpoints: service.WrapOp(h.handleDescribeEndpoints)}
}
func extractCoreResourceField(c *echo.Context, action string) string {
	switch action {
	case opDescribeEndpoints:
		return extractField(c, "EndpointIdentifier", "EndpointArn")
	}
	return ""
}
func (h *Handler) handleDescribeEndpoints(
	ctx context.Context,
	in *describeEndpointsInput,
) (*out, error) {
	return nil, nil
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "explicit generic instantiation decode",
			op:   shapeOp("UpdateInstanceMetadataOptions", "HttpEndpoint", "Ghost"),
			src: `package fixture
type Handler struct{}
type updateReq struct{ HTTPEndpoint string ` + "`json:\"httpEndpoint\"`" + ` }
func decodeBody[T any](body []byte) (*T, error) {
	var v T
	err := json.Unmarshal(body, &v)
	return &v, err
}
func (h *Handler) handleUpdateInstanceMetadataOptions(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateReq](body)
	_, _ = req, err
	return nil, nil
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "generic instantiation of a non-decoder is ignored",
			op:   shapeOp("UpdateThing", "HttpEndpoint"),
			src: `package fixture
type Handler struct{}
type updateReq struct{ HTTPEndpoint string }
func firstOf[T any](xs []T) T { var z T; return z }
func (h *Handler) handleUpdateThing(_ context.Context, body []byte) ([]byte, error) {
	_ = firstOf[updateReq](nil)
	return nil, nil
}`,
			wantMissing: []string{"HttpEndpoint"},
		},
		{
			name: "header names read through a helper taking the request header",
			op:   shapeOp("CreateMultipartUpload", "GrantRead", "GrantReadACP", "Ghost"),
			src: `package fixture
type Handler struct{}
func grantsFromHeaders(h http.Header) []string {
	_ = h.Get("X-Amz-Grant-Read")
	_ = h.Get("X-Amz-Grant-Read-Acp")
	return nil
}
func aclXMLFromGrantHeaders(h http.Header) string { _ = grantsFromHeaders(h); return "" }
func (h *Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	_ = aclXMLFromGrantHeaders(r.Header)
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "helper not handed the header reads nothing",
			op:   shapeOp("CreateMultipartUpload", "GrantRead"),
			src: `package fixture
type Handler struct{}
func other(h http.Header) { _ = h.Get("X-Amz-Grant-Read") }
func (h *Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	other(nil)
}`,
			wantMissing: []string{"GrantRead"},
		},
		{
			name: "form value with sprintf prefix",
			op:   shapeOp("CreateTopic", "Attributes", "Ghost"),
			src: `package fixture
type Handler struct{}
func extractFormAttributes(c *echo.Context) map[string]string {
	_ = c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.key", 1))
	return nil
}
func (h *Handler) handleCreateTopic(c *echo.Context) error {
	_ = extractFormAttributes(c)
	return nil
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "request Form.Get read",
			op:   shapeOp("CreateTopic", "DisplayName", "Ghost"),
			src: `package fixture
type Handler struct{}
func (h *Handler) handleCreateTopic(r *http.Request) error {
	_ = r.PostForm.Get("DisplayName")
	return nil
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "query key forwarded through nested helper",
			op:   shapeOp("DescribeAccessPoints", "MaxResults", "NextToken", "Ghost"),
			src: `package fixture
type Handler struct{}
func queryInt(c *echo.Context, key string) int { return len(c.QueryParam(key)) }
func describeList(c *echo.Context, markerKey, maxKey string) error {
	_ = c.Request().URL.Query().Get(markerKey)
	_ = queryInt(c, maxKey)
	return nil
}
func (h *Handler) handleDescribeAccessPoints(c *echo.Context) error {
	return describeList(c, "NextToken", "MaxResults")
}`,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "sdk input type decoded directly",
			op:   shapeOp("GetRecords", "ShardIterator", "Limit"),
			src: `package fixture
type Handler struct{}
func (h *Handler) dispatch(operation string, body []byte) (any, error) {
	switch operation {
	case "GetRecords":
		return dispatchGetRecords(body, nil)
	}
	return nil, nil
}
func dispatchGetRecords(body []byte, op any) (any, error) {
	var input dynamodbstreams.GetRecordsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}
	return nil, nil
}`,
			wantMissing: nil,
		},
		{
			name: "sdk input type of another op is not credited",
			op:   shapeOp("GetRecords", "ShardIterator"),
			src: `package fixture
type Handler struct{}
func (h *Handler) handleGetRecords(body []byte) (any, error) {
	var input dynamodbstreams.ListStreamsInput
	_ = json.Unmarshal(body, &input)
	return nil, nil
}`,
			wantMissing: []string{"ShardIterator"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := parseSrc(t, tt.src).resolveOps([]sdkOp{tt.op})[tt.op.Name]
			assert.ElementsMatch(t, tt.wantMissing, missingNames(tt.op, res))
		})
	}
}

func TestResolveOp_SecondDispatchTable(t *testing.T) {
	t.Parallel()

	const src = `package fixture
type Handler struct{}
type ServerlessHandler struct{}
type classicIn struct{ ScheduledActionName string; IamRole string }
type serverlessIn struct{ NamespaceName string; RoleArn string }
const opCreateScheduledAction = "CreateScheduledAction"
func (h *Handler) table() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{opCreateScheduledAction: service.WrapOp(h.handleCreateScheduledAction)}
}
var slTable = map[string]func(*ServerlessHandler, []byte) error{
	opCreateScheduledAction: (*ServerlessHandler).handleCreateScheduledAction,
}
func (h *Handler) handleCreateScheduledAction(ctx context.Context, in *classicIn) (*out, error) { return nil, nil }
func (h *ServerlessHandler) handleCreateScheduledAction(ctx context.Context, in *serverlessIn) error { return nil }`

	tests := []struct {
		name        string
		op          sdkOp
		wantMissing []string
	}{
		{
			name:        "classic op resolves to the classic handler only",
			op:          shapeOp("CreateScheduledAction", "ScheduledActionName", "IamRole", "NamespaceName"),
			wantMissing: []string{"NamespaceName"},
		},
		{
			name:        "serverless-shaped op resolves to the serverless handler only",
			op:          shapeOp("CreateScheduledAction", "NamespaceName", "RoleArn", "ScheduledActionName"),
			wantMissing: []string{"ScheduledActionName"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := parseSrc(t, src).resolveOps([]sdkOp{tt.op})[tt.op.Name]
			assert.ElementsMatch(t, tt.wantMissing, missingNames(tt.op, res))
		})
	}
}
