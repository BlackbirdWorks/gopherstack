package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadSerializerFixture(t *testing.T, name string) map[string]*opWire {
	t.Helper()

	src, err := os.ReadFile(filepath.Join("testdata", "serializers", name))
	require.NoError(t, err)

	return parseWireKeys(string(src))
}

func missingNames(op sdkOp, res opResolution) []string {
	missing := findMissing(op, res)
	out := make([]string, 0, len(missing))

	for _, m := range missing {
		out = append(out, m.Field.Name)
	}

	return out
}

func TestParseWireKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		file        string
		op          string
		wantKeys    map[string][]string
		wantPayload []string
	}{
		{
			name: "ec2query flat keys",
			file: "ec2query.fixture",
			op:   "ModifyWidget",
			wantKeys: map[string][]string{
				"Description": {"Description"},
				"Filters":     {"Filter"},
				"Groups":      {"SecurityGroupId"},
				"Latest":      {"Latest"},
			},
		},
		{
			name: "awsquery member lists",
			file: "awsquery.fixture",
			op:   "CreateGadget",
			wantKeys: map[string][]string{
				"Name":                {"Name"},
				"Tags":                {"Tags"},
				"VpcSecurityGroupIds": {"VpcSecurityGroupIds"},
			},
		},
		{
			name: "restxml header query and payload",
			file: "restxml.fixture",
			op:   "PutThing",
			wantKeys: map[string][]string{
				"ServerSideEncryption": {"Server-Side-Encryption"},
				"MaxKeys":              {"max-keys"},
			},
			wantPayload: []string{"ThingConfiguration"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := loadSerializerFixture(t, tt.file)[tt.op]
			require.NotNil(t, w)
			assert.Equal(t, tt.wantKeys, w.Keys)

			for _, p := range tt.wantPayload {
				assert.True(t, w.Payload[p], p)
			}
		})
	}
}

func TestResolveOp_QueryProtocolReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		op      string
		fields  []string
		src     string
		missing []string
	}{
		{
			name:   "ec2 flat list under singular wire key",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Groups"},
			src: `package fixture
type Handler struct{}
func parseMemberList(vals url.Values, prefix string) []string { return nil }
func (h *Handler) handleModifyWidget(vals url.Values) (any, error) {
	_ = parseMemberList(vals, "SecurityGroupId")
	return nil, nil
}`,
		},
		{
			name:   "ec2 nested filter via sprintf helper",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Filters"},
			src: `package fixture
type Handler struct{}
func parseFilters(vals url.Values) []string {
	for i := 1; ; i++ {
		_ = vals.Get(fmt.Sprintf("Filter.%d.Name", i))
	}
}
func (h *Handler) handleModifyWidget(vals url.Values) (any, error) {
	_ = parseFilters(vals)
	return nil, nil
}`,
		},
		{
			name:   "awsquery member list via prefix argument",
			file:   "awsquery.fixture",
			op:     "CreateGadget",
			fields: []string{"Tags", "VpcSecurityGroupIds"},
			src: `package fixture
type Handler struct{}
func parseMembers(vals url.Values, prefix string) []string { return nil }
func (h *Handler) handleCreateGadget(vals url.Values) (any, error) {
	_ = parseMembers(vals, "Tags.Tag")
	_ = parseMembers(vals, "VpcSecurityGroupIds.VpcSecurityGroupId")
	return nil, nil
}`,
		},
		{
			name:   "nested map entry keys",
			file:   "awsquery.fixture",
			op:     "CreateGadget",
			fields: []string{"Tags"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handleCreateGadget(vals url.Values) (any, error) {
	_ = vals.Get("Tags.member.1.Key")
	return nil, nil
}`,
		},
		{
			name:   "helper takes url.Values at a later position",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Latest"},
			src: `package fixture
type Handler struct{}
func parseLatest(c *echo.Context, form url.Values) bool { return form.Get("Latest") == "true" }
func (h *Handler) handleModifyWidget(c *echo.Context, vals url.Values) error {
	_ = parseLatest(c, vals)
	return nil
}`,
		},
		{
			name:   "method helper on the handler",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Latest"},
			src: `package fixture
type Handler struct{}
func (h *Handler) latest(vals url.Values) bool { return vals.Get("Latest") == "true" }
func (h *Handler) handleModifyWidget(vals url.Values) error {
	_ = h.latest(vals)
	return nil
}`,
		},
		{
			name:   "values from ParseQuery local",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handleModifyWidget(raw string) error {
	q, _ := url.ParseQuery(raw)
	_ = q.Get("Description")
	return nil
}`,
		},
		{
			name:   "values from ParseFormBody local",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handleModifyWidget(r *http.Request) error {
	form, _ := httputils.ParseFormBody(r)
	_ = form.Get("Description")
	return nil
}`,
		},
		{
			name:   "values from a package func returning url.Values",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description"},
			src: `package fixture
type Handler struct{}
func formOf(r *http.Request) (url.Values, error) { return nil, nil }
func (h *Handler) handleModifyWidget(r *http.Request) error {
	f, _ := formOf(r)
	_ = f.Get("Description")
	return nil
}`,
		},
		{
			name:   "package const key",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Latest"},
			src: `package fixture
type Handler struct{}
const keyLatest = "Latest"
func (h *Handler) handleModifyWidget(vals url.Values) error {
	_ = vals.Get(keyLatest)
	return nil
}`,
		},
		{
			name:   "loop table with dynamic key",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description", "Latest"},
			src: `package fixture
type Handler struct{}
func readInto(vals url.Values, key string, dst *string) {}
func apply(vals url.Values) {
	rows := []struct{ param string; dest *string }{
		{param: "Description"}, {param: "Latest"},
	}
	for _, r := range rows {
		readInto(vals, r.param, r.dest)
	}
}
func (h *Handler) handleModifyWidget(vals url.Values) error {
	apply(vals)
	return nil
}`,
		},
		{
			name:   "camelCase rest query read on url.Values",
			file:   "restxml.fixture",
			op:     "PutThing",
			fields: []string{"MaxKeys"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handlePutThing(c *echo.Context) error {
	q := c.Request().URL.Query()
	_ = q.Get("max-keys")
	return nil
}`,
		},
		{
			name:   "header const resolves to wire name",
			file:   "restxml.fixture",
			op:     "PutThing",
			fields: []string{"ServerSideEncryption"},
			src: `package fixture
type Handler struct{}
type Header struct{}
type Request struct{ Header Header }
func (h Header) Get(name string) string { return "" }
const headerSSE = "X-Amz-Server-Side-Encryption"
func (h *Handler) handlePutThing(r *Request) error {
	_ = r.Header.Get(headerSSE)
	return nil
}`,
		},
		{
			name:   "inline query accessor wrapper",
			file:   "restxml.fixture",
			op:     "PutThing",
			fields: []string{"MaxKeys"},
			src: `package fixture
type Handler struct{}
func queryInt(c *echo.Context, key string) int { return len(c.Request().URL.Query().Get(key)) }
func (h *Handler) handlePutThing(c *echo.Context) error {
	_ = queryInt(c, "max-keys")
	return nil
}`,
		},
		{
			name:   "payload wrapper member is never a wire field",
			file:   "restxml.fixture",
			op:     "PutThing",
			fields: []string{"ThingConfiguration"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handlePutThing(c *echo.Context) error { return nil }`,
		},
		{
			name:   "index-assigned dispatch to a differently named handler",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description", "Latest"},
			src: `package fixture
type Handler struct{}
func (h *Handler) resolveDescription(vals url.Values) string { return vals.Get("Description") }
func (h *Handler) handleChangeWidget(vals url.Values) error {
	d, err := h.resolveDescription(vals)
	_, _ = d, err
	return nil
}
func (h *Handler) register(ops map[string]any) { ops["ModifyWidget"] = h.handleChangeWidget }`,
			missing: []string{"Latest"},
		},
		{
			name:   "unread field is still reported",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Description", "Latest", "Groups"},
			src: `package fixture
type Handler struct{}
func (h *Handler) handleModifyWidget(vals url.Values) error {
	_ = vals.Get("Description")
	return nil
}`,
			missing: []string{"Latest", "Groups"},
		},
		{
			name:   "non-values receiver with a matching name is not a read",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Latest"},
			src: `package fixture
type Handler struct{}
type cache struct{}
func (c cache) Get(k string) string { return "" }
func (h *Handler) handleModifyWidget(vals url.Values) error {
	var c cache
	_ = c.Get("Latest")
	_ = vals.Get("Description")
	return nil
}`,
			missing: []string{"Latest"},
		},
		{
			name:   "dynamic-key function does not credit unrelated literals",
			file:   "ec2query.fixture",
			op:     "ModifyWidget",
			fields: []string{"Latest"},
			src: `package fixture
type Handler struct{}
func readInto(vals url.Values, key string, dst *string) {}
func (h *Handler) handleModifyWidget(vals url.Values) error {
	rows := []struct{ param string }{{param: "Description"}}
	for _, r := range rows {
		readInto(vals, r.param, nil)
	}
	_ = "latest in a sentence"
	return nil
}`,
			missing: []string{"Latest"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fields := make([]sdkField, 0, len(tt.fields))
			for _, f := range tt.fields {
				fields = append(fields, mustField(f, "", false))
			}

			ops := []sdkOp{{Name: tt.op, Fields: fields}}
			attachWireKeys(ops, loadSerializerFixture(t, tt.file))

			res := parseSrc(t, tt.src).resolveOps(ops)[tt.op]

			assert.ElementsMatch(t, tt.missing, missingNames(ops[0], res))
		})
	}
}
