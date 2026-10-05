package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveOp_RawBodyWrapperKeys(t *testing.T) {
	t.Parallel()

	const gate = `
func isGated(op string) bool {
	switch op {
	case "CreateThing":
		return true
	default:
		return false
	}
}`

	tests := []struct {
		name        string
		op          sdkOp
		src         string
		wantMissing []string
	}{
		{
			name: "map index in a helper credits gated op",
			op:   shapeOp("CreateThing", "Name", "ClientRequestToken"),
			src: `package fixture
type Handler struct{}
type createThingInput struct{ Name string ` + "`json:\"Name\"`" + ` }
func (h *Handler) handleCreateThing(ctx context.Context, in *createThingInput) (*out, error) { return nil, nil }
func tokenOf(body []byte) string {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil { return "" }
	s, _ := m["ClientRequestToken"].(string)
	return s
}
func (h *Handler) dispatchIdem(op string, body []byte) error {
	if !isGated(op) { return nil }
	_ = tokenOf(body)
	return nil
}` + gate,
		},
		{
			name: "small struct decode of raw body credits gated op",
			op:   shapeOp("CreateThing", "Name", "ClientRequestToken"),
			src: `package fixture
type Handler struct{}
type createThingInput struct{ Name string ` + "`json:\"Name\"`" + ` }
func (h *Handler) handleCreateThing(ctx context.Context, in *createThingInput) (*out, error) { return nil, nil }
type tokenEnvelope struct{ ClientRequestToken string ` + "`json:\"ClientRequestToken\"`" + ` }
func (h *Handler) dispatchIdem(op string, body []byte) error {
	var env tokenEnvelope
	_ = json.Unmarshal(body, &env)
	if !isGated(op) { return nil }
	return nil
}` + gate,
		},
		{
			name: "gjson lookup credits gated op",
			op:   shapeOp("CreateThing", "Name", "ClientRequestToken"),
			src: `package fixture
type Handler struct{}
type createThingInput struct{ Name string ` + "`json:\"Name\"`" + ` }
func (h *Handler) handleCreateThing(ctx context.Context, in *createThingInput) (*out, error) { return nil, nil }
func (h *Handler) dispatchIdem(op string, body []byte) error {
	_ = gjson.GetBytes(body, "ClientRequestToken")
	if !isGated(op) { return nil }
	return nil
}` + gate,
		},
		{
			name: "ungated op is not credited",
			op:   shapeOp("DeleteThing", "ThingID", "ClientRequestToken"),
			src: `package fixture
type Handler struct{}
type deleteThingInput struct{ ThingID string ` + "`json:\"ThingID\"`" + ` }
func (h *Handler) handleDeleteThing(ctx context.Context, in *deleteThingInput) (*out, error) { return nil, nil }
func tokenOf(body []byte) string {
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	s, _ := m["ClientRequestToken"].(string)
	return s
}
func (h *Handler) dispatchIdem(op string, body []byte) error {
	if !isGated(op) { return nil }
	_ = tokenOf(body)
	return nil
}` + gate,
			wantMissing: []string{"ClientRequestToken"},
		},
		{
			name: "key outside the sdk fields is not invented",
			op:   shapeOp("CreateThing", "Name", "Ghost"),
			src: `package fixture
type Handler struct{}
type createThingInput struct{ Name string ` + "`json:\"Name\"`" + ` }
func (h *Handler) handleCreateThing(ctx context.Context, in *createThingInput) (*out, error) { return nil, nil }
func tokenOf(body []byte) string {
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	s, _ := m["ClientRequestToken"].(string)
	return s
}
func (h *Handler) dispatchIdem(op string, body []byte) error {
	if !isGated(op) { return nil }
	_ = tokenOf(body)
	return nil
}` + gate,
			wantMissing: []string{"Ghost"},
		},
		{
			name: "wrapper without a gate credits nothing",
			op:   shapeOp("CreateThing", "Name", "ClientRequestToken"),
			src: `package fixture
type Handler struct{}
type createThingInput struct{ Name string ` + "`json:\"Name\"`" + ` }
func (h *Handler) handleCreateThing(ctx context.Context, in *createThingInput) (*out, error) { return nil, nil }
func (h *Handler) dispatchIdem(op string, body []byte) error {
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	_ = m["ClientRequestToken"]
	return nil
}`,
			wantMissing: []string{"ClientRequestToken"},
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

func TestResolveOp_EmbeddedDecodeStructs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		op          sdkOp
		src         string
		wantMissing []string
	}{
		{
			name: "anonymous decode struct embeds a named struct",
			op:   shapeOp("MergeThing", "ID", "Level", "Keep"),
			src: `package fixture
type Handler struct{}
type settings struct {
	Level string ` + "`json:\"level\"`" + `
	Keep  bool ` + "`json:\"keep\"`" + `
}
func (h *Handler) handleMergeThing(body []byte) error {
	var req struct {
		ID string ` + "`json:\"id\"`" + `
		settings
	}
	return json.Unmarshal(body, &req)
}`,
		},
		{
			name: "pointer embed nested two deep",
			op:   shapeOp("MergeThing", "ID", "Level", "Page"),
			src: `package fixture
type Handler struct{}
type settings struct{ Level string ` + "`json:\"level\"`" + ` }
type query struct {
	Page string ` + "`json:\"page\"`" + `
	*settings
}
type mergeInput struct {
	ID string ` + "`json:\"id\"`" + `
	query
}
func (h *Handler) handleMergeThing(body []byte) error {
	var req mergeInput
	return json.Unmarshal(body, &req)
}`,
		},
		{
			name: "xml tagged embedded fields",
			op:   shapeOp("MergeThing", "ID", "Level"),
			src: `package fixture
type Handler struct{}
type settings struct{ Level string ` + "`xml:\"Level\"`" + ` }
func (h *Handler) handleMergeThing(body []byte) error {
	var req struct {
		ID string ` + "`xml:\"ID\"`" + `
		settings
	}
	return xml.Unmarshal(body, &req)
}`,
		},
		{
			name: "embedded struct does not credit unrelated fields",
			op:   shapeOp("MergeThing", "ID", "Level", "Ghost"),
			src: `package fixture
type Handler struct{}
type settings struct{ Level string ` + "`json:\"level\"`" + ` }
func (h *Handler) handleMergeThing(body []byte) error {
	var req struct {
		ID string ` + "`json:\"id\"`" + `
		settings
	}
	return json.Unmarshal(body, &req)
}`,
			wantMissing: []string{"Ghost"},
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

func TestResolveOp_FamilyHandler(t *testing.T) {
	t.Parallel()

	const handler = `package fixture
type Handler struct{}
type Backend struct{}
func (b *Backend) MergeThingBySquash(id, src string) error { return nil }
func (h *Handler) handleMergeThing(option string, body []byte) error {
	var req struct{ ID string ` + "`json:\"id\"`" + `; Level string ` + "`json:\"level\"`" + ` }
	return json.Unmarshal(body, &req)
}`

	tests := []struct {
		name        string
		op          sdkOp
		extraOps    []sdkOp
		wantMissing []string
	}{
		{
			name:        "shared prefix handler resolves the op",
			op:          shapeOp("MergeThingBySquash", "ID", "Level", "Ghost"),
			wantMissing: []string{"Ghost"},
		},
		{
			name:        "prefix that is itself an sdk op is not a family",
			op:          shapeOp("MergeThingBySquash", "ID", "Level"),
			extraOps:    []sdkOp{shapeOp("MergeThing", "ID")},
			wantMissing: []string{"Level"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ops := append([]sdkOp{tt.op}, tt.extraOps...)
			res := parseSrc(t, handler).resolveOps(ops)[tt.op.Name]

			assert.ElementsMatch(t, tt.wantMissing, missingNames(tt.op, res))
		})
	}
}

func TestSplitRecorded(t *testing.T) {
	t.Parallel()

	finding := func(op, field string) triageFinding {
		return triageFinding{Op: op, Field: sdkField{Name: field}}
	}

	tests := []struct {
		name         string
		lines        []string
		wantRecorded int
		wantFindings int
	}{
		{
			name:         "same line records",
			lines:        []string{"- `CreateThing` ignores ClientRequestToken"},
			wantRecorded: 1,
			wantFindings: 1,
		},
		{
			name:         "split across lines does not record",
			lines:        []string{"CreateThing is fine", "ClientRequestToken elsewhere"},
			wantFindings: 2,
		},
		{
			name:         "substring is not a word match",
			lines:        []string{"CreateThingTwo ignores ClientRequestToken"},
			wantFindings: 2,
		},
		{name: "no parity file", wantFindings: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := serviceReport{Findings: []triageFinding{
				finding("CreateThing", "ClientRequestToken"),
				finding("DeleteThing", "ThingID"),
			}}

			splitRecorded(&r, tt.lines)

			assert.Len(t, r.Recorded, tt.wantRecorded)
			assert.Len(t, r.Findings, tt.wantFindings)
		})
	}
}
