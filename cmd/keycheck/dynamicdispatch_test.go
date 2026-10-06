package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sdkDispatchShapesFixture = `package fakesdk

type GetScheduleOutput struct{}
type ListSchedulesOutput struct{}

func awsAwsjson11_deserializeOpDocumentGetScheduleOutput(v **GetScheduleOutput, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "Name":
			_ = val
		}
	}
	*v = sv
	return nil
}

func awsAwsjson11_deserializeOpDocumentListSchedulesOutput(v **ListSchedulesOutput, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "Names":
			_ = val
		}
	}
	*v = sv
	return nil
}
`

func runFixture(t *testing.T, svcSrc string) *checkResult {
	t.Helper()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkDispatchShapesFixture)

	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcSrc)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsAwsjson11_", svcDir, "")
	require.NoError(t, err)

	return res
}

func TestRunCheck_DispatchShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		src           string
		wantChecked   []string
		wantUnbound   []string
		wantUnresolve bool
	}{
		{
			name: "if-chain on an action parameter binds each op",
			src: `package svc
type Handler struct{}
func (h *Handler) dispatch(action string) map[string]any {
	if action == "GetSchedule" {
		return h.getSchedule()
	}
	if action == "ListSchedules" {
		return h.listSchedules()
	}
	return nil
}
func (h *Handler) getSchedule() map[string]any { return map[string]any{"Name": "x"} }
func (h *Handler) listSchedules() map[string]any { return map[string]any{"Names": 1} }
`,
			wantChecked: []string{"GetSchedule", "ListSchedules"},
		},
		{
			name: "operator comparison on a non-op parameter is not dispatch",
			src: `package svc
type Handler struct{}
func (h *Handler) isMul(op string) bool {
	if op == "GetSchedule" {
		return h.getSchedule() != nil
	}
	return false
}
func (h *Handler) getSchedule() map[string]any { return map[string]any{"Name": "x"} }
`,
		},
		{
			name: "switch on operation returning a package function call binds",
			src: `package svc
type Handler struct{}
func (h *Handler) dispatch(operation string) map[string]any {
	switch operation {
	case "GetSchedule":
		return dispatchGet(h)
	}
	return nil
}
func dispatchGet(h *Handler) map[string]any { return map[string]any{"Name": "x"} }
`,
			wantChecked: []string{"GetSchedule"},
		},
		{
			name: "incremental map with computed keys is reported unresolvable",
			src: `package svc
type opFunc func() map[string]any
type Handler struct{}
func (h *Handler) build() map[string]opFunc {
	ops := map[string]opFunc{"GetSchedule": h.getSchedule}
	for _, p := range []string{"X"} {
		ops["List"+p] = h.listSchedules
	}
	return ops
}
func (h *Handler) getSchedule() map[string]any { return map[string]any{"Name": "x"} }
func (h *Handler) listSchedules() map[string]any { return nil }
`,
			wantChecked:   []string{"GetSchedule"},
			wantUnbound:   []string{"ListSchedules"},
			wantUnresolve: true,
		},
		{
			name: "incremental map with constant keys binds",
			src: `package svc
type opFunc func() map[string]any
type Handler struct{}
func (h *Handler) build() map[string]opFunc {
	ops := map[string]opFunc{"GetSchedule": h.getSchedule}
	ops["ListSchedules"] = h.listSchedules
	return ops
}
func (h *Handler) getSchedule() map[string]any { return map[string]any{"Name": "x"} }
func (h *Handler) listSchedules() map[string]any { return map[string]any{"Names": 1} }
`,
			wantChecked: []string{"GetSchedule", "ListSchedules"},
		},
		{
			name: "data-driven spec table looked up by action is reported unresolvable",
			src: `package svc
type spec struct{ name string }
type Handler struct{ ops map[string]spec }
func (h *Handler) dispatch(action string) map[string]any {
	if action == "GetSchedule" {
		return h.getSchedule()
	}
	s, ok := h.ops[action]
	if !ok {
		return nil
	}
	return map[string]any{"n": s.name}
}
func (h *Handler) getSchedule() map[string]any { return map[string]any{"Name": "x"} }
`,
			wantChecked:   []string{"GetSchedule"},
			wantUnbound:   []string{"ListSchedules"},
			wantUnresolve: true,
		},
		{
			name: "func-valued lookup that is called stays resolved, not unresolvable",
			src: `package svc
type Handler struct{ ops map[string]func() map[string]any }
func (h *Handler) dispatch(action string) map[string]any {
	fn, ok := h.ops[action]
	if !ok {
		return nil
	}
	return fn()
}
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := runFixture(t, tc.src)

			var checked []string
			for _, o := range res.OpsChecked {
				checked = append(checked, o.Op)
			}

			assert.ElementsMatch(t, tc.wantChecked, checked)
			assert.ElementsMatch(t, tc.wantUnbound, res.UnboundOps)
			assert.Equal(t, tc.wantUnresolve, len(res.UnresolvableSites) > 0)
		})
	}
}

func TestPrintVerdict_UnboundOpsAreNotClean(t *testing.T) {
	t.Parallel()

	assert.Equal(t, exitPartial, printVerdict(5, 0, 3))
	assert.Equal(t, exitClean, printVerdict(5, 0, 0))
}
