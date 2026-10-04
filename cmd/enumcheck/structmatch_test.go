package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cmd/internal/sdkshape"
)

func overlapReg() *enumRegistry {
	return &enumRegistry{
		membersByType: map[string]map[string]bool{
			"JobStatus":  {"RUNNING": true, "DONE": true},
			"TaskStatus": {"QUEUED": true},
		},
		constByIdent: map[string]enumConst{},
		sdkFieldTypes: map[string]map[string]string{
			"DescribeJobOutput": {"JobName": "string", "JobArn": "string", "Status": "JobStatus", "Tags": "Tag"},
			"DescribeTaskOutput": {
				"TaskName": "string", "TaskArn": "string", "Status": "TaskStatus", "Tags": "Tag",
			},
		},
	}
}

func TestResolveByOverlap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wireKey  string
		wantEnum string
		local    []string
		wantRes  fieldResolution
	}{
		{
			name: "local struct overlapping one SDK struct resolves its enum", wireKey: "Status",
			local:   []string{"JobName", "JobArn", "Status", "Tags"},
			wantRes: fieldIsEnum, wantEnum: "JobStatus",
		},
		{
			name: "the sibling SDK struct sharing the key is not chosen", wireKey: "Status",
			local:   []string{"TaskName", "TaskArn", "Status"},
			wantRes: fieldIsEnum, wantEnum: "TaskStatus",
		},
		{
			name: "too little overlap stays unresolved", wireKey: "Status",
			local:   []string{"Status", "Tags"},
			wantRes: fieldUnknownType,
		},
		{
			name: "mostly foreign local fields stay unresolved", wireKey: "Status",
			local:   []string{"JobName", "JobArn", "Status", "A", "B", "C", "D", "E"},
			wantRes: fieldUnknownType,
		},
		{
			name: "lowercase wire names match the exported SDK names", wireKey: "status",
			local:   []string{"jobName", "jobArn", "status"},
			wantRes: fieldIsEnum, wantEnum: "JobStatus",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, enum := overlapReg().resolveByOverlap(tc.wireKey, tc.local)
			assert.Equal(t, tc.wantRes, res)
			assert.Equal(t, tc.wantEnum, enum)
		})
	}
}

func TestResolveByOverlap_TiedDifferentEnumsUnresolved(t *testing.T) {
	t.Parallel()

	reg := overlapReg()
	reg.sdkFieldTypes["DescribeTaskOutput"] = map[string]string{
		"JobName": "string", "JobArn": "string", "Status": "TaskStatus", "Tags": "Tag",
	}

	res, _ := reg.resolveByOverlap("Status", []string{"JobName", "JobArn", "Status", "Tags"})
	assert.Equal(t, fieldUnknownType, res)
}

func TestMergeStructDef(t *testing.T) {
	t.Parallel()

	def := func(name, typ string) sdkshape.StructDef {
		return sdkshape.StructDef{Fields: []sdkshape.Field{{Name: name, Type: typ}}}
	}

	tests := []struct {
		name      string
		wantField string
		order     []bool
		types     []string
	}{
		{
			name: "native wins over a foreign same-named struct", wantField: "string",
			order: []bool{false, true}, types: []string{"ServerlessStatus", "string"},
		},
		{
			name: "foreign cannot overwrite a native struct", wantField: "string",
			order: []bool{true, false}, types: []string{"string", "ServerlessStatus"},
		},
		{
			name: "two foreign modules disagreeing is ambiguous", wantField: ambiguousFieldType,
			order: []bool{false, false}, types: []string{"A", "B"},
		},
		{
			name: "two foreign modules agreeing keeps the type", wantField: "A",
			order: []bool{false, false}, types: []string{"A", "A"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reg := &enumRegistry{sdkFieldTypes: map[string]map[string]string{}, sdkNativeStructs: map[string]bool{}}
			for i, native := range tc.order {
				mergeStructDef(reg, "Snapshot", def("Status", tc.types[i]), native)
			}

			assert.Equal(t, tc.wantField, reg.sdkFieldTypes["Snapshot"]["Status"])
		})
	}
}

func TestScanStructByOverlap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		value         string
		wantFindings  int
		wantConfident bool
	}{
		{name: "bad value on a structurally matched struct is needs-review", value: "BOGUS", wantFindings: 1},
		{name: "member value on a structurally matched struct is clean", value: "RUNNING"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := `package svc
type Job struct {
	JobName string ` + "`json:\"JobName\"`" + `
	JobArn string ` + "`json:\"JobArn\"`" + `
	Status string ` + "`json:\"Status\"`" + `
}
func build() *Job { return &Job{Status: "` + tc.value + `"} }`
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

			wk := map[string]wireKeyFact{"Status": {Enums: []string{"JobStatus", "TaskStatus"}}}
			findings, err := scanPackage(dir, overlapReg(), wk, dir)
			require.NoError(t, err)
			require.Len(t, findings, tc.wantFindings)

			if tc.wantFindings > 0 {
				assert.False(t, findings[0].Confident)
				assert.Equal(t, kindLiteral, findings[0].Kind)
				assert.Equal(t, "JobStatus", findings[0].Enum)
			}
		})
	}
}
