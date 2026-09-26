package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// fieldResolveReg is a small, real-shaped fixture: Cluster.Status resolves
// to the single real ClusterStatus enum, Cluster.Arn is a plain *string,
// and Job flattens Summary's Status one hop (amplify's real shape).
func fieldResolveReg() *enumRegistry {
	return &enumRegistry{
		membersByType: map[string]map[string]bool{
			"ClusterStatus": {"ACTIVE": true, "CREATING": true},
		},
		constByIdent: map[string]enumConst{},
		sdkFieldTypes: map[string]map[string]string{
			"Cluster":    {"Status": "ClusterStatus", "Arn": "string"},
			"Job":        {"Steps": "Step", "Summary": "JobSummary"},
			"JobSummary": {"Status": "ClusterStatus"},
			"Step":       {"StepName": "string"},
		},
	}
}

func TestResolveRealField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		structName string
		wireKey    string
		wantEnum   string
		wantRes    fieldResolution
	}{
		{
			name:       "direct enum field resolves to the one real enum",
			structName: "Cluster",
			wireKey:    "Status",
			wantRes:    fieldIsEnum,
			wantEnum:   "ClusterStatus",
		},
		{
			name:       "plain string field is not an enum",
			structName: "Cluster",
			wireKey:    "Arn",
			wantRes:    fieldNotEnum,
		},
		{
			name:       "field absent even one hop through a nested type is a phantom shape",
			structName: "Cluster",
			wireKey:    "NoSuchField",
			wantRes:    fieldAbsent,
		},
		{
			name:       "field resolved one hop through a directly nested struct field",
			structName: "Job",
			wireKey:    "Status",
			wantRes:    fieldIsEnum,
			wantEnum:   "ClusterStatus",
		},
		{
			name:       "unknown struct type has no ground truth at all",
			structName: "Nonexistent",
			wireKey:    "Status",
			wantRes:    fieldUnknownType,
		},
	}

	reg := fieldResolveReg()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, enum := reg.resolveRealField(tc.structName, tc.wireKey)
			assert.Equal(t, tc.wantRes, res)
			assert.Equal(t, tc.wantEnum, enum)
		})
	}
}
