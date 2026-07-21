package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSchemas_Paths(t *testing.T) {
	t.Parallel()

	type args struct {
		method    string
		collabID  string
		schemaID  string
		ruleType  string
		schemaIDs []string
	}
	type wants struct {
		err bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "NotFound on GetSchema",
			args: args{
				method:   "GetSchema",
				collabID: "invalid",
				schemaID: "invalid",
			},
			wants: wants{err: true},
		},
		{
			name: "NotFound on ListSchemas",
			args: args{
				method:   "ListSchemas",
				collabID: "invalid",
			},
			wants: wants{err: true},
		},
		{
			name: "NotFound on BatchGetSchema",
			args: args{
				method:    "BatchGetSchema",
				collabID:  "invalid",
				schemaIDs: []string{"a"},
			},
			wants: wants{err: true},
		},
		{
			name: "BatchGetSchema with some missing",
			args: args{
				method:    "BatchGetSchema",
				collabID:  "seed",
				schemaIDs: []string{"invalid"},
			},
			wants: wants{err: false},
		},
		{
			name: "NotFound on GetSchemaAnalysisRule",
			args: args{
				method:   "GetSchemaAnalysisRule",
				collabID: "invalid",
				schemaID: "invalid",
				ruleType: "invalid",
			},
			wants: wants{err: true},
		},
		{
			name: "NotFound on BatchGetSchemaAnalysisRule",
			args: args{
				method:    "BatchGetSchemaAnalysisRule",
				collabID:  "invalid",
				schemaIDs: []string{"a"},
				ruleType:  "b",
			},
			wants: wants{err: true},
		},
		{
			name: "BatchGetSchemaAnalysisRule with some missing",
			args: args{
				method:    "BatchGetSchemaAnalysisRule",
				collabID:  "seed",
				schemaIDs: []string{"invalid"},
				ruleType:  "AGGREGATION",
			},
			wants: wants{err: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			seed := seedFullState(t, b)

			collabID := tt.args.collabID
			if collabID == "seed" {
				collabID = seed.collab.CollaborationIdentifier
			}

			switch tt.args.method {
			case "GetSchema":
				_, err := b.GetSchema(collabID, tt.args.schemaID)
				if tt.wants.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case "ListSchemas":
				_, _, err := b.ListSchemas(collabID, "", "", "")
				if tt.wants.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case "BatchGetSchema":
				schemas, errors, err := b.BatchGetSchema(collabID, tt.args.schemaIDs)
				if tt.wants.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Empty(t, schemas)
					assert.Len(t, errors, 1)
				}
			case "GetSchemaAnalysisRule":
				_, err := b.GetSchemaAnalysisRule(collabID, tt.args.schemaID, tt.args.ruleType)
				if tt.wants.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case "BatchGetSchemaAnalysisRule":
				rules, errors2, err := b.BatchGetSchemaAnalysisRule(collabID, tt.args.schemaIDs, tt.args.ruleType)
				if tt.wants.err {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Empty(t, rules)
					assert.Len(t, errors2, 1)
				}
			}
		})
	}
}
