package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSchemasBackend(t *testing.T) {
	t.Parallel()

	type args struct {
		name string
	}
	type wants struct {
		count int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "Restore schemas",
			args: args{
				name: "test-schema",
			},
			wants: wants{
				count: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

			collab, err := b.CreateCollaboration("seed-collab", "", "creator", nil, nil, "", nil)
			require.NoError(t, err)

			_, _, err = b.BatchGetSchema(collab.CollaborationIdentifier, []string{tt.args.name})
			require.NoError(t, err)

			_, _, err = b.BatchGetSchemaAnalysisRule(
				collab.CollaborationIdentifier,
				[]string{tt.args.name},
				"AGGREGATION",
			)
			require.NoError(t, err)

			// Inject a fake schema using Restore
			snapJSON := `{"version":1,"tables":{` +
				`"collaborations":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier + `"}],` +
				`"schemas":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier +
				`","Name":"` + tt.args.name + `","Type":"TABLE","AnalysisMethod":"DIRECT_QUERY"}],` +
				`"schemaAnalysisRules":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier +
				`","Name":"` + tt.args.name + `","Type":"AGGREGATION"}]}}`
			err = b.Restore(t.Context(), []byte(snapJSON))
			require.NoError(t, err)

			// ListSchemas
			summaries, _, err := b.ListSchemas(collab.CollaborationIdentifier, "", "", "")
			require.NoError(t, err)
			require.Len(t, summaries, tt.wants.count)

			// BatchGetSchema again
			schemas, _, err := b.BatchGetSchema(collab.CollaborationIdentifier, []string{tt.args.name})
			require.NoError(t, err)
			require.Len(t, schemas, tt.wants.count)

			rules, _, err := b.BatchGetSchemaAnalysisRule(
				collab.CollaborationIdentifier,
				[]string{tt.args.name},
				"AGGREGATION",
			)
			require.NoError(t, err)
			require.Len(t, rules, tt.wants.count)
		})
	}
}

func TestStoreBackend(t *testing.T) {
	t.Parallel()

	type args struct{}
	type wants struct{}

	tests := []struct {
		args  args
		wants wants
		name  string
	}{
		{
			name:  "Reset",
			args:  args{},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackendWithContext(t.Context(), config.DefaultAccountID, config.DefaultRegion)
			require.NotNil(t, b)
			b.Reset()
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx *service.AppContext
	}
	type wants struct {
		err bool
	}

	tests := []struct {
		args  args
		name  string
		wants wants
	}{
		{
			name: "Valid context",
			args: args{
				ctx: &service.AppContext{},
			},
			wants: wants{
				err: false,
			},
		},
		{
			name: "Nil context",
			args: args{
				ctx: nil,
			},
			wants: wants{
				err: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := cleanrooms.Provider{}
			require.Equal(t, "CleanRooms", p.Name())

			ctx := tt.args.ctx
			if ctx != nil && ctx.JanitorCtx == nil {
				ctx.JanitorCtx = t.Context()
			}

			b, err := p.Init(ctx)

			if tt.wants.err {
				require.ErrorIs(t, err, cleanrooms.ErrNilAppContext)
			} else {
				require.NoError(t, err)
				require.NotNil(t, b)
			}
		})
	}
}
