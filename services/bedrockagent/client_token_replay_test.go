package bedrockagent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func TestClientToken_ReplaySurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(*bedrockagent.InMemoryBackend, string) (string, error)
		name   string
	}{
		{name: "knowledge base", create: func(b *bedrockagent.InMemoryBackend, tok string) (string, error) {
			kb, err := b.CreateKnowledgeBase(t.Context(), bedrockagent.KnowledgeBaseConfig{
				Name: "kb", RoleARN: "arn:aws:iam::000000000000:role/r", ClientToken: tok,
			})
			if err != nil {
				return "", err
			}

			return kb.KnowledgeBaseID, nil
		}},
		{name: "flow", create: func(b *bedrockagent.InMemoryBackend, tok string) (string, error) {
			f, err := b.CreateFlow(t.Context(), bedrockagent.FlowConfig{
				Name: "flow", RoleARN: "arn:aws:iam::000000000000:role/r", ClientToken: tok,
			})
			if err != nil {
				return "", err
			}

			return f.FlowID, nil
		}},
		{name: "prompt", create: func(b *bedrockagent.InMemoryBackend, tok string) (string, error) {
			p, err := b.CreatePrompt(t.Context(), bedrockagent.PromptConfig{Name: "prompt", ClientToken: tok})
			if err != nil {
				return "", err
			}

			return p.PromptID, nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := bedrockagent.NewInMemoryBackend("us-east-1", "000000000000")

			first, err := tt.create(src, "token-1")
			require.NoError(t, err)

			dst := bedrockagent.NewInMemoryBackend("us-east-1", "000000000000")
			require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

			again, err := tt.create(dst, "token-1")
			require.NoError(t, err)
			assert.Equal(t, first, again, "replayed token after restore must return the original resource")
		})
	}
}

func TestKBDocuments_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	ident := func(id string) bedrockagent.KBDocumentIdentifier {
		return bedrockagent.KBDocumentIdentifier{
			DataSourceType: "CUSTOM",
			Custom:         &bedrockagent.KBCustomDocumentIdentifier{ID: id},
		}
	}

	tests := []struct {
		name  string
		token string
		want  int
	}{
		{name: "same token replays", token: "tok", want: 1},
		{name: "no token reapplies", token: "", want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := bedrockagent.NewInMemoryBackend("us-east-1", "000000000000")

			kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
				Name: "kb", RoleARN: "arn:aws:iam::000000000000:role/r",
			})
			require.NoError(t, err)

			ds, err := b.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{Name: "ds"})
			require.NoError(t, err)

			_, err = b.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, tt.token,
				[]bedrockagent.KBDocument{{Identifier: ident("a")}})
			require.NoError(t, err)

			_, err = b.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, tt.token,
				[]bedrockagent.KBDocument{{Identifier: ident("b")}})
			require.NoError(t, err)

			docs, _, err := b.ListKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, 0, "")
			require.NoError(t, err)
			assert.Len(t, docs, tt.want)

			first, err := b.DeleteKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, tt.token,
				[]bedrockagent.KBDocumentIdentifier{ident("a")})
			require.NoError(t, err)
			require.Len(t, first, 1)

			replay, err := b.DeleteKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, tt.token,
				[]bedrockagent.KBDocumentIdentifier{ident("a")})
			require.NoError(t, err)
			require.Len(t, replay, 1)

			if tt.token != "" {
				assert.Equal(t, first[0].Status, replay[0].Status, "replay returns the original result")
			} else {
				assert.NotEqual(t, first[0].Status, replay[0].Status, "untokened retry sees the document gone")
			}
		})
	}
}
