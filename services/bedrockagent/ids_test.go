package bedrockagent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func TestResourceIDsAreTenAlphanumericCharacters(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/BedrockRole"

	tests := []struct {
		create func(b *bedrockagent.InMemoryBackend) (string, error)
		name   string
	}{
		{name: "agent", create: func(b *bedrockagent.InMemoryBackend) (string, error) {
			a, err := b.CreateAgent(t.Context(), bedrockagent.AgentConfig{
				AgentName: "a", FoundationModel: "anthropic.claude-v2", RoleARN: role,
			})
			if err != nil {
				return "", err
			}

			return a.AgentID, nil
		}},
		{name: "kb", create: func(b *bedrockagent.InMemoryBackend) (string, error) {
			k, err := b.CreateKnowledgeBase(t.Context(), bedrockagent.KnowledgeBaseConfig{Name: "k", RoleARN: role})
			if err != nil {
				return "", err
			}

			return k.KnowledgeBaseID, nil
		}},
		{name: "flow", create: func(b *bedrockagent.InMemoryBackend) (string, error) {
			f, err := b.CreateFlow(t.Context(), bedrockagent.FlowConfig{Name: "f", RoleARN: role})
			if err != nil {
				return "", err
			}

			return f.FlowID, nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockagent.NewInMemoryBackend("000000000000", "us-east-1")
			id, err := tt.create(b)
			require.NoError(t, err)
			assert.Regexp(t, `^[0-9a-zA-Z]{10}$`, id)
		})
	}
}
