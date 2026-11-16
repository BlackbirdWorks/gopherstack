package connect_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

func TestAssociateBotLogsWouldGrantAccess(t *testing.T) {
	t.Parallel()

	const alias = "arn:aws:lex:us-east-1:123456789012:bot-alias/ABCDEFGHIJ/KLMNOPQRST"

	tests := []struct {
		name string
		cfg  cn.LexBotConfig
		want string
	}{
		{
			name: "v1",
			cfg:  cn.LexBotConfig{LexBot: &cn.LexBot{Name: "Orders", LexRegion: "us-east-1"}},
			want: "us-east-1/Orders",
		},
		{name: "v2", cfg: cn.LexBotConfig{LexV2Bot: &cn.LexV2Bot{AliasArn: alias}}, want: alias},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id := f.newInstance(t, "lexlog")

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			require.NoError(t, f.backend.AssociateBot(ctx, id, tt.cfg))

			assert.Contains(t, buf.String(), "would grant the instance access to the Lex bot")
			assert.Contains(t, buf.String(), tt.want)

			got, _, err := f.backend.ListBots(id, map[bool]string{true: "V1", false: "V2"}[tt.cfg.LexBot != nil], "", 0)
			require.NoError(t, err)
			assert.Len(t, got, 1)
		})
	}
}
