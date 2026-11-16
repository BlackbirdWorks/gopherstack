package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accountbackend "github.com/blackbirdworks/gopherstack/services/account"
	oambackend "github.com/blackbirdworks/gopherstack/services/oam"
)

func TestOAMLabelUsesStandaloneAccount(t *testing.T) {
	t.Parallel()

	const acct = "000000000000"

	tests := []struct {
		name     string
		template string
		want     string
		wire     bool
	}{
		{name: "account name", template: "$AccountName", want: "Test Account", wire: true},
		{name: "without directory", template: "$AccountName", want: acct, wire: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := oambackend.NewInMemoryBackend(acct, "us-east-1")
			if tt.wire {
				dir := oamAccountDirectory{account: accountbackend.NewInMemoryBackend(acct, "us-east-1")}
				bk.SetAccountDirectory(dir)
			}

			sink, err := bk.CreateSink(t.Context(), "s", nil)
			require.NoError(t, err)

			_, err = bk.PutSinkPolicy(t.Context(), sink.ARN,
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",`+
					`"Action":["oam:CreateLink","oam:UpdateLink"],"Resource":"*"}]}`)
			require.NoError(t, err)

			link, err := bk.CreateLink(t.Context(), oambackend.LinkInput{
				SinkARN: sink.ARN, LabelTemplate: tt.template, ResourceTypes: []string{"AWS::CloudWatch::Metric"},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, link.Label)
		})
	}
}
