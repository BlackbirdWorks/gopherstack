package pods_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/pods"
)

func TestMergeScoped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		current    string
		incoming   string
		want       string
		wantScoped bool
		wantErr    bool
	}{
		{
			name:       "array replaces same scope keeps other",
			current:    `{"q":[{"accountID":"1","region":"a","n":"old"},{"accountID":"1","region":"b","n":"keep"}]}`,
			incoming:   `{"q":[{"accountID":"1","region":"a","n":"new"}]}`,
			want:       `{"q":[{"accountID":"1","n":"keep","region":"b"},{"accountID":"1","n":"new","region":"a"}]}`,
			wantScoped: true,
		},
		{
			name:       "map keyed by id",
			current:    `{"t":{"x":{"accountId":"1","region":"a"},"y":{"accountId":"2","region":"a"}}}`,
			incoming:   `{"t":{"z":{"accountId":"1","region":"a"}}}`,
			want:       `{"t":{"y":{"accountId":"2","region":"a"},"z":{"accountId":"1","region":"a"}}}`,
			wantScoped: true,
		},
		{
			name:       "unscoped replaces",
			current:    `{"a":1}`,
			incoming:   `{"b":2}`,
			want:       `{"b":2}`,
			wantScoped: false,
		},
		{name: "corrupt incoming", current: `{}`, incoming: `{bad`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, scoped, err := pods.MergeScoped([]byte(tt.current), []byte(tt.incoming))
			if tt.wantErr {
				require.ErrorIs(t, err, pods.ErrCorrupt)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantScoped, scoped)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
