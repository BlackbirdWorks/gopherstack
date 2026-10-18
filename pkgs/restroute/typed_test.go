package restroute_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type typedIn struct {
	Max   *int32 `json:"-" query:"max"`
	Alias *string
	ID    string `json:"-"              path:"Id"`
	Name  string
	Keys  []string `json:"-" query:"keys"`
	Flag  bool     `json:"-" query:"flag"`
}

type typedOut struct{ Echo string }

func TestTyped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query   url.Values
		name    string
		body    string
		want    string
		wantErr string
	}{
		{name: "path and body", body: `{"Name":"n"}`, want: "i/n/0/false/0"},
		{name: "query", query: url.Values{"max": {"5"}, "flag": {"true"}, "keys": {"a", "b"}}, want: "i//5/true/2"},
		{name: "bad int", query: url.Values{"max": {"x"}}, wantErr: "ValidationException"},
		{name: "bad body", body: `{`, wantErr: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op := restroute.Typed(func(_ context.Context, in *typedIn) (*typedOut, error) {
				limit := int32(0)
				if in.Max != nil {
					limit = *in.Max
				}

				return &typedOut{Echo: fmt.Sprintf("%s/%s/%d/%v/%d", in.ID, in.Name, limit, in.Flag, len(in.Keys))}, nil
			})

			out, _, err := op(t.Context(), &restroute.Request{
				Params: map[string]string{"Id": "i"}, Query: tt.query, Body: []byte(tt.body),
			})
			if tt.wantErr != "" {
				var re *restroute.Error

				require.ErrorAs(t, err, &re)
				assert.Equal(t, tt.wantErr, re.Code)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, out.(*typedOut).Echo)
		})
	}
}

type typedEmbed struct {
	typedPaging
	ID string `json:"-" path:"Id"`
}

type typedPaging struct {
	Token string `json:"-" query:"nextToken"`
}

func TestTypedEmbeddedBindings(t *testing.T) {
	t.Parallel()

	op := restroute.Typed(func(_ context.Context, in *typedEmbed) (*typedOut, error) {
		return &typedOut{Echo: in.ID + "/" + in.Token}, nil
	})

	out, _, err := op(t.Context(), &restroute.Request{
		Params: map[string]string{"Id": "i"}, Query: url.Values{"nextToken": {"tok"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "i/tok", out.(*typedOut).Echo)
}
