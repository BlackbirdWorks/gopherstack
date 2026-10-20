package gcprest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

func TestTemplateMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tmpl     string
		path     string
		wantVerb string
		want     [][2]string
		noMatch  bool
	}{
		{
			name: "single var", tmpl: "/v1/{name=projects/*/secrets/*}", path: "/v1/projects/p/secrets/s",
			want: [][2]string{{"name", "projects/p/secrets/s"}},
		},
		{name: "bare var is star", tmpl: "/v1/{name}", path: "/v1/abc", want: [][2]string{{"name", "abc"}}},
		{
			name: "two vars", tmpl: "/v1/{a=projects/*}/x/{b=*}", path: "/v1/projects/p/x/y",
			want: [][2]string{{"a", "projects/p"}, {"b", "y"}},
		},
		{
			name: "double star", tmpl: "/v1/{name=operations/**}", path: "/v1/operations/a/b/c",
			want: [][2]string{{"name", "operations/a/b/c"}},
		},
		{
			name: "double star then literal", tmpl: "/v1/{r=**}/tail", path: "/v1/a/b/tail",
			want: [][2]string{{"r", "a/b"}},
		},
		{
			name: "verb parsed", tmpl: "/v1/{name=projects/*}:access", path: "/v1/projects/p:access",
			wantVerb: "access", want: [][2]string{{"name", "projects/p"}},
		},
		{name: "too short", tmpl: "/v1/{name=projects/*/secrets/*}", path: "/v1/projects/p/secrets", noMatch: true},
		{name: "too long", tmpl: "/v1/{name=projects/*}", path: "/v1/projects/p/extra", noMatch: true},
		{name: "wrong literal", tmpl: "/v1/{name=projects/*}", path: "/v1/folders/p", noMatch: true},
		{name: "empty segment", tmpl: "/v1/{name=projects/*}", path: "/v1/projects/", noMatch: true},
		{
			name:     "missing verb",
			tmpl:     "/v1/{name=projects/*}:access",
			path:     "/v1/projects/p",
			noMatch:  true,
			wantVerb: "access",
		},
		{name: "pure literal", tmpl: "/v1/ping", path: "/v1/ping", want: [][2]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verb, vars, ok, err := gcprest.MatchPath(tt.tmpl, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVerb, verb)
			assert.Equal(t, !tt.noMatch, ok)

			if !tt.noMatch {
				assert.Equal(t, tt.want, vars)
			}
		})
	}
}

func TestParseTemplateErrors(t *testing.T) {
	t.Parallel()

	for name, tmpl := range map[string]string{"no slash": "v1/x", "unterminated": "/v1/{name=a/*"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := gcprest.MatchPath(tmpl, "/v1/x")
			require.ErrorIs(t, err, gcprest.ErrBadTemplate)
		})
	}
}

func TestSplitPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		verb    string
		want    []string
		wantErr bool
	}{
		{name: "plain", path: "/v1/a/b", want: []string{"v1", "a", "b"}},
		{name: "verb stripped", path: "/v1/a:go", verb: "go", want: []string{"v1", "a"}},
		{name: "verb absent", path: "/v1/a", verb: "go", wantErr: true},
		{name: "escaped slash", path: "/v1/a%2Fb", want: []string{"v1", "a/b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := gcprest.SplitPath(tt.path, tt.verb)
			if tt.wantErr {
				assert.False(t, ok)

				return
			}

			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
