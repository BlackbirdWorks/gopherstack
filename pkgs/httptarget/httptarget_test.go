package httptarget_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/httptarget"
)

func TestParseExecuteAPIARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want httptarget.Route
		ok   bool
	}{
		{
			name: "full",
			arn:  "arn:aws:execute-api:us-west-2:000000000000:abc123/prod/GET/pets/*/toys",
			want: httptarget.Route{
				APIID: "abc123", Stage: "prod", Method: "GET", Path: "/pets/*/toys", Region: "us-west-2",
			},
			ok: true,
		},
		{
			name: "root path",
			arn:  "arn:aws:execute-api:us-east-1:000000000000:abc/dev/post",
			want: httptarget.Route{APIID: "abc", Stage: "dev", Method: "POST", Path: "/", Region: "us-east-1"},
			ok:   true,
		},
		{
			name: "any method",
			arn:  "arn:aws:execute-api:us-east-1:000000000000:abc/dev/*/x",
			want: httptarget.Route{APIID: "abc", Stage: "dev", Method: "POST", Path: "/x", Region: "us-east-1"},
			ok:   true,
		},
		{name: "wrong service", arn: "arn:aws:sqs:us-east-1:000000000000:q"},
		{name: "missing stage", arn: "arn:aws:execute-api:us-east-1:000000000000:abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := httptarget.ParseExecuteAPIARN(tt.arn)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFillWildcards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		want   string
		values []string
	}{
		{name: "none", in: "/a/*", want: "/a/*"},
		{name: "ordered", in: "/a/*/b/*", values: []string{"x", "y"}, want: "/a/x/b/y"},
		{name: "surplus wildcard", in: "/a/*/b/*", values: []string{"x"}, want: "/a/x/b/*"},
		{name: "escaped", in: "/a/*", values: []string{"x y/z"}, want: "/a/x%20y%2Fz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, httptarget.FillWildcards(tt.in, tt.values))
		})
	}
}
