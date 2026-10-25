package apigatewayv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvalRouteSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		body string
		want string
	}{
		{name: "top level", expr: "$request.body.action", body: `{"action":"join"}`, want: "join"},
		{name: "nested", expr: "$request.body.a.b", body: `{"a":{"b":"x"}}`, want: "x"},
		{name: "braced with suffix", expr: "${request.body.v}-beta", body: `{"v":2}`, want: "2-beta"},
		{name: "missing key", expr: "$request.body.action", body: `{}`, want: ""},
		{name: "non json", expr: "$request.body.action", body: `nope`, want: ""},
		{name: "object value", expr: "$request.body.a", body: `{"a":{}}`, want: ""},
		{name: "escaped default", expr: `\$default`, body: `{}`, want: "$default"},
		{name: "unsupported variable", expr: "$request.header.x", body: `{}`, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, evalRouteSelection(tc.expr, []byte(tc.body)))
		})
	}
}
