package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		page string
		want []string
	}{
		{
			name: "sections",
			page: `<h3 id="services-a">A</h3><li><code class="code">amplify:apps</code></li>` +
				`<li><code class="code">amplify:apps/branches</code></li>` +
				`<h3 id="services-b">B</h3><code class="code">sqs:queue</code>` +
				`<h2 id="x">end</h2><code class="code">ec2:ignored</code>`,
			want: []string{"amplify:apps", "amplify:apps/branches", "sqs:queue"},
		},
		{name: "empty", page: "<p>none</p>", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parse(tt.page))
		})
	}
}
