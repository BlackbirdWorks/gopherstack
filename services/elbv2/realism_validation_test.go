package elbv2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceNames_Realism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantCode int
	}{
		{
			name: "tg underscore",
			vals: url.Values{
				"Action":   {"CreateTargetGroup"},
				"Name":     {"bad_tg"},
				"Protocol": {"HTTP"},
				"Port":     {"80"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "tg hyphen ok",
			vals: url.Values{
				"Action":   {"CreateTargetGroup"},
				"Name":     {"good-tg"},
				"Protocol": {"HTTP"},
				"Port":     {"80"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "lb internal prefix",
			vals:     url.Values{"Action": {"CreateLoadBalancer"}, "Name": {"internal-x"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lb underscore",
			vals:     url.Values{"Action": {"CreateLoadBalancer"}, "Name": {"bad_lb"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestARNs_UniqueAndShaped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	lb := mustCreateLB(t, h, "lb1")
	tg := mustCreateTG(t, h, "tg1")
	listener := mustCreateListener(t, h, lb, tg)

	lb2 := mustCreateLB(t, h, "lb2")

	assert.Regexp(t, `:loadbalancer/app/lb1/[0-9a-f]{16}$`, lb)
	assert.Regexp(t, `:targetgroup/tg1/[0-9a-f]{16}$`, tg)
	assert.Regexp(t, `:listener/app/lb1/[0-9a-f]{16}/[0-9a-f]{16}$`, listener)
	assert.NotEqual(t, lb, lb2)
}

func TestErrorMessage_NoCodePrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	mustCreateLB(t, h, "dup")

	rec := doELBv2(t, h, url.Values{"Action": {"CreateLoadBalancer"}, "Name": {"dup"}})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>DuplicateLoadBalancerName</Code>")
	assert.NotContains(t, rec.Body.String(), "<Message>DuplicateLoadBalancerName")
}
