package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAPIMIPFilterPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantPut    int
		wantStatus int
	}{
		{name: "allow listed address", policy: `<ip-filter action="allow"><address>127.0.0.1</address></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusOK},
		{name: "allow other address", policy: `<ip-filter action="allow"><address>10.1.2.3</address></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusForbidden},
		{name: "allow range containing client",
			policy:  `<ip-filter action="allow"><address-range from="127.0.0.0" to="127.255.255.255"/></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusOK},
		{name: "allow range elsewhere",
			policy:  `<ip-filter action="allow"><address-range from="10.0.0.1" to="10.0.0.9"/></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusForbidden},
		{name: "forbid listed address", policy: `<ip-filter action="forbid"><address>127.0.0.1</address></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusForbidden},
		{name: "forbid other range",
			policy:  `<ip-filter action="forbid"><address-range from="10.0.0.1" to="10.0.0.9"/></ip-filter>`,
			wantPut: http.StatusCreated, wantStatus: http.StatusOK},
		{name: "bad action", policy: `<ip-filter action="deny"><address>127.0.0.1</address></ip-filter>`,
			wantPut: http.StatusBadRequest},
		{name: "no entries", policy: `<ip-filter action="allow"></ip-filter>`, wantPut: http.StatusBadRequest},
		{name: "bad address", policy: `<ip-filter action="allow"><address>not-an-ip</address></ip-filter>`,
			wantPut: http.StatusBadRequest},
		{name: "reversed range",
			policy:  `<ip-filter action="allow"><address-range from="10.0.0.9" to="10.0.0.1"/></ip-filter>`,
			wantPut: http.StatusBadRequest},
		{name: "zoned address", policy: `<ip-filter action="allow"><address>fe80::1%eth0</address></ip-filter>`,
			wantPut: http.StatusBadRequest},
		{name: "mixed family range",
			policy:  `<ip-filter action="allow"><address-range from="10.0.0.1" to="::1"/></ip-filter>`,
			wantPut: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.policy(t, "/apis/echo", policyDoc(tt.policy, "", ""), tt.wantPut)

			if tt.wantPut != http.StatusCreated {
				return
			}

			status, body, _ := e.call(t, http.MethodGet, "/echo/items/1", "", nil)
			assert.Equal(t, tt.wantStatus, status, body)
		})
	}
}

func TestAPIMIPFilterIgnoresForwardedFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		want   int
	}{
		{name: "allow spoofed", policy: `<ip-filter action="allow"><address>10.1.2.3</address></ip-filter>`,
			want: http.StatusForbidden},
		{name: "forbid spoofed", policy: `<ip-filter action="forbid"><address>127.0.0.1</address></ip-filter>`,
			want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.policy(t, "/apis/echo", policyDoc(tt.policy, "", ""), http.StatusCreated)

			status, body, _ := e.call(t, http.MethodGet, "/echo/items/1", "",
				map[string]string{"X-Forwarded-For": "10.1.2.3", "X-Real-IP": "10.1.2.3"})
			assert.Equal(t, tt.want, status, body)
		})
	}
}
