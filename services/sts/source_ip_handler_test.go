package sts_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestHandler_RemoteAddrBecomesSourceIP(t *testing.T) {
	t.Parallel()

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"sts:AssumeRole","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`

	tests := []struct {
		name       string
		remoteAddr string
		wantStatus int
	}{
		{name: "inside_cidr", remoteAddr: "10.2.3.4:51234", wantStatus: http.StatusOK},
		{name: "outside_cidr", remoteAddr: "198.51.100.9:51234", wantStatus: http.StatusForbidden},
		{name: "ipv6_outside_cidr", remoteAddr: "[2001:db8::1]:51234", wantStatus: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			h := sts.NewHandler(backend)
			e := echo.New()

			form := url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {"arn:aws:iam::123456789012:role/MyRole"},
				"RoleSessionName": {"session"},
			}
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.RemoteAddr = tt.remoteAddr
			req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
				Principal: &awsmeta.Principal{Arn: "arn:aws:iam::123456789012:user/alice"},
			}))

			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
