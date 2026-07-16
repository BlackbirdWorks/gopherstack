package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- PutResourcePolicy / GetResourcePolicy / DeleteResourcePolicy ----

func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "put_success",
			body: "Action=PutResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:test&Policy={}",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutResourcePolicyResponse"},
		},
		{
			name: "get_success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t, h,
					"Action=PutResourcePolicy&Version=2012-12-01"+
						"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:c1&Policy={}",
				)
			},
			body: "Action=GetResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:c1",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetResourcePolicyResponse"},
		},
		{
			name: "get_not_found",
			body: "Action=GetResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "delete_success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=PutResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:del&Policy={}")
			},
			body:     "Action=DeleteResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:del",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			body:     "Action=DeleteResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:missing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
