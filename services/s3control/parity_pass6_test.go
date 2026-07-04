package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	agLocationPath = "/v20180820/accessgrantsinstance/location"
	agLocationXML  = `<CreateAccessGrantsLocationRequest>` +
		`<LocationScope>s3://</LocationScope>` +
		`<IAMRoleArn>arn:aws:iam::000000000000:role/MyRole</IAMRoleArn>` +
		`</CreateAccessGrantsLocationRequest>`
	agLocationNoRoleXML = `<CreateAccessGrantsLocationRequest>` +
		`<LocationScope>s3://</LocationScope>` +
		`</CreateAccessGrantsLocationRequest>`
	agLocationEmptyRoleXML = `<CreateAccessGrantsLocationRequest>` +
		`<LocationScope>s3://</LocationScope>` +
		`<IAMRoleArn></IAMRoleArn>` +
		`</CreateAccessGrantsLocationRequest>`
)

// TestParity_CreateAccessGrantsLocation_RequiresIAMRoleArn verifies that
// CreateAccessGrantsLocation rejects requests with a missing or empty
// IAMRoleArn. Real AWS returns 400 for this case; the emulator previously
// silently stored the location with an empty role ARN.
func TestParity_CreateAccessGrantsLocation_RequiresIAMRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "absent_iam_role_arn_rejected",
			body:     agLocationNoRoleXML,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_iam_role_arn_rejected",
			body:     agLocationEmptyRoleXML,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_iam_role_arn_accepted",
			body:     agLocationXML,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3Request(t, h, http.MethodPost, agLocationPath, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateAccessGrantsLocation status for case %q", tt.name)
		})
	}
}
