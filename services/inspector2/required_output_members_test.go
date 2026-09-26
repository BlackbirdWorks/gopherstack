package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateOrganizationConfiguration_AutoEnableAlwaysPresent proves
// UpdateOrganizationConfigurationOutput.AutoEnable (required,
// api_op_UpdateOrganizationConfiguration.go) is never nil on a successful
// response. AutoEnable is also required on the *input*; previously the
// handler accepted a request omitting it and persisted a nil map, so
// DescribeOrganizationConfiguration's very next read -- and this op's own
// echoed response -- decoded a required field to nil instead of rejecting
// the malformed request the way real AWS's server-side validation would.
func TestUpdateOrganizationConfiguration_AutoEnableAlwaysPresent(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.UpdateOrganizationConfiguration(ctx, &inspector2sdk.UpdateOrganizationConfigurationInput{
		AutoEnable: &types.AutoEnable{
			Ec2: aws.Bool(true),
			Ecr: aws.Bool(false),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.AutoEnable, "AutoEnable is required on UpdateOrganizationConfigurationOutput")
	assert.True(t, *out.AutoEnable.Ec2)
	assert.False(t, *out.AutoEnable.Ecr)
}

// TestUpdateOrganizationConfiguration_MissingAutoEnableRejected proves a
// request omitting the required AutoEnable input member is rejected rather
// than succeeding with a null required output member. The real SDK client
// validates AutoEnable client-side and never sends such a request, so this
// drives the handler directly over HTTP the way handler_organization_test.go's
// other org-configuration tests do.
func TestUpdateOrganizationConfiguration_MissingAutoEnableRejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/organizationconfiguration/update", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}
