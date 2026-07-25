package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVpcEncryptionControl_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.91.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")
	require.NotEmpty(t, vpcID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<CreateVpcEncryptionControlResponse")
	assert.NotContains(t, createResp, "StubResponse")
	assert.Contains(t, createResp, "<mode>monitor</mode>")
	controlID := accuracyExtractXMLValue(createResp, "vpcEncryptionControlId")
	require.NotEmpty(t, controlID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"DescribeVpcEncryptionControls"},
		"VpcEncryptionControlId.1": []string{controlID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, controlID)

	modifyResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"ModifyVpcEncryptionControl"},
		"VpcEncryptionControlId":   []string{controlID},
		"Mode":                     []string{"enforce"},
		"InternetGatewayExclusion": []string{"enable"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<mode>enforce</mode>")
	assert.Contains(t, modifyResp, "<internetGateway><state>enabled</state></internetGateway>")

	blockingResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetVpcResourcesBlockingEncryptionEnforcement"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, blockingResp, "<GetVpcResourcesBlockingEncryptionEnforcementResponse")

	deleteResp, err := dispatchHandler(h, url.Values{
		"Action":                 []string{"DeleteVpcEncryptionControl"},
		"VpcEncryptionControlId": []string{controlID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteResp, "<DeleteVpcEncryptionControlResponse")

	// Duplicate create on the same VPC should fail, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err) // control was deleted above, so a fresh create succeeds again

	_, err = dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.Error(t, err)
}

// TestAccountVpcEncryptionControl verifies DescribeAccountVpcEncryptionControl
// and ModifyAccountVpcEncryptionControl (parity-4): the account-level
// singleton starts "unmanaged" and Modify mutates real, persisted state
// (mode + per-traffic-type exclusions), distinct from the per-VPC
// VpcEncryptionControl configuration exercised above.
func TestAccountVpcEncryptionControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vals     url.Values
		wantBody []string
		wantCode int
	}{
		{
			name:     "describe_default_is_unmanaged",
			vals:     url.Values{"Action": {"DescribeAccountVpcEncryptionControl"}},
			wantCode: http.StatusOK,
			wantBody: []string{
				"DescribeAccountVpcEncryptionControlResponse",
				"<mode>unmanaged</mode>",
				"<state>default-state</state>",
				"<managedBy>account</managedBy>",
			},
		},
		{
			name: "modify_sets_mode_and_exclusion",
			vals: url.Values{
				"Action":          {"ModifyAccountVpcEncryptionControl"},
				"Mode":            {"attempt-monitor"},
				"InternetGateway": {"enable"},
			},
			wantCode: http.StatusOK,
			wantBody: []string{
				"ModifyAccountVpcEncryptionControlResponse",
				"<mode>attempt-monitor</mode>",
				"<internetGateway><state>enabled</state></internetGateway>",
			},
		},
		{
			name: "modify_invalid_mode_fails",
			vals: url.Values{
				"Action": {"ModifyAccountVpcEncryptionControl"},
				"Mode":   {"bogus-mode"},
			},
			wantCode: http.StatusBadRequest,
			wantBody: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, tt.vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestAccountVpcEncryptionControl_ModifyPersistsAcrossDescribes verifies a
// Modify call's exclusion state is visible on a subsequent Describe (real
// mutated state, not an echoed-back-but-discarded value).
func TestAccountVpcEncryptionControl_ModifyPersistsAcrossDescribes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	modifyRec := postForm(t, h,
		"Action=ModifyAccountVpcEncryptionControl&Version=2016-11-15&Mode=attempt-enforce&Lambda=enable")
	require.Equal(t, http.StatusOK, modifyRec.Code)

	describeRec := postForm(t, h, "Action=DescribeAccountVpcEncryptionControl&Version=2016-11-15")
	require.Equal(t, http.StatusOK, describeRec.Code)
	body := describeRec.Body.String()
	assert.Contains(t, body, "<mode>attempt-enforce</mode>")
	assert.Contains(t, body, "<lambda><state>enabled</state></lambda>")
}
