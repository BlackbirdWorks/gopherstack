package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- ValidateRoleARN (table-driven) -----------------------------------------------

func TestValidateRoleARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "empty ARN is valid (no role specified)",
			roleARN: "",
			wantErr: false,
		},
		{
			name:    "valid standard role ARN",
			roleARN: "arn:aws:iam::123456789012:role/MyCloudFormationRole",
			wantErr: false,
		},
		{
			name:    "valid role ARN with path",
			roleARN: "arn:aws:iam::123456789012:role/path/to/MyRole",
			wantErr: false,
		},
		{
			name:    "gov cloud role ARN",
			roleARN: "arn:aws-gov:iam::123456789012:role/GovRole",
			wantErr: false,
		},
		{
			name:    "cn cloud role ARN",
			roleARN: "arn:aws-cn:iam::123456789012:role/ChinaRole",
			wantErr: false,
		},
		{
			name:    "invalid: not an ARN",
			roleARN: "not-an-arn",
			wantErr: true,
		},
		{
			name:    "invalid: S3 bucket ARN instead of IAM role",
			roleARN: "arn:aws:s3:::my-bucket",
			wantErr: true,
		},
		{
			name:    "invalid: missing account ID",
			roleARN: "arn:aws:iam:::role/MyRole",
			wantErr: true,
		},
		{
			name:    "invalid: ec2 resource type",
			roleARN: "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := cloudformation.ValidateRoleARN(tc.roleARN)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudformation.ErrInvalidRoleARN)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- CreateStack IAM PassRole validation (table-driven) ---------------------------

func TestCreateStack_RoleARN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "valid role ARN accepted",
			roleARN: "arn:aws:iam::123456789012:role/CFNRole",
			wantErr: false,
		},
		{
			name:    "empty role ARN accepted",
			roleARN: "",
			wantErr: false,
		},
		{
			name:    "invalid role ARN rejected",
			roleARN: "not-a-valid-arn",
			wantErr: true,
			errIs:   cloudformation.ErrInvalidRoleARN,
		},
		{
			name:    "S3 ARN rejected as role ARN",
			roleARN: "arn:aws:s3:::my-bucket",
			wantErr: true,
			errIs:   cloudformation.ErrInvalidRoleARN,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "role-test", simpleTemplate, nil,
				cloudformation.StackOptions{RoleARN: tc.roleARN})
			if tc.wantErr {
				require.Error(t, err)
				if tc.errIs != nil {
					require.ErrorIs(t, err, tc.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- IAM Capability requirement (table-driven) ------------------------------------

func TestCreateStack_IAMCapabilityRequired(t *testing.T) {
	t.Parallel()

	iamTemplate := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{
			"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{}}}
		}
	}`

	tests := []struct {
		errIs        error
		name         string
		template     string
		capabilities []string
		wantErr      bool
	}{
		{
			name:         "IAM template without capabilities fails",
			template:     iamTemplate,
			capabilities: nil,
			wantErr:      true,
			errIs:        cloudformation.ErrInsufficientCapabilities,
		},
		{
			name:         "IAM template with CAPABILITY_IAM succeeds",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_IAM"},
			wantErr:      false,
		},
		{
			name:         "IAM template with CAPABILITY_NAMED_IAM succeeds",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_NAMED_IAM"},
			wantErr:      false,
		},
		{
			// CAPABILITY_AUTO_EXPAND only authorizes macro/transform expansion
			// (e.g. SAM); it does not grant permission to create IAM resources
			// declared directly in the template, so it must NOT substitute for
			// CAPABILITY_IAM / CAPABILITY_NAMED_IAM.
			name:         "IAM template with only CAPABILITY_AUTO_EXPAND still fails",
			template:     iamTemplate,
			capabilities: []string{"CAPABILITY_AUTO_EXPAND"},
			wantErr:      true,
			errIs:        cloudformation.ErrInsufficientCapabilities,
		},
		{
			name:         "non-IAM template without capabilities succeeds",
			template:     simpleTemplate,
			capabilities: nil,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "cap-check", tc.template, nil,
				cloudformation.StackOptions{Capabilities: tc.capabilities})
			if tc.wantErr {
				require.Error(t, err)
				if tc.errIs != nil {
					require.ErrorIs(t, err, tc.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- UpdateStack IAM Capability (table-driven) ------------------------------------

func TestUpdateStack_IAMCapabilityRequired(t *testing.T) {
	t.Parallel()

	iamTemplate := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{
			"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{}}}
		}
	}`

	tests := []struct {
		name       string
		updateOpts cloudformation.StackOptions
		wantErr    bool
	}{
		{
			name:       "update with IAM template and no capabilities fails",
			updateOpts: cloudformation.StackOptions{},
			wantErr:    true,
		},
		{
			name:       "update with IAM template and CAPABILITY_IAM succeeds",
			updateOpts: cloudformation.StackOptions{Capabilities: []string{"CAPABILITY_IAM"}},
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "iam-upd", simpleTemplate, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)

			_, err = b.UpdateStack(t.Context(), "iam-upd", iamTemplate, nil, tc.updateOpts)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, cloudformation.ErrInsufficientCapabilities)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
