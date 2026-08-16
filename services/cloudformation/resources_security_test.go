package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestResourceCreator_SSMParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logicalID  string
		props      map[string]any
		wantPhysID string
		doDelete   bool
	}{
		{
			name:      "explicit_name",
			logicalID: "MyParam",
			props: map[string]any{
				"Name":  "/cfn/test-param",
				"Type":  "String",
				"Value": "hello",
			},
			wantPhysID: "/cfn/test-param",
			doDelete:   true,
		},
		{
			name:       "default_name",
			logicalID:  "MySSMParam",
			props:      map[string]any{"Value": "val"},
			wantPhysID: "/MySSMParam",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::SSM::Parameter",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SSM::Parameter", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_KMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props     map[string]any
		name      string
		logicalID string
		doDelete  bool
	}{
		{
			name:      "with_description",
			logicalID: "MyKey",
			props:     map[string]any{"Description": "cfn test key"},
			doDelete:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::KMS::Key", tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::KMS::Key", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_SecretsManagerSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		doDelete     bool
	}{
		{
			name:      "explicit_name",
			logicalID: "MySecret",
			props: map[string]any{
				"Name":         "cfn-test-secret",
				"SecretString": `{"key":"value"}`,
			},
			wantContains: "cfn-test-secret",
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultSecret",
			props:        map[string]any{"SecretString": "secret"},
			wantContains: "MyDefaultSecret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::SecretsManager::Secret",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SecretsManager::Secret", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_IAMResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		wantContains string
	}{
		{
			name:         "iam_role",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props: map[string]any{
				"RoleName":                 "cfn-my-role",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantContains: "cfn-my-role",
		},
		{
			name:         "iam_policy",
			logicalID:    "MyPolicy",
			resourceType: "AWS::IAM::Policy",
			props: map[string]any{
				"PolicyName":     "cfn-my-policy",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantContains: "cfn-my-policy",
		},
		{
			name:         "iam_managed_policy",
			logicalID:    "MyManagedPolicy",
			resourceType: "AWS::IAM::ManagedPolicy",
			props: map[string]any{
				"ManagedPolicyName": "cfn-managed-policy",
				"PolicyDocument":    `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantContains: "cfn-managed-policy",
		},
		{
			name:         "iam_instance_profile",
			logicalID:    "MyInstanceProfile",
			resourceType: "AWS::IAM::InstanceProfile",
			props: map[string]any{
				"InstanceProfileName": "cfn-instance-profile",
			},
			wantContains: "cfn-instance-profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), tt.resourceType, physID, tt.props)
			require.NoError(t, err)
		})
	}
}
