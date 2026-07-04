package iot_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// ---------------------------------------------------------------------------
// Encryption configuration
// ---------------------------------------------------------------------------

func TestFinalOps_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "customer_managed_requires_kms_key",
			update: map[string]any{
				"encryptionType": "CUSTOMER_MANAGED_KMS_KEY",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "customer_managed_succeeds",
			update: map[string]any{
				"encryptionType":   "CUSTOMER_MANAGED_KMS_KEY",
				"kmsKeyArn":        "arn:aws:kms:us-east-1:123456789012:key/abc",
				"kmsAccessRoleArn": "arn:aws:iam::123456789012:role/kms-role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_encryption_type",
			update: map[string]any{
				"encryptionType": "KMS_BASED",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			// Default (never configured) should describe as AWS-owned.
			rec := doRefRequest(t, h, http.MethodGet, "/encryption-configuration", nil, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "AWS_OWNED_KMS_KEY")

			rec = doRefRequest(t, h, http.MethodPatch, "/encryption-configuration", tt.update, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec = doRefRequest(t, h, http.MethodGet, "/encryption-configuration", nil, nil)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "CUSTOMER_MANAGED_KMS_KEY")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestAuthorization
// ---------------------------------------------------------------------------

func TestFinalOps_TestAuthorization(t *testing.T) {
	t.Parallel()

	const principal = "arn:aws:iot:us-east-1:123456789012:cert/abc123"

	h, b := newRefHandler()

	b.AddPolicyInternal(iot.Policy{
		PolicyName: "AllowConnect",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[
			{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}
		]}`,
	})

	require.NoError(t, b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
		PolicyName: "AllowConnect",
		Principal:  principal,
	}))

	tests := []struct {
		name       string
		actionType string
		wantDecide string
	}{
		{name: "allowed_action", actionType: "CONNECT", wantDecide: "ALLOWED"},
		{name: "implicit_deny_action", actionType: "PUBLISH", wantDecide: "IMPLICIT_DENY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"principal": principal,
				"authInfos": []map[string]any{
					{"actionType": tt.actionType, "resources": []string{"*"}},
				},
			}

			rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", body, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantDecide)
		})
	}

	t.Run("unknown_principal_required", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", map[string]any{
			"authInfos": []map[string]any{{"actionType": "CONNECT", "resources": []string{"*"}}},
		}, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown_policy_to_add", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", map[string]any{
			"principal":        principal,
			"policyNamesToAdd": []string{"NoSuchPolicy"},
			"authInfos":        []map[string]any{{"actionType": "CONNECT", "resources": []string{"*"}}},
		}, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// TestInvokeAuthorizer
// ---------------------------------------------------------------------------

func TestFinalOps_TestInvokeAuthorizer(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	_, err := b.CreateAuthorizer(&iot.CreateAuthorizerInput{
		AuthorizerName:        "my-authorizer",
		AuthorizerFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:auth",
		SigningDisabled:       true,
		Status:                "ACTIVE",
	})
	require.NoError(t, err)

	rec := doRefRequest(t, h, http.MethodPost, "/authorizer/my-authorizer/test", map[string]any{
		"token": "sometoken",
	}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"isAuthenticated":true`)
	assert.Contains(t, rec.Body.String(), "authz-")

	t.Run("unknown_authorizer", func(t *testing.T) {
		t.Parallel()

		unknownRec := doRefRequest(t, h, http.MethodPost, "/authorizer/no-such/test", map[string]any{
			"token": "x",
		}, nil)
		assert.Equal(t, http.StatusNotFound, unknownRec.Code)
	})

	t.Run("signing_required_missing_signature", func(t *testing.T) {
		t.Parallel()

		_, createErr := b.CreateAuthorizer(&iot.CreateAuthorizerInput{
			AuthorizerName: "signed-authorizer",
			Status:         "ACTIVE",
		})
		require.NoError(t, createErr)

		signedRec := doRefRequest(t, h, http.MethodPost, "/authorizer/signed-authorizer/test", map[string]any{
			"token": "sometoken",
		}, nil)
		require.Equal(t, http.StatusOK, signedRec.Code)
		assert.Contains(t, signedRec.Body.String(), `"isAuthenticated":false`)
	})
}

// ---------------------------------------------------------------------------
// DetachPrincipalPolicy
// ---------------------------------------------------------------------------

func TestFinalOps_DetachPrincipalPolicy(t *testing.T) {
	t.Parallel()

	const principal = "arn:aws:iot:us-east-1:123456789012:cert/xyz"

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		b.AddPolicyInternal(iot.Policy{PolicyName: "MyPolicy"})
		require.NoError(t, b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
			PolicyName: "MyPolicy",
			Principal:  principal,
		}))

		require.Len(t, b.ListPrincipalPolicies(principal), 1)

		rec := doRefRequest(t, h, http.MethodDelete, "/principal-policies/MyPolicy", nil,
			map[string]string{"X-Amzn-Iot-Principal": principal})
		require.Equal(t, http.StatusOK, rec.Code)

		assert.Empty(t, b.ListPrincipalPolicies(principal))
	})

	t.Run("unknown_policy_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/principal-policies/NoSuchPolicy", nil,
			map[string]string{"X-Amzn-Iot-Principal": principal})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// ConfirmTopicRuleDestination
// ---------------------------------------------------------------------------

func TestFinalOps_ConfirmTopicRuleDestination(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		dest, err := b.CreateTopicRuleDestination(&iot.CreateTopicRuleDestinationInput{
			DestinationConfiguration: &iot.TopicRuleDestinationConfiguration{
				HTTPURLConfiguration: &iot.HTTPURLDestinationConfiguration{
					ConfirmationURL: "https://example.com/confirm",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, "IN_PROGRESS", dest.Status)
		require.NotEmpty(t, dest.ConfirmationToken)

		rec := doRefRequest(t, h, http.MethodGet, "/confirmdestination/"+dest.ConfirmationToken, nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		got, err := b.GetTopicRuleDestination(dest.ARN)
		require.NoError(t, err)
		assert.Equal(t, "ENABLED", got.Status)
	})

	t.Run("invalid_token", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet, "/confirmdestination/bogus-token", nil, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// DeleteCommandExecution
// ---------------------------------------------------------------------------

func TestFinalOps_DeleteCommandExecution(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		b.AddCommandExecutionInternal("cmd-1", "exec-1", iot.IoTCommandExecution{
			CommandARN: "arn:aws:iot:us-east-1:123456789012:command/cmd-1",
			ThingARN:   "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
			Status:     "SUCCEEDED",
		})

		rec := doRefRequest(t, h, http.MethodDelete,
			"/command-executions/exec-1?targetArn=arn:aws:iot:us-east-1:123456789012:thing/my-thing", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		_, err := b.GetCommandExecution("cmd-1", "exec-1")
		require.Error(t, err)
	})

	t.Run("unknown_execution_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/command-executions/no-such", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// GetBehaviorModelTrainingSummaries
// ---------------------------------------------------------------------------

func TestFinalOps_GetBehaviorModelTrainingSummaries(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateSecurityProfile(&iot.CreateSecurityProfileInput{SecurityProfileName: "sp1"})
		require.NoError(t, err)

		b.AddBehaviorModelTrainingSummaryInternal("sp1", iot.BehaviorModelTrainingSummary{
			BehaviorName: "excessive-connects",
			ModelStatus:  "ACTIVE",
		})

		rec := doRefRequest(t, h, http.MethodGet,
			"/behavior-model-training/summaries?securityProfileName=sp1", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "excessive-connects")
	})

	t.Run("unknown_profile_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet,
			"/behavior-model-training/summaries?securityProfileName=no-such", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// ListOutgoingCertificates
// ---------------------------------------------------------------------------

func TestFinalOps_ListOutgoingCertificates(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{Status: "ACTIVE"})
	require.NoError(t, err)

	// Not pending transfer yet: should not appear.
	rec := doRefRequest(t, h, http.MethodGet, "/certificates-out-going", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), cert.CertificateID)

	// PENDING_TRANSFER is set via TransferCertificate, matching real AWS
	// behavior (UpdateCertificate rejects that status directly).
	require.NoError(t, b.TransferCertificate(cert.CertificateID, "123456789012"))

	rec = doRefRequest(t, h, http.MethodGet, "/certificates-out-going", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), cert.CertificateID)
}

// ---------------------------------------------------------------------------
// SBOM disassociation / validation results
// ---------------------------------------------------------------------------

func TestFinalOps_SbomDisassociateAndValidationResults(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateIoTPackageVersion("pkg1", "1.0", "", nil)
		require.NoError(t, err)

		_, err = b.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
			PackageName: "pkg1",
			VersionName: "1.0",
			Sbom: &iot.SbomDocument{
				S3Location: &iot.S3Location{Bucket: "my-bucket", Key: "sbom.json"},
			},
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SUCCEEDED")

		rec = doRefRequest(t, h, http.MethodDelete, "/packages/pkg1/versions/1.0/sbom", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "SUCCEEDED")
	})

	t.Run("missing_s3_location_fails_validation", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateIoTPackageVersion("pkg2", "1.0", "", nil)
		require.NoError(t, err)

		_, err = b.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
			PackageName: "pkg2",
			VersionName: "1.0",
			Sbom:        &iot.SbomDocument{},
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/packages/pkg2/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "FAILED")
	})

	t.Run("unknown_package_version_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/packages/nosuch/versions/1.0/sbom", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// ListMetricValues
// ---------------------------------------------------------------------------

func TestFinalOps_ListMetricValues(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()
		b.AddThingInternal(iot.Thing{ThingName: "my-thing"})

		count := int64(5)
		b.AddMetricValueInternal("my-thing", "aws:num-connections", iot.MetricDatapoint{
			Timestamp: 100,
			Value:     &iot.MetricValueData{Count: &count},
		})

		rec := doRefRequest(t, h, http.MethodGet,
			"/metric-values?thingName=my-thing&metricName=aws:num-connections", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "\"count\":5")
	})

	t.Run("unknown_thing_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet,
			"/metric-values?thingName=no-such&metricName=aws:num-connections", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// GetThingConnectivityData
// ---------------------------------------------------------------------------

func TestFinalOps_GetThingConnectivityData(t *testing.T) {
	t.Parallel()

	t.Run("default_not_connected", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()
		b.AddThingInternal(iot.Thing{ThingName: "my-thing"})

		rec := doRefRequest(t, h, http.MethodPost, "/things/my-thing/connectivity-data", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"connected":false`)
	})

	t.Run("seeded_connected", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()
		b.AddThingInternal(iot.Thing{ThingName: "my-thing"})
		b.SetThingConnectivityInternal("my-thing", iot.ThingConnectivityData{Connected: true, Timestamp: 42})

		rec := doRefRequest(t, h, http.MethodPost, "/things/my-thing/connectivity-data", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"connected":true`)
	})

	t.Run("unknown_thing_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodPost, "/things/no-such/connectivity-data", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// DescribeProvisioningTemplateVersion
// ---------------------------------------------------------------------------

func TestFinalOps_DescribeProvisioningTemplateVersion(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
			TemplateName: "tmpl1",
			TemplateBody: `{"Version":"2020-01-01"}`,
		})
		require.NoError(t, err)

		v, err := b.CreateProvisioningTemplateVersion("tmpl1", `{"Version":"2020-01-01","v":2}`)
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet,
			"/provisioning-templates/tmpl1/versions/"+itoa(v.VersionID), nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"versionId":2`)
		assert.Contains(t, rec.Body.String(), `2020-01-01`)
	})

	t.Run("unknown_template_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet, "/provisioning-templates/no-such/versions/1", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("unknown_version_404", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
			TemplateName: "tmpl2",
			TemplateBody: `{}`,
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/provisioning-templates/tmpl2/versions/999", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// itoa avoids importing strconv solely for one call site in tests.
func itoa(n int32) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	if neg {
		return "-" + string(digits)
	}

	return string(digits)
}

// ---------------------------------------------------------------------------
// ValidateSecurityProfileBehaviors
// ---------------------------------------------------------------------------

func TestFinalOps_ValidateSecurityProfileBehaviors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		behaviors  []map[string]any
		wantValid  bool
		wantStatus int
	}{
		{
			name: "valid_behavior",
			behaviors: []map[string]any{
				{
					"name":   "excessive-connects",
					"metric": "aws:num-connections",
					"criteria": map[string]any{
						"comparisonOperator": "greater-than",
						"durationSeconds":    300,
					},
				},
			},
			wantValid:  true,
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_name",
			behaviors: []map[string]any{
				{
					"criteria": map[string]any{"comparisonOperator": "greater-than"},
				},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_comparison_operator",
			behaviors: []map[string]any{
				{
					"name":     "bad-behavior",
					"criteria": map[string]any{"comparisonOperator": "not-a-real-operator"},
				},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_criteria",
			behaviors: []map[string]any{
				{"name": "no-criteria"},
			},
			wantValid:  false,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			rec := doRefRequest(t, h, http.MethodPost, "/security-profile-behaviors/validate", map[string]any{
				"behaviors": tt.behaviors,
			}, nil)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantValid {
				assert.Contains(t, rec.Body.String(), `"valid":true`)
			} else {
				assert.Contains(t, rec.Body.String(), `"valid":false`)
			}
		})
	}
}
