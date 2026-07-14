package kms_test

// replicate_key_policy_test.go — regression for ReplicateKey honoring an inline Policy.
//
// Terraform's aws_kms_replica_key resource (like aws_kms_key) can set an inline `policy`
// argument, which the AWS provider sends as ReplicateKeyInput.Policy. Just like the
// already-fixed CreateKey bug (see create_key_policy_test.go), if ReplicateKey drops the
// Policy field, GetKeyPolicy on the replica returns the synthesized default forever and
// the provider's post-apply poll never converges -- a perpetual-drift / apply-timeout bug.
//
// The real aws-sdk-go-v2 ReplicateKeyInput carries a Policy field (confirmed against
// aws-sdk-go-v2/service/kms@v1.54.0's api_op_ReplicateKey.go): "The key policy is not a
// shared property of multi-Region keys... KMS does not synchronize this property", so the
// replica must get its own independently-stored policy rather than inheriting the
// primary's.
//
// Routed through the full HTTP handler (like a real region-scoped Terraform provider
// client would call it) rather than the backend directly: the replica lives in a
// different region than the primary, and GetKeyPolicy resolves a bare KeyId against the
// request's own region rather than searching every region, exactly like a real AWS
// regional endpoint would.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func Test_ReplicateKey_InlinePolicy(t *testing.T) {
	t.Parallel()

	const customPolicy = `{"Version":"2012-10-17","Statement":[{"Sid":"ReplicaCustom",` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/admin"},` +
		`"Action":"kms:*","Resource":"*"}]}`

	tests := []struct {
		name       string
		policy     string
		wantPolicy string // empty => expect the synthesized default (non-empty, != customPolicy)
		wantErr    bool
	}{
		{
			name:       "inline policy stored and returned verbatim on replica",
			policy:     customPolicy,
			wantPolicy: customPolicy,
		},
		{
			name:       "no policy falls back to default on replica",
			policy:     "",
			wantPolicy: "",
		},
		{
			name:    "malformed policy rejected at replicate, no replica created",
			policy:  `{"not":"a policy"}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend)

			createRec := kmsCall(t, h, e, "CreateKey", "us-east-1", `{"MultiRegion":true}`)
			require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

			var createOut kms.CreateKeyOutput
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
			primaryID := createOut.KeyMetadata.KeyID

			replicateBody := `{"KeyId":"` + primaryID + `","ReplicaRegion":"us-west-2","Policy":` +
				strconvQuote(tc.policy) + `}`
			replicateRec := kmsCall(t, h, e, "ReplicateKey", "us-east-1", replicateBody)

			if tc.wantErr {
				assert.Equal(t, http.StatusBadRequest, replicateRec.Code)

				return
			}
			require.Equal(t, http.StatusOK, replicateRec.Code, replicateRec.Body.String())

			var replicateOut kms.ReplicateKeyOutput
			require.NoError(t, json.Unmarshal(replicateRec.Body.Bytes(), &replicateOut))
			replicaID := replicateOut.ReplicaKeyMetadata.KeyID
			require.NotEmpty(t, replicaID)

			// GetKeyPolicy against the replica's own region (us-west-2), exactly as a
			// region-scoped Terraform provider client managing the replica would call it.
			polRec := kmsCall(t, h, e, "GetKeyPolicy", "us-west-2", `{"KeyId":"`+replicaID+`"}`)
			require.Equal(t, http.StatusOK, polRec.Code, polRec.Body.String())

			var polOut kms.GetKeyPolicyOutput
			require.NoError(t, json.Unmarshal(polRec.Body.Bytes(), &polOut))
			assert.Equal(t, "default", polOut.PolicyName)

			if tc.wantPolicy != "" {
				assert.Equal(t, tc.wantPolicy, polOut.Policy,
					"GetKeyPolicy on the replica must return the policy supplied to ReplicateKey")
			} else {
				assert.NotEmpty(t, polOut.Policy, "default policy must be non-empty")
				assert.NotEqual(t, customPolicy, polOut.Policy)
			}

			// The key policy is explicitly NOT a shared multi-region property: the
			// primary's own policy must be unaffected by the replica's Policy param.
			primaryPolRec := kmsCall(t, h, e, "GetKeyPolicy", "us-east-1", `{"KeyId":"`+primaryID+`"}`)
			require.Equal(t, http.StatusOK, primaryPolRec.Code)

			var primaryPolOut kms.GetKeyPolicyOutput
			require.NoError(t, json.Unmarshal(primaryPolRec.Body.Bytes(), &primaryPolOut))
			assert.NotEqual(t, customPolicy, primaryPolOut.Policy)
		})
	}
}
