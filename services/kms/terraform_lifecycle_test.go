package kms_test

// terraform_lifecycle_test.go — end-to-end read-after-write parity test that models,
// against the handler directly (raw X-Amz-Target JSON requests, no real Terraform), the
// exact sequence the Terraform AWS provider performs for aws_kms_key across apply and
// refresh:
//
//	CreateKey (Policy + Tags + KeySpec)
//	  -> DescribeKey / GetKeyPolicy / ListResourceTags / GetKeyRotationStatus must all
//	     echo the configured values EXACTLY, and IDENTICALLY across repeated calls
//	     (a mismatch here is exactly what makes `terraform plan` show perpetual drift,
//	     or makes the post-apply waiter never converge)
//	  -> CreateAlias -> ListAliases must show it
//	  -> ScheduleKeyDeletion -> DescribeKey must immediately reflect PendingDeletion +
//	     DeletionDate + PendingDeletionWindowInDays (the destroy-time waiter Terraform
//	     polls; a mismatch here is exactly the class of bug that makes `terraform destroy`
//	     hang for the full 10-minute waiter timeout).
//
// Uses raw JSON request bodies (not the Go SDK types) throughout so a wire-key casing
// regression (e.g. "KeyId" vs "KeyID") would also be caught, not just a Go-side one.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// tagsWireOutput mirrors the (package-private) ListResourceTags response shape.
type tagsWireOutput struct {
	Tags []struct {
		TagKey   string `json:"TagKey"`
		TagValue string `json:"TagValue"`
	} `json:"Tags"`
	Truncated bool `json:"Truncated"`
}

func TestTerraformLifecycle_KMSKey(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	const region = "us-east-1"
	const policy = `{"Version":"2012-10-17","Statement":[{"Sid":"TFPolicy",` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:root"},` +
		`"Action":"kms:*","Resource":"*"}]}`

	// --- CreateKey with Policy + Tags + KeySpec, exactly as the AWS provider sends it. ---
	createBody := `{` +
		`"Description":"terraform-managed key",` +
		`"KeyUsage":"ENCRYPT_DECRYPT",` +
		`"KeySpec":"SYMMETRIC_DEFAULT",` +
		`"Policy":` + strconvQuote(policy) + `,` +
		`"Tags":[{"TagKey":"Environment","TagValue":"test"},{"TagKey":"ManagedBy","TagValue":"terraform"}]` +
		`}`

	createRec := kmsCall(t, h, e, "CreateKey", region, createBody)
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	keyID := createOut.KeyMetadata.KeyID
	require.NotEmpty(t, keyID)
	assert.Equal(t, "terraform-managed key", createOut.KeyMetadata.Description)
	assert.Equal(t, "SYMMETRIC_DEFAULT", createOut.KeyMetadata.KeySpec)
	assert.Equal(t, kms.KeyUsageEncryptDecrypt, createOut.KeyMetadata.KeyUsage)
	assert.Equal(t, kms.KeyStateEnabled, createOut.KeyMetadata.KeyState)
	assert.True(t, createOut.KeyMetadata.Enabled)

	descBody := `{"KeyId":"` + keyID + `"}`

	// --- DescribeKey: two consecutive reads (simulating two `terraform plan` runs)
	// must be byte-identical AND match the CreateKey config exactly. ---
	descRec1 := kmsCall(t, h, e, "DescribeKey", region, descBody)
	descRec2 := kmsCall(t, h, e, "DescribeKey", region, descBody)
	require.Equal(t, http.StatusOK, descRec1.Code)
	assert.JSONEq(t, descRec1.Body.String(), descRec2.Body.String(),
		"repeated DescribeKey must be byte-identical -- any diff here is perpetual terraform plan drift")

	var desc1 kms.DescribeKeyOutput
	require.NoError(t, json.Unmarshal(descRec1.Body.Bytes(), &desc1))
	assert.Equal(t, "terraform-managed key", desc1.KeyMetadata.Description)
	assert.Equal(t, "SYMMETRIC_DEFAULT", desc1.KeyMetadata.KeySpec)
	assert.Equal(t, "SYMMETRIC_DEFAULT", desc1.KeyMetadata.CustomerMasterKeySpec,
		"the deprecated CustomerMasterKeySpec alias must mirror KeySpec")
	assert.Equal(t, kms.KeyUsageEncryptDecrypt, desc1.KeyMetadata.KeyUsage)
	assert.Equal(t, kms.KeyStateEnabled, desc1.KeyMetadata.KeyState)
	assert.True(t, desc1.KeyMetadata.Enabled)
	assert.False(t, desc1.KeyMetadata.MultiRegion)
	assert.Zero(t, desc1.KeyMetadata.DeletionDate, "DeletionDate must be unset before ScheduleKeyDeletion")

	// --- GetKeyPolicy: must return exactly the inline policy, identically on repeat. ---
	polBody := `{"KeyId":"` + keyID + `"}`
	polRec1 := kmsCall(t, h, e, "GetKeyPolicy", region, polBody)
	polRec2 := kmsCall(t, h, e, "GetKeyPolicy", region, polBody)
	require.Equal(t, http.StatusOK, polRec1.Code)
	assert.JSONEq(t, polRec1.Body.String(), polRec2.Body.String(),
		"repeated GetKeyPolicy must be byte-identical -- a regenerated default here is the exact bug "+
			"that made aws_kms_key poll to a 10-minute timeout")

	var polOut kms.GetKeyPolicyOutput
	require.NoError(t, json.Unmarshal(polRec1.Body.Bytes(), &polOut))
	assert.JSONEq(t, policy, polOut.Policy, "GetKeyPolicy must return exactly the policy supplied to CreateKey")

	// --- ListResourceTags: must return exactly the tags supplied to CreateKey. ---
	tagsBody := `{"KeyId":"` + keyID + `"}`
	tagsRec1 := kmsCall(t, h, e, "ListResourceTags", region, tagsBody)
	tagsRec2 := kmsCall(t, h, e, "ListResourceTags", region, tagsBody)
	require.Equal(t, http.StatusOK, tagsRec1.Code)
	assert.JSONEq(t, tagsRec1.Body.String(), tagsRec2.Body.String(),
		"repeated ListResourceTags must be byte-identical")

	var tagsOut tagsWireOutput
	require.NoError(t, json.Unmarshal(tagsRec1.Body.Bytes(), &tagsOut))

	gotTags := make(map[string]string, len(tagsOut.Tags))
	for _, tg := range tagsOut.Tags {
		gotTags[tg.TagKey] = tg.TagValue
	}
	assert.Equal(t, map[string]string{"Environment": "test", "ManagedBy": "terraform"}, gotTags,
		"ListResourceTags must match CreateKey's Tags exactly (no more, no fewer)")

	// --- GetKeyRotationStatus: default must be false and stable across reads. ---
	rotBody := `{"KeyId":"` + keyID + `"}`
	rotRec1 := kmsCall(t, h, e, "GetKeyRotationStatus", region, rotBody)
	rotRec2 := kmsCall(t, h, e, "GetKeyRotationStatus", region, rotBody)
	require.Equal(t, http.StatusOK, rotRec1.Code)
	assert.JSONEq(t, rotRec1.Body.String(), rotRec2.Body.String())

	var rotOut kms.GetKeyRotationStatusOutput
	require.NoError(t, json.Unmarshal(rotRec1.Body.Bytes(), &rotOut))
	assert.False(t, rotOut.KeyRotationEnabled, "key rotation must default to disabled")

	// --- CreateAlias -> ListAliases must show it, resolving TargetKeyId correctly. ---
	aliasBody := `{"AliasName":"alias/tf-lifecycle-test","TargetKeyId":"` + keyID + `"}`
	aliasRec := kmsCall(t, h, e, "CreateAlias", region, aliasBody)
	require.Equal(t, http.StatusOK, aliasRec.Code, aliasRec.Body.String())

	listAliasesBody := `{"KeyId":"` + keyID + `"}`
	listAliasesRec := kmsCall(t, h, e, "ListAliases", region, listAliasesBody)
	require.Equal(t, http.StatusOK, listAliasesRec.Code)

	var aliasesOut kms.ListAliasesOutput
	require.NoError(t, json.Unmarshal(listAliasesRec.Body.Bytes(), &aliasesOut))
	require.Len(t, aliasesOut.Aliases, 1)
	assert.Equal(t, "alias/tf-lifecycle-test", aliasesOut.Aliases[0].AliasName)
	assert.Equal(t, keyID, aliasesOut.Aliases[0].TargetKeyID)

	// --- ScheduleKeyDeletion (the `terraform destroy` path) -> DescribeKey must
	// IMMEDIATELY reflect PendingDeletion + DeletionDate + PendingDeletionWindowInDays,
	// with no async lag Terraform's destroy waiter would have to poll through. ---
	scheduleBody := `{"KeyId":"` + keyID + `","PendingWindowInDays":7}`
	scheduleRec := kmsCall(t, h, e, "ScheduleKeyDeletion", region, scheduleBody)
	require.Equal(t, http.StatusOK, scheduleRec.Code, scheduleRec.Body.String())

	var scheduleOut kms.ScheduleKeyDeletionOutput
	require.NoError(t, json.Unmarshal(scheduleRec.Body.Bytes(), &scheduleOut))
	assert.Equal(t, kms.KeyStatePendingDeletion, scheduleOut.KeyState)
	assert.Positive(t, scheduleOut.DeletionDate)
	assert.Equal(t, 7, scheduleOut.PendingWindowInDays)

	finalDescRec := kmsCall(t, h, e, "DescribeKey", region, descBody)
	require.Equal(t, http.StatusOK, finalDescRec.Code)

	var finalDesc kms.DescribeKeyOutput
	require.NoError(t, json.Unmarshal(finalDescRec.Body.Bytes(), &finalDesc))
	assert.Equal(t, kms.KeyStatePendingDeletion, finalDesc.KeyMetadata.KeyState)
	assert.Positive(t, finalDesc.KeyMetadata.DeletionDate)
	assert.Equal(t, 7, finalDesc.KeyMetadata.PendingDeletionWindowInDays)
	assert.False(t, finalDesc.KeyMetadata.Enabled)
}

// strconvQuote JSON-quotes s (equivalent to strconv.Quote for the ASCII/simple policy
// strings this test uses, without pulling in strconv just for one call site).
func strconvQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return string(b)
}
