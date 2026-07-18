package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ProfileCertificateIDsRoundtrip verifies that CertificateIds set via UpdateProfile
// are returned by DescribeProfile. Real AWS stores CertificateIds on AS2 profiles.
func TestHandler_ProfileCertificateIDsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "MY-AS2-ID",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateProfile failed: %s", createRec.Body.String())

	var createOut struct {
		ProfileID string `json:"ProfileId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	certIDs := []string{"cert-aaa111", "cert-bbb222"}
	updateRec := doTransferRequest(t, h, "UpdateProfile", map[string]any{
		"ProfileId":      createOut.ProfileID,
		"CertificateIds": certIDs,
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateProfile failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": createOut.ProfileID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeProfile failed: %s", descRec.Body.String())

	var descOut struct {
		Profile struct {
			CertificateIDs []string `json:"CertificateIds"`
		} `json:"Profile"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, certIDs, descOut.Profile.CertificateIDs)
}

func TestHandler_DescribeProfileIncludesArnAndTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-partner-1",
		"ProfileType": "LOCAL",
		"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profile := resp["Profile"].(map[string]any)

	arn, hasArn := profile["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeProfile response")
	assert.Contains(t, arn, profileID, "Arn must contain ProfileId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := profile["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeProfile response")
	assert.Len(t, tags, 1)
}

func TestHandler_ListProfilesIncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-partner-2",
		"ProfileType": "PARTNER",
	})

	rec := doTransferRequest(t, h, "ListProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profiles := resp["Profiles"].([]any)
	require.NotEmpty(t, profiles)

	item := profiles[0].(map[string]any)
	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListProfiles items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestHandler_ProfileARNContainsAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "AS2-x",
		"ProfileType": "PARTNER",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	profile := resp["Profile"].(map[string]any)

	// newTestHandler uses testAccountID = "123456789012" and testRegion = "us-east-1"
	arn := profile["Arn"].(string)
	assert.Contains(t, arn, "123456789012")
	assert.Contains(t, arn, "us-east-1")
	assert.Contains(t, arn, "profile/"+profileID)
}

// TestHandler_ProfileTypeValidation verifies HTTP returns 400 for invalid profile type.
func TestHandler_ProfileTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "local",
			body:     map[string]any{"ProfileType": "LOCAL", "As2Id": "myid"},
			wantCode: http.StatusOK,
		},
		{
			name:     "partner",
			body:     map[string]any{"ProfileType": "PARTNER", "As2Id": "myid"},
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid type",
			body:     map[string]any{"ProfileType": "INVALID", "As2Id": "myid"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing type",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateProfile", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_CreateProfileReturnsProfileID verifies response schema.
func TestHandler_CreateProfileReturnsProfileID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"ProfileType": "PARTNER",
		"As2Id":       "partner-id",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["ProfileId"])
}

func TestHandler_CreateProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "local profile",
			body: map[string]any{
				"ProfileType": "LOCAL",
				"As2Id":       "AS2-local-id",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "partner profile",
			body: map[string]any{
				"ProfileType": "PARTNER",
				"As2Id":       "AS2-partner-id",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing profile type",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateProfile", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["ProfileId"])
			}
		})
	}
}

func TestHandler_DescribeProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "TESTPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": profileID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListProfiles(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListProfiles", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "DELETEPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "DeleteProfile", map[string]any{
		"ProfileId": profileID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "UPDATEPROFILE",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	profileID := createResp["ProfileId"].(string)

	rec := doTransferRequest(t, h, "UpdateProfile", map[string]any{
		"ProfileId":      profileID,
		"CertificateIds": []string{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
