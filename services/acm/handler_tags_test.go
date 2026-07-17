package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestACMHandler_TagOps_CertExistenceValidation verifies that tag ops reject unknown ARNs.
func TestACMHandler_TagOps_CertExistenceValidation(t *testing.T) {
	t.Parallel()

	const fakeARN = "arn:aws:acm:us-east-1:000000000000:certificate/does-not-exist"

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "AddTags_NotFound",
			action: "AddTagsToCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `","Tags":[{"Key":"k","Value":"v"}]}`,
		},
		{
			name:   "ListTags_NotFound",
			action: "ListTagsForCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `"}`,
		},
		{
			name:   "RemoveTags_NotFound",
			action: "RemoveTagsFromCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `","Tags":[{"Key":"k"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
		})
	}
}

// TestACMHandler_RequestCertificate_Tags verifies that tags passed at request time are stored.
func TestACMHandler_RequestCertificate_Tags(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"tagged.example.com","Tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"infra"}]}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Verify tags are stored
	listBody, _ := json.Marshal(map[string]string{"CertificateArn": out.CertificateArn})
	listRec := postACMJSON(t, h, "ListTagsForCertificate", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tagMap := make(map[string]string)
	for _, t2 := range tagsOut.Tags {
		tagMap[t2["Key"]] = t2["Value"]
	}

	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "infra", tagMap["team"])
}
