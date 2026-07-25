package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomain_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-pool")

	createRec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-test-domain",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	descRec := doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "my-test-domain",
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	delRec := doCognitoRequest(t, h, "DeleteUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-test-domain",
	})
	require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())
}

func TestCreateUserPoolDomain_ManagedDomain_EmptyCloudFrontDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		certArn string
		wantCF  bool
	}{
		{
			name:    "managed_domain_no_cert",
			certArn: "",
			wantCF:  false,
		},
		{
			name:    "custom_domain_with_cert",
			certArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc123",
			wantCF:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "domain-audit-"+tt.name+"-pool")

			body := map[string]any{
				"UserPoolId": poolID,
				"Domain":     "audit-domain-" + tt.name,
			}
			if tt.certArn != "" {
				body["CustomDomainConfig"] = map[string]any{
					"CertificateArn": tt.certArn,
				}
			}

			rec := doCognitoRequest(t, h, "CreateUserPoolDomain", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tt.wantCF {
				assert.Contains(
					t,
					out.CloudFrontDomain,
					"cloudfront.net",
					"custom domain must return CloudFront domain",
				)
			} else {
				assert.Empty(
					t,
					out.CloudFrontDomain,
					"managed domain must return empty CloudFrontDomain in create response",
				)
			}
		})
	}
}

func TestUserPoolDomain_Managed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-managed-pool")

	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp-managed",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	// Managed domains: AWS returns empty CloudFrontDomain (no CloudFront distribution).
	assert.Empty(t, out.CloudFrontDomain)
}

func TestUserPoolDomain_Custom_WithCertArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-custom-pool")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/abc123"

	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": certArn,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.Contains(t, createOut.CloudFrontDomain, "cloudfront.net")

	// Describe should return the domain with status.
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "auth.mycompany.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		DomainDescription *struct {
			CustomDomainConfig *struct {
				CertificateArn string `json:"CertificateArn,omitempty"`
			} `json:"CustomDomainConfig,omitempty"`
			Domain                 string `json:"Domain,omitempty"`
			UserPoolID             string `json:"UserPoolId,omitempty"`
			Status                 string `json:"Status,omitempty"`
			CloudFrontDistribution string `json:"CloudFrontDistribution,omitempty"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.DomainDescription)
	assert.Equal(t, "auth.mycompany.com", descOut.DomainDescription.Domain)
	assert.Equal(t, "ACTIVE", descOut.DomainDescription.Status)

	// AWS echoes CustomDomainConfig.CertificateArn back for a custom domain -- e.g. the
	// Terraform AWS provider reads this to detect drift on the configured certificate.
	require.NotNil(t, descOut.DomainDescription.CustomDomainConfig, "custom domain must echo CustomDomainConfig")
	assert.Equal(t, certArn, descOut.DomainDescription.CustomDomainConfig.CertificateArn)
}

// TestUserPoolDomain_Managed_NoCustomDomainConfig proves a managed (non-custom, no ACM
// certificate) domain's DescribeUserPoolDomain omits CustomDomainConfig entirely,
// matching AWS -- only domains with a certificate get one.
func TestUserPoolDomain_Managed_NoCustomDomainConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-managed-no-cdc-pool")

	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp-managed-no-cdc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "myapp-managed-no-cdc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		DomainDescription *struct {
			CustomDomainConfig *struct {
				CertificateArn string `json:"CertificateArn,omitempty"`
			} `json:"CustomDomainConfig,omitempty"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.DomainDescription)
	assert.Nil(t, descOut.DomainDescription.CustomDomainConfig)
}

func TestUserPoolDomain_Update_WithCertArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-update-pool")

	// Create managed domain first.
	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth-update.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": "arn:aws:acm:us-east-1:123:certificate/old",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with new cert.
	rec = doCognitoRequest(t, h, "UpdateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "auth-update.mycompany.com",
		"CustomDomainConfig": map[string]any{
			"CertificateArn": "arn:aws:acm:us-east-1:123:certificate/new",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out.CloudFrontDomain, "cloudfront.net")
}

func TestUserPoolDomain_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("domain-direct-pool")
	require.NoError(t, err)

	// Managed domain (no cert).
	d, err := b.CreateUserPoolDomainFull(pool.ID, "my-managed-domain", "")
	require.NoError(t, err)
	assert.Contains(t, d.CloudFrontDistribution, "amazoncognito.com")
	assert.Empty(t, d.CertificateArn)

	// Custom domain with cert.
	certArn := "arn:aws:acm:us-east-1:123:certificate/xyz"
	d2, err := b.CreateUserPoolDomainFull(pool.ID, "auth.example.com", certArn)
	require.NoError(t, err)
	assert.Contains(t, d2.CloudFrontDistribution, "cloudfront.net")
	assert.Equal(t, certArn, d2.CertificateArn)

	// Update cert.
	newCert := "arn:aws:acm:us-east-1:123:certificate/new"
	cfDomain, err := b.UpdateUserPoolDomainFull(pool.ID, "auth.example.com", newCert)
	require.NoError(t, err)
	assert.Contains(t, cfDomain, "cloudfront.net")

	// Delete.
	require.NoError(t, b.DeleteUserPoolDomain(pool.ID, "auth.example.com"))
	d3 := b.FindUserPoolDomain("auth.example.com")
	assert.Nil(t, d3)
}

func TestDomain_FullCRUDLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "domain-crud-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	// Managed domains: AWS returns empty CloudFrontDomain in the create response.
	assert.Empty(t, createResp.CloudFrontDomain)

	// Describe
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		DomainDescription struct {
			Domain string `json:"Domain,omitempty"`
			Status string `json:"Status,omitempty"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "myapp", descResp.DomainDescription.Domain)
	assert.Equal(t, "ACTIVE", descResp.DomainDescription.Status)

	// Update
	rec = doCognitoRequest(t, h, "UpdateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — returns empty description (not an error per AWS behaviour)
	rec = doCognitoRequest(t, h, "DescribeUserPoolDomain", map[string]any{
		"Domain": "myapp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterDeleteResp struct {
		DomainDescription struct {
			Domain string `json:"Domain,omitempty"`
			Status string `json:"Status,omitempty"`
		} `json:"DomainDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterDeleteResp))
	assert.Empty(t, afterDeleteResp.DomainDescription.Domain)
}
