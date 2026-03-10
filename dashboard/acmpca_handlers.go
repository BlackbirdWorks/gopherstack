package dashboard

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
)

// acmpcaCAView is the view model for a single ACM PCA Certificate Authority.
type acmpcaCAView struct {
	ARN       string
	ARNShort  string
	Type      string
	Status    string
	CreatedAt string
}

// acmpcaIndexData is the template data for the ACM PCA index page.
type acmpcaIndexData struct {
	PageData

	CertificateAuthorities []acmpcaCAView
}

// acmpcaSnippet returns the code snippet data for ACM PCA operations.
func (h *DashboardHandler) acmpcaSnippet() *SnippetData {
	return &SnippetData{
		ID:    "acmpca-operations",
		Title: "Using ACM PCA",
		Cli:   `aws acm-pca help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for ACM PCA
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := acmpca.NewFromConfig(cfg, func(o *acmpca.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

out, err := client.CreateCertificateAuthority(context.TODO(), &acmpca.CreateCertificateAuthorityInput{
    CertificateAuthorityType: types.CertificateAuthorityTypeRoot,
    CertificateAuthorityConfiguration: &types.CertificateAuthorityConfiguration{
        KeyAlgorithm:     types.KeyAlgorithmEcPrime256v1,
        SigningAlgorithm: types.SigningAlgorithmSha256withecdsa,
        Subject:          &types.ASN1Subject{CommonName: aws.String("My Root CA")},
    },
})`,
		Python: `# Initialize boto3 client for ACM PCA
import boto3

client = boto3.client('acm-pca', endpoint_url='http://localhost:8000')

response = client.create_certificate_authority(
    CertificateAuthorityConfiguration={
        'KeyAlgorithm': 'EC_prime256v1',
        'SigningAlgorithm': 'SHA256WITHECDSA',
        'Subject': {'CommonName': 'My Root CA'},
    },
    CertificateAuthorityType='ROOT',
)`,
	}
}

// acmpcaIndex renders the list of all ACM PCA Certificate Authorities.
func (h *DashboardHandler) acmpcaIndex(c *echo.Context) error {
	w := c.Response()

	if h.ACMPCAOps == nil {
		h.renderTemplate(w, "acmpca/index.html", acmpcaIndexData{
			PageData: PageData{
				Title:     "ACM PCA Certificate Authorities",
				ActiveTab: "acmpca",
				Snippet:   h.acmpcaSnippet(),
			},
			CertificateAuthorities: []acmpcaCAView{},
		})

		return nil
	}

	cas := h.ACMPCAOps.Backend.ListCertificateAuthorities("", 0).Data
	views := make([]acmpcaCAView, 0, len(cas))

	for _, ca := range cas {
		arnShort := ca.ARN
		if parts := strings.Split(ca.ARN, "/"); len(parts) > 1 {
			arnShort = fmt.Sprintf(".../%s", parts[len(parts)-1])
		}

		views = append(views, acmpcaCAView{
			ARN:       ca.ARN,
			ARNShort:  arnShort,
			Type:      ca.Type,
			Status:    ca.Status,
			CreatedAt: ca.CreatedAt.Format(time.RFC3339),
		})
	}

	h.renderTemplate(w, "acmpca/index.html", acmpcaIndexData{
		PageData: PageData{
			Title:     "ACM PCA Certificate Authorities",
			ActiveTab: "acmpca",
			Snippet:   h.acmpcaSnippet(),
		},
		CertificateAuthorities: views,
	})

	return nil
}

// acmpcaCreateCA handles POST /dashboard/acmpca/create.
func (h *DashboardHandler) acmpcaCreateCA(c *echo.Context) error {
	if h.ACMPCAOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	commonName := c.Request().FormValue("common_name")
	if commonName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	caType := c.Request().FormValue("ca_type")
	if caType == "" {
		caType = "ROOT"
	}

	if _, err := h.ACMPCAOps.Backend.CreateCertificateAuthority(
		caType,
		acmpcabackend.CertificateAuthorityConfiguration{
			Subject: acmpcabackend.CertificateAuthoritySubject{CommonName: commonName},
		},
	); err != nil {
		h.Logger.Error("failed to create ACM PCA certificate authority", "common_name", commonName, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/acmpca")
}

// acmpcaDeleteCA handles POST /dashboard/acmpca/delete.
func (h *DashboardHandler) acmpcaDeleteCA(c *echo.Context) error {
	if h.ACMPCAOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	caARN := c.Request().FormValue("arn")
	if caARN == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ACMPCAOps.Backend.DeleteCertificateAuthority(caARN); err != nil {
		h.Logger.Error("failed to delete ACM PCA certificate authority", "arn", caARN, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/acmpca")
}
