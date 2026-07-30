package acm_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/acm"
)

const wireTestRegion = "us-east-1"

// newTestACMClient stands up the real aws-sdk-go-v2 ACM client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through
// the genuine SDK serializer/deserializer -- rather than decoding the raw
// JSON body with ad-hoc structs, as most other tests in this package do --
// is what actually proves a response is wire-compatible, matching the
// pattern services/codedeploy's handler_sdk_roundtrip_test.go uses.
func newTestACMClient(t *testing.T, h *acm.Handler) *acmsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(wireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return acmsdk.NewFromConfig(cfg, func(o *acmsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_AcmeEndpoint_CreateDescribe proves CreateAcmeEndpoint/
// DescribeAcmeEndpoint's CertificateAuthority union
// ({"PublicCertificateAuthority":{"AllowedKeyAlgorithms":[...]}}) and
// epoch-seconds CreatedAt/UpdatedAt decode correctly through the real SDK
// deserializer -- a hand-rolled union wrapper key or a UnixMilli-scaled
// timestamp would fail this round trip even though raw-JSON assertions in
// handler_test.go's TestACMHandler_AcmeEndpoints would not catch it.
func Test_SDKRoundTrip_AcmeEndpoint_CreateDescribe(t *testing.T) {
	t.Parallel()

	backend := acm.NewInMemoryBackend("000000000000", wireTestRegion)
	h := acm.NewHandler(backend)
	client := newTestACMClient(t, h)

	created, err := client.CreateAcmeEndpoint(t.Context(), &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{
				AllowedKeyAlgorithms: []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmRsa2048},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.AcmeEndpointArn)
	assert.Contains(t, *created.AcmeEndpointArn, "acme-endpoint/")

	described, err := client.DescribeAcmeEndpoint(t.Context(), &acmsdk.DescribeAcmeEndpointInput{
		AcmeEndpointArn: created.AcmeEndpointArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.AcmeEndpoint)
	assert.Equal(t, types.AcmeEndpointStatusActive, described.AcmeEndpoint.Status)
	assert.Equal(t, types.AcmeAuthorizationBehaviorPreApproved, described.AcmeEndpoint.AuthorizationBehavior)
	require.NotNil(t, described.AcmeEndpoint.CreatedAt)
	assert.NotZero(t, *described.AcmeEndpoint.CreatedAt)

	ca, ok := described.AcmeEndpoint.CertificateAuthority.(*types.CertificateAuthorityMemberPublicCertificateAuthority)
	require.True(t, ok, "CertificateAuthority must round-trip as PublicCertificateAuthority")
	assert.Equal(t, []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmRsa2048}, ca.Value.AllowedKeyAlgorithms)
}

// Test_SDKRoundTrip_TagResource_AcmeEndpoint proves the generic TagResource/
// ListTagsForResource pair round-trips through the real SDK client for a
// non-certificate resource ARN.
func Test_SDKRoundTrip_TagResource_AcmeEndpoint(t *testing.T) {
	t.Parallel()

	backend := acm.NewInMemoryBackend("000000000000", wireTestRegion)
	h := acm.NewHandler(backend)
	client := newTestACMClient(t, h)

	created, err := client.CreateAcmeEndpoint(t.Context(), &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{},
		},
	})
	require.NoError(t, err)

	_, err = client.TagResource(t.Context(), &acmsdk.TagResourceInput{
		ResourceArn: created.AcmeEndpointArn,
		Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(t.Context(), &acmsdk.ListTagsForResourceInput{
		ResourceArn: created.AcmeEndpointArn,
	})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)
	assert.Equal(t, "team", *tags.Tags[0].Key)
	assert.Equal(t, "platform", *tags.Tags[0].Value)
}

// Test_SDKRoundTrip_SearchCertificates proves the recursive
// CertificateFilterStatement union and CertificateSearchResult response
// (CertificateMetadata.AcmCertificateMetadata, X509Attributes) decode
// correctly through the real SDK deserializer.
func Test_SDKRoundTrip_SearchCertificates(t *testing.T) {
	t.Parallel()

	backend := acm.NewInMemoryBackend("000000000000", wireTestRegion)
	h := acm.NewHandler(backend)
	client := newTestACMClient(t, h)

	_, err := client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
		DomainName: aws.String("sdk-search.example.com"),
	})
	require.NoError(t, err)

	out, err := client.SearchCertificates(t.Context(), &acmsdk.SearchCertificatesInput{
		FilterStatement: &types.CertificateFilterStatementMemberFilter{
			Value: &types.CertificateFilterMemberAcmCertificateMetadataFilter{
				Value: &types.AcmCertificateMetadataFilterMemberStatus{
					Value: types.CertificateStatusIssued,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Results, 1)

	result := out.Results[0]
	require.NotNil(t, result.CertificateArn)
	assert.Contains(t, *result.CertificateArn, "certificate/")

	meta, ok := result.CertificateMetadata.(*types.CertificateMetadataMemberAcmCertificateMetadata)
	require.True(t, ok)
	assert.Equal(t, types.CertificateStatusIssued, meta.Value.Status)
	require.NotNil(t, result.X509Attributes)
	assert.NotZero(t, result.X509Attributes.NotAfter)
}

// TestACMHandler_ListCertificates_SummaryHasCreatedAtAndInUse locks in the
// fix for CertificateSummary previously omitting CreatedAt entirely (a field
// always present on the real AWS wire) and never surfacing InUse.
func TestACMHandler_ListCertificates_SummaryHasCreatedAtAndInUse(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"summaryfields.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			CreatedAt *int64 `json:"CreatedAt"`
			InUse     *bool  `json:"InUse"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)

	summary := out.CertificateSummaryList[0]
	require.NotNil(t, summary.CreatedAt, "CertificateSummary.CreatedAt must always be present on the wire")
	assert.Positive(t, *summary.CreatedAt)
	require.NotNil(t, summary.InUse)
	assert.False(t, *summary.InUse)
}

// TestACMHandler_ListCertificates_SummaryKeyUsages verifies that
// CertificateSummary.KeyUsages/ExtendedKeyUsages project the same key-usage
// data DescribeCertificate exposes, not just DescribeCertificate. Unlike
// CertificateDetail.KeyUsages ([]types.KeyUsage, a slice of {"Name": "..."}
// objects), real AWS's CertificateSummary.KeyUsages/ExtendedKeyUsages
// (returned by ListCertificates) are plain string arrays
// ([]types.KeyUsageName/[]types.ExtendedKeyUsageName) -- see
// aws-sdk-go-v2/service/acm/types.CertificateSummary. Every real SDK
// client's ListCertificates deserializer rejects the object-wrapped shape
// with "expected KeyUsageName to be of type string, got map[string]interface{}
// instead", caught by TestTerraform_ACM.
func TestACMHandler_ListCertificates_SummaryKeyUsages(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"summaryusage.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			KeyUsages         []string `json:"KeyUsages"`
			ExtendedKeyUsages []string `json:"ExtendedKeyUsages"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)

	summary := out.CertificateSummaryList[0]
	require.NotEmpty(t, summary.KeyUsages)
	assert.Equal(t, "DIGITAL_SIGNATURE", summary.KeyUsages[0])
	require.NotEmpty(t, summary.ExtendedKeyUsages)
	assert.Equal(t, "TLS_WEB_SERVER_AUTHENTICATION", summary.ExtendedKeyUsages[0])
}

// TestACMHandler_RenewCertificate_RenewalSummaryHasUpdatedAt locks in the fix
// for RenewalSummary previously omitting UpdatedAt, a required (always
// present) field on the real AWS wire.
func TestACMHandler_RenewCertificate_RenewalSummaryHasUpdatedAt(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"renewalupdated.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	renewBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	renewRec := postACMJSON(t, h, "RenewCertificate", string(renewBody))
	require.Equal(t, http.StatusOK, renewRec.Code)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(renewBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	type renewalSummary struct {
		RenewalStatus string `json:"RenewalStatus"`
		UpdatedAt     int64  `json:"UpdatedAt"`
	}

	var descOut struct {
		Certificate struct {
			RenewalSummary renewalSummary `json:"RenewalSummary"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut.Certificate.RenewalSummary.RenewalStatus)
	assert.Positive(t, descOut.Certificate.RenewalSummary.UpdatedAt,
		"RenewalSummary.UpdatedAt must always be present on the wire")
}

// TestACMHandler_RequestCertificate_ExportOption_RoundTrips verifies that
// Options.Export supplied on RequestCertificate is stored and echoed back on
// DescribeCertificate.Options.Export.
func TestACMHandler_RequestCertificate_ExportOption_RoundTrips(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"exportopt.example.com","Options":{"Export":"ENABLED"}}`
	reqRec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Options struct {
				Export string `json:"Export"`
			} `json:"Options"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "ENABLED", descOut.Certificate.Options.Export)
}

// TestACMHandler_RequestCertificate_ManagedBy locks in the fix for
// RequestCertificate.ManagedBy (real AWS's CertificateManagedBy enum, whose
// single defined value is "CLOUDFRONT" -- aws-sdk-go-v2/service/acm/types.
// CertificateManagedByCloudfront) being previously accepted nowhere: the
// field was parsed nowhere, so a caller-supplied ManagedBy never reached
// DescribeCertificate.ManagedBy or CertificateSummary.ManagedBy, and an
// unrecognized value was silently accepted rather than rejected.
func TestACMHandler_RequestCertificate_ManagedBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		domain        string
		managedBy     string
		wantManagedBy string
		wantStatus    int
	}{
		{
			name:          "cloudfront_accepted_and_echoed",
			domain:        "managedby-cloudfront.example.com",
			managedBy:     "CLOUDFRONT",
			wantManagedBy: "CLOUDFRONT",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "absent_stays_absent",
			domain:        "managedby-absent.example.com",
			managedBy:     "",
			wantManagedBy: "",
			wantStatus:    http.StatusOK,
		},
		{
			name:       "unknown_value_rejected",
			domain:     "managedby-invalid.example.com",
			managedBy:  "LAMBDA",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()

			body, err := json.Marshal(map[string]string{
				"DomainName": tt.domain,
				"ManagedBy":  tt.managedBy,
			})
			require.NoError(t, err)

			reqRec := postACMJSON(t, h, "RequestCertificate", string(body))
			require.Equal(t, tt.wantStatus, reqRec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			descBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
			require.NoError(t, err)

			descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				Certificate struct {
					ManagedBy string `json:"ManagedBy"`
				} `json:"Certificate"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			assert.Equal(t, tt.wantManagedBy, descOut.Certificate.ManagedBy)

			listRec := postACMJSON(t, h, "ListCertificates", `{}`)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				CertificateSummaryList []struct {
					ManagedBy string `json:"ManagedBy"`
				} `json:"CertificateSummaryList"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
			require.Len(t, listOut.CertificateSummaryList, 1)
			assert.Equal(t, tt.wantManagedBy, listOut.CertificateSummaryList[0].ManagedBy)
		})
	}
}
