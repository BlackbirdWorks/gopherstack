package codeartifact_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodeArtifactSDK_DescribePackageVersionNpmFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		namespace   string
		packageJSON string
		wantDisplay string
		wantSummary string
		wantHome    string
		wantRepo    string
		wantLicense string
	}{
		{name: "scoped without metadata", namespace: "vue", wantDisplay: "@vue/ui"},
		{name: "unscoped without metadata", wantDisplay: "ui"},
		{
			name:      "metadata strings",
			namespace: "vue",
			packageJSON: `{"description":"UI kit","homepage":"https://example.com",` +
				`"repository":"https://git.example.com/ui.git","license":"MIT"}`,
			wantDisplay: "@vue/ui", wantSummary: "UI kit", wantHome: "https://example.com",
			wantRepo: "https://git.example.com/ui.git", wantLicense: "MIT",
		},
		{
			name: "metadata objects",
			packageJSON: `{"repository":{"type":"git","url":"git+https://git.example.com/ui.git"},` +
				`"license":{"type":"ISC"}}`,
			wantDisplay: "ui", wantRepo: "git+https://git.example.com/ui.git", wantLicense: "ISC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "dpv-domain")
			setupRepo(t, h, "dpv-domain", "dpv-repo")
			seedVersion(t, h, "dpv-domain", "dpv-repo", "npm", tt.namespace, "ui", "1.0.0")
			if tt.packageJSON != "" {
				path := "/v1/package/version/publish?domain=dpv-domain&repository=dpv-repo&format=npm" +
					"&package=ui&version=1.0.0&asset=package.json"
				if tt.namespace != "" {
					path += "&namespace=" + tt.namespace
				}
				require.Equal(t, http.StatusOK, doRawRequest(t, h, path, []byte(tt.packageJSON)).Code)
			}

			in := &casdk.DescribePackageVersionInput{
				Domain: aws.String("dpv-domain"), Repository: aws.String("dpv-repo"), Format: "npm",
				Package: aws.String("ui"), PackageVersion: aws.String("1.0.0"),
			}
			if tt.namespace != "" {
				in.Namespace = aws.String(tt.namespace)
			}
			out, err := newTestCodeArtifactClient(t, h).DescribePackageVersion(t.Context(), in)
			require.NoError(t, err)
			pv := out.PackageVersion
			assert.Equal(t, tt.wantDisplay, aws.ToString(pv.DisplayName))
			assert.Equal(t, tt.wantSummary, aws.ToString(pv.Summary))
			assert.Equal(t, tt.wantHome, aws.ToString(pv.HomePage))
			assert.Equal(t, tt.wantRepo, aws.ToString(pv.SourceCodeRepository))
			if tt.wantLicense == "" {
				assert.Empty(t, pv.Licenses)

				return
			}
			require.Len(t, pv.Licenses, 1)
			assert.Equal(t, tt.wantLicense, aws.ToString(pv.Licenses[0].Name))
		})
	}
}

func TestCodeArtifactSDK_GetRepositoryEndpointType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		epType  types.EndpointType
		wantErr bool
	}{
		{name: "default"},
		{name: "ipv4", epType: types.EndpointTypeIpv4},
		{name: "dualstack", epType: types.EndpointTypeDualstack},
		{name: "unknown", epType: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "ep-domain")
			setupRepo(t, h, "ep-domain", "ep-repo")
			client := newTestCodeArtifactClient(t, h)
			out, err := client.GetRepositoryEndpoint(t.Context(), &casdk.GetRepositoryEndpointInput{
				Domain: aws.String("ep-domain"), Repository: aws.String("ep-repo"),
				Format: "npm", EndpointType: tt.epType,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.RepositoryEndpoint), "ep-domain")
		})
	}
}
