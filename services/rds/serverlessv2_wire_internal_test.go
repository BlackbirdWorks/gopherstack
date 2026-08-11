package rds

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// TestServerlessV2PlatformVersionsWireShape decodes gopherstack's actual
// describeServerlessV2PlatformVersionsResponse marshaling through the real
// aws-sdk-go-v2 RDS client. staticServerlessV2PlatformVersions is always
// empty (see its doc comment), so this bypasses the backend and marshals the
// response struct directly -- it is the only way to exercise a populated
// list for this operation, and it still validates the production XML tags,
// not a copy of them.
func TestServerlessV2PlatformVersionsWireShape(t *testing.T) {
	t.Parallel()

	resp := &describeServerlessV2PlatformVersionsResponse{
		Xmlns: rdsXMLNS,
		Versions: xmlServerlessV2VersionList{
			Members: []xmlServerlessV2PlatformVersionInfo{
				{Engine: engineAuroraPostgresql, ServerlessV2PlatformVersion: "1", Status: "ACTIVE", IsDefault: true},
			},
		},
	}

	xmlBytes, err := marshalXML(resp)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(xmlBytes)
	}))
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := rdssdk.NewFromConfig(cfg, func(o *rdssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	out, err := client.DescribeServerlessV2PlatformVersions(
		t.Context(), &rdssdk.DescribeServerlessV2PlatformVersionsInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.ServerlessV2PlatformVersions, 1)
}
