package elasticsearch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestElasticsearchHandler_ExportCountHelpers verifies DomainCount,
// PackageCount, InboundConnectionCount, OutboundConnectionCount, and
// VpcEndpointCount helpers work correctly.
func TestElasticsearchHandler_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Equal(t, 0, b.DomainCount())
	assert.Equal(t, 0, b.PackageCount())
	assert.Equal(t, 0, b.InboundConnectionCount())
	assert.Equal(t, 0, b.OutboundConnectionCount())
	assert.Equal(t, 0, b.VpcEndpointCount())

	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "cnt-domain"},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.DomainCount())

	_, err = b.CreatePackage(context.Background(), "my-pkg", "TXT-DICTIONARY", "desc",
		elasticsearch.PackageSource{S3BucketName: "b", S3Key: "k"})
	require.NoError(t, err)
	assert.Equal(t, 1, b.PackageCount())

	_, err = b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/cnt-domain", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, b.VpcEndpointCount())

	conn := elasticsearch.InboundConnection{
		ConnectionID:     "conn-001",
		ConnectionStatus: "PENDING_ACCEPTANCE",
	}
	b.AddInboundConnectionInternal(context.Background(), conn)
	assert.Equal(t, 1, b.InboundConnectionCount())

	_, err = b.CreateOutboundCrossClusterSearchConnection(
		context.Background(),
		elasticsearch.CrossClusterDomainInfo{DomainName: "local"},
		elasticsearch.CrossClusterDomainInfo{DomainName: "remote"},
		"my-alias",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.OutboundConnectionCount())
}

// TestElasticsearchHandler_ResetClearsAllMaps verifies that Reset clears all
// internal maps.
func TestElasticsearchHandler_ResetClearsAllMaps(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "reset-dom"},
	)
	require.NoError(t, err)

	_, err = b.CreatePackage(context.Background(), "pkg1", "TXT-DICTIONARY", "",
		elasticsearch.PackageSource{S3BucketName: "b", S3Key: "k"})
	require.NoError(t, err)

	_, err = b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/reset-dom", nil)
	require.NoError(t, err)

	b.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{ConnectionID: "c1"})

	h := elasticsearch.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, b.DomainCount())
	assert.Equal(t, 0, b.PackageCount())
	assert.Equal(t, 0, b.VpcEndpointCount())
	assert.Equal(t, 0, b.InboundConnectionCount())
}

// TestElasticsearchHandler_HandlerResetDelegatesToBackend verifies
// handler.Reset() clears backend state.
func TestElasticsearchHandler_HandlerResetDelegatesToBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "del-domain"},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.DomainCount())

	h := elasticsearch.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, b.DomainCount())
}
