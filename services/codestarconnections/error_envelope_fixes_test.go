package codestarconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// These tests prove that an empty-but-present required ARN reaches the
// declared not-found error through the real SDK client, not
// InvalidInputException (gopherstack-6flj/uox6 error-envelope sweep).
//
// A real client's own client-side validators (codestarconnections@v1.38.4
// validators.go) only reject a NIL required-ARN pointer before the request
// ever leaves the process -- they do not check for an empty string, so
// aws.String("") for e.g. GetConnectionInput.ConnectionArn reaches this
// service exactly as sent. Every op below previously answered with
// errInvalidRequest -> InvalidInputException, a code absent from that op's
// own deserializeOpError<Op> switch (deserializers.go); each op's switch
// declares ResourceNotFoundException and nothing else fits an ARN that
// resolves to no resource, so the fix removes the invented pre-check and
// lets the existing lookup-miss path answer, which was already correct.
func TestEmptyRequiredARN_NotFoundNotInvalidInput_RealClient(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *codestarconnectionssdk.Client {
		t.Helper()

		backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

		return newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))
	}

	assertNotFound := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var apiErr *codestarconnectionstypes.ResourceNotFoundException
		require.ErrorAs(t, err, &apiErr,
			"expected a real ResourceNotFoundException from the SDK deserializer, got: %v", err)
	}

	t.Run("get connection", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.GetConnection(t.Context(), &codestarconnectionssdk.GetConnectionInput{
			ConnectionArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("delete connection", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteConnection(t.Context(), &codestarconnectionssdk.DeleteConnectionInput{
			ConnectionArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("get host", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.GetHost(t.Context(), &codestarconnectionssdk.GetHostInput{
			HostArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("delete host", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteHost(t.Context(), &codestarconnectionssdk.DeleteHostInput{
			HostArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("list tags for resource", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.ListTagsForResource(t.Context(), &codestarconnectionssdk.ListTagsForResourceInput{
			ResourceArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("tag resource", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.TagResource(t.Context(), &codestarconnectionssdk.TagResourceInput{
			ResourceArn: aws.String(""),
			Tags:        []codestarconnectionstypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
		})
		assertNotFound(t, err)
	})

	t.Run("untag resource", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.UntagResource(t.Context(), &codestarconnectionssdk.UntagResourceInput{
			ResourceArn: aws.String(""),
			TagKeys:     []string{"k"},
		})
		assertNotFound(t, err)
	})

	t.Run("update host", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.UpdateHost(t.Context(), &codestarconnectionssdk.UpdateHostInput{
			HostArn: aws.String(""),
		})
		assertNotFound(t, err)
	})
}

// TestUpdateHost_ProviderEndpointTooLong_NoDeclaredType documents a refusal:
// UpdateHost's own error switch declares ConflictException,
// ResourceNotFoundException, ResourceUnavailableException,
// UnsupportedOperationException -- no InvalidInputException, and no
// ValidationException equivalent exists anywhere in this SDK module. A
// too-long ProviderEndpoint therefore has no correct declared type to send;
// this test only pins the pre-existing (wrong-but-unimprovable) status quo
// so a future change to it is deliberate, not accidental.
func TestUpdateHost_ProviderEndpointTooLong_NoDeclaredType(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))

	out, err := client.CreateHost(t.Context(), &codestarconnectionssdk.CreateHostInput{
		Name:             aws.String("refusal-host"),
		ProviderType:     "GitHub",
		ProviderEndpoint: aws.String("https://example.com"),
	})
	require.NoError(t, err)

	longEndpoint := "https://" + string(make([]byte, 600)) + ".example.com"

	_, err = client.UpdateHost(t.Context(), &codestarconnectionssdk.UpdateHostInput{
		HostArn:          out.HostArn,
		ProviderEndpoint: aws.String(longEndpoint),
	})
	require.Error(t, err)

	var apiErr *codestarconnectionstypes.ResourceNotFoundException
	require.NotErrorAs(t, err, &apiErr, "must not be the not-found type")
}
