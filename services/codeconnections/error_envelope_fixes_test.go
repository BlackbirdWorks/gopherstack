package codeconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestEmptyHostArn_NotFoundNotInvalidInput_RealClient proves that GetHost and
// UpdateHost answer an empty-but-present HostArn with the declared
// ResourceNotFoundException, not InvalidInputException (gopherstack-uox6
// error-envelope sweep, errtargetaudit).
//
// A real client's own client-side validators (codeconnections@v1.13.4
// validators.go) only reject a NIL HostArn pointer before the request ever
// leaves the process -- they do not check for an empty string, so
// aws.String("") reaches this service exactly as sent. Both ops previously
// answered with ErrValidation -> InvalidInputException, a code absent from
// GetHost's and UpdateHost's own deserializeOpError<Op> switches
// (deserializers.go); both declare ResourceNotFoundException and the
// backend's existing lookup-miss path (b.hosts.Get("")) already answers
// that correctly once the invented pre-check is removed.
func TestEmptyHostArn_NotFoundNotInvalidInput_RealClient(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *codeconnectionssdk.Client {
		t.Helper()

		backend := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")

		return newTestCodeConnectionsClient(t, codeconnections.NewHandler(backend))
	}

	assertNotFound := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var apiErr *codeconnectionstypes.ResourceNotFoundException
		require.ErrorAs(t, err, &apiErr,
			"expected a real ResourceNotFoundException from the SDK deserializer, got: %v", err)
	}

	t.Run("get host", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.GetHost(t.Context(), &codeconnectionssdk.GetHostInput{
			HostArn: aws.String(""),
		})
		assertNotFound(t, err)
	})

	t.Run("update host", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.UpdateHost(t.Context(), &codeconnectionssdk.UpdateHostInput{
			HostArn: aws.String(""),
		})
		assertNotFound(t, err)
	})
}
