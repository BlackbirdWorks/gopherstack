package eventbridge_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestSearchSchemas_ConcurrentDelete_NeverSurfacesNotFoundException reproduces
// gopherstack-uox6's SearchSchemas finding: schemasRESTSearchSchemas
// (handler_schemas_rest.go) lists schemas via Backend.SearchSchemas (which
// takes and releases its own RLock), then separately calls
// Backend.ListSchemaVersions per schema name (a second, independent
// RLock/RUnlock). If a schema is deleted in the window between those two
// calls, ListSchemaVersions returns ErrNotFound, which the shared
// writeSchemasRESTError mapper renders as wire code "NotFoundException" --
// a code SearchSchemas' own deserializeOpError switch (schemas@v1.37.4
// deserializers.go) does not declare (its declared set has no
// NotFoundException at all, unlike ListSchemaVersions itself, which does).
// Because SearchSchemas' switch has no case for it, a real client can't
// even decode it into the typed *types.NotFoundException -- it falls
// through to a bare *smithy.GenericAPIError, which is what this test
// detects (checking errors.As into the typed exception would pass
// trivially on both pre- and post-fix code, proving nothing).
//
// This drives many concurrent SearchSchemas requests against a real SDK
// client while a background goroutine repeatedly deletes and recreates one
// of the searched-for schemas, to land in that window. Pre-fix, this
// reliably produces at least one such generic NotFoundException error
// within the loop below; post-fix, the handler skips a vanished entry
// instead of surfacing the undeclared code, so it must never occur.
func TestSearchSchemas_ConcurrentDelete_NeverSurfacesNotFoundException(t *testing.T) {
	t.Parallel()

	client := newTestSchemasClient(t, newTestSchemasHandler(t))
	ctx := t.Context()

	_, err := client.CreateRegistry(ctx, &schemas.CreateRegistryInput{RegistryName: aws.String("race-reg")})
	require.NoError(t, err)

	const numSchemas = 20
	for i := range numSchemas {
		_, createErr := client.CreateSchema(ctx, &schemas.CreateSchemaInput{
			RegistryName: aws.String("race-reg"),
			SchemaName:   aws.String(schemaName(i)),
			Type:         schemastypes.TypeOpenApi3,
			Content:      aws.String(`{"openapi":"3.0.0"}`),
		})
		require.NoError(t, createErr)
	}

	const raceDuration = 500 * time.Millisecond
	deadline := time.Now().Add(raceDuration)

	var wg sync.WaitGroup

	stopCh := make(chan struct{})

	wg.Go(func() {
		for time.Now().Before(deadline) {
			_, _ = client.DeleteSchema(ctx, &schemas.DeleteSchemaInput{
				RegistryName: aws.String("race-reg"),
				SchemaName:   aws.String(schemaName(numSchemas / 2)),
			})
			_, _ = client.CreateSchema(ctx, &schemas.CreateSchemaInput{
				RegistryName: aws.String("race-reg"),
				SchemaName:   aws.String(schemaName(numSchemas / 2)),
				Type:         schemastypes.TypeOpenApi3,
				Content:      aws.String(`{"openapi":"3.0.0"}`),
			})
		}
		close(stopCh)
	})

	var mu sync.Mutex

	var sawNotFound error

	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				_, searchErr := client.SearchSchemas(ctx, &schemas.SearchSchemasInput{
					RegistryName: aws.String("race-reg"),
					Keywords:     aws.String("openapi"),
				})
				if searchErr == nil {
					continue
				}

				var genericErr *smithy.GenericAPIError
				if errors.As(searchErr, &genericErr) && genericErr.ErrorCode() == "NotFoundException" {
					mu.Lock()
					if sawNotFound == nil {
						sawNotFound = searchErr
					}
					mu.Unlock()

					return
				}
			}
		})
	}

	wg.Wait()

	require.NoError(t, sawNotFound,
		"SearchSchemas must never surface NotFoundException (a code it does not declare) "+
			"when an entry is deleted mid-request; got: %v", sawNotFound)
}

func schemaName(i int) string {
	return "schema-" + string(rune('a'+i))
}
