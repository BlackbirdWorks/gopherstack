package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
	"github.com/stretchr/testify/require"
)

// assertSchemasBadRequestNotNotFound asserts errors.As unwraps to the real
// *types.BadRequestException from the schemas SDK deserializer, not
// *types.NotFoundException or *types.ConflictException.
func assertSchemasBadRequestNotNotFound(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)

	var notFound *schemastypes.NotFoundException
	require.NotErrorAs(t, err, &notFound, "op does not declare NotFoundException; got: %v", err)

	var conflict *schemastypes.ConflictException
	require.NotErrorAs(t, err, &conflict, "op does not declare ConflictException; got: %v", err)

	var badReq *schemastypes.BadRequestException
	require.ErrorAs(t, err, &badReq,
		"expected a real BadRequestException from the SDK deserializer, got: %v", err)
}

// TestCreateSchema_RegistryNotFound_RealClient drives CreateSchema against a
// nonexistent registry through the real aws-sdk-go-v2 schemas client.
// CreateSchema's own deserializeOpError switch (schemas@v1.37.4
// deserializers.go) declares only [BadRequestException, ForbiddenException,
// InternalServerErrorException, ServiceUnavailableException] -- no
// NotFoundException at all, unlike most of its sibling ops
// (Delete/Describe/Update*) which do declare it (gopherstack-uox6 sweep).
func TestCreateSchema_RegistryNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSchemasClient(t, newTestSchemasHandler(t))

	_, err := client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
		RegistryName: aws.String("does-not-exist"),
		SchemaName:   aws.String("my-schema"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(`{"openapi":"3.0.0"}`),
	}, func(o *schemas.Options) { o.RetryMaxAttempts = 1 })
	assertSchemasBadRequestNotNotFound(t, err)
}

// TestCreateSchema_DuplicateName_RealClient drives CreateSchema against an
// already-existing schema name through the real aws-sdk-go-v2 schemas
// client. CreateSchema's own switch declares no ConflictException either
// (see the not-found sibling test above for the full declared set)
// (gopherstack-uox6 sweep).
func TestCreateSchema_DuplicateName_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSchemasClient(t, newTestSchemasHandler(t))

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{
		RegistryName: aws.String("reg1"),
	})
	require.NoError(t, err)

	input := &schemas.CreateSchemaInput{
		RegistryName: aws.String("reg1"),
		SchemaName:   aws.String("dup-schema"),
		Type:         schemastypes.TypeOpenApi3,
		Content:      aws.String(`{"openapi":"3.0.0"}`),
	}
	_, err = client.CreateSchema(t.Context(), input)
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), input, func(o *schemas.Options) { o.RetryMaxAttempts = 1 })
	assertSchemasBadRequestNotNotFound(t, err)
}

// TestListSchemas_RegistryNotFound_RealClient drives ListSchemas against a
// nonexistent registry through the real aws-sdk-go-v2 schemas client.
// ListSchemas' own switch declares [BadRequestException,
// ForbiddenException, InternalServerErrorException,
// ServiceUnavailableException, UnauthorizedException] -- no
// NotFoundException (gopherstack-uox6 sweep).
func TestListSchemas_RegistryNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSchemasClient(t, newTestSchemasHandler(t))

	_, err := client.ListSchemas(t.Context(), &schemas.ListSchemasInput{
		RegistryName: aws.String("does-not-exist"),
	}, func(o *schemas.Options) { o.RetryMaxAttempts = 1 })
	assertSchemasBadRequestNotNotFound(t, err)
}

// TestSearchSchemas_RegistryNotFound_RealClient drives SearchSchemas against
// a nonexistent registry through the real aws-sdk-go-v2 schemas client.
// SearchSchemas' own switch declares the same set as ListSchemas -- no
// NotFoundException (gopherstack-uox6 sweep).
func TestSearchSchemas_RegistryNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSchemasClient(t, newTestSchemasHandler(t))

	_, err := client.SearchSchemas(t.Context(), &schemas.SearchSchemasInput{
		RegistryName: aws.String("does-not-exist"),
		Keywords:     aws.String("foo"),
	}, func(o *schemas.Options) { o.RetryMaxAttempts = 1 })
	assertSchemasBadRequestNotNotFound(t, err)
}
