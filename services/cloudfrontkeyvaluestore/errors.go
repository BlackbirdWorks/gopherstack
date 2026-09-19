package cloudfrontkeyvaluestore

import (
	"errors"
	"net/http"

	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// Exception type names verified against cloudfrontkeyvaluestore@v1.15.4
// types/errors.go and deserializers.go's per-op
// awsRestjson1_deserializeOpError<Op> switches: AccessDeniedException,
// ConflictException, InternalServerException, ResourceNotFoundException,
// ServiceQuotaExceededException, ValidationException. AccessDeniedException
// is produced by the repo-wide iam.EnforcementMiddleware (cli.go wires it
// ahead of every service's handler, this one included -- see
// iam_enforcement_test.go), not by this package, so it never appears in
// classifyError below. ServiceQuotaExceededException is deserializer-only on
// PutKey/DeleteKey/UpdateKeys (not GetKey/ListKeys/DescribeKeyValueStore,
// verified per-op against deserializers.go), matching that it models the
// per-store/per-request size quotas enforced in handler.go's
// checkKeyValueSize/checkStoreSizeQuota/checkUpdateKeysBatch. Status codes
// follow the repo-wide convention for these exact exception names (see e.g.
// services/fis/handler.go, services/grafana/handler.go for
// ServiceQuotaExceededException -> 402).
const (
	exceptionConflict             = "ConflictException"
	exceptionInternalServer       = "InternalServerException"
	exceptionResourceNotFound     = "ResourceNotFoundException"
	exceptionServiceQuotaExceeded = "ServiceQuotaExceededException"
	exceptionValidation           = "ValidationException"
	// errorTypeHeader uses Go's canonical MIME header casing (matches
	// services/apigatewayv2's errTypeHeader) -- http.Header canonicalizes on
	// Set/Get regardless, so the wire bytes are identical either way, but
	// golangci-lint's canonicalheader linter wants the literal to match.
	errorTypeHeader = "X-Amzn-Errortype"
)

// Quota limits from the AWS Developer Guide's "Quotas on key value stores"
// table (docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/
// cloudfront-limits.html#limits-keyvaluestores -- not stated in the SDK's
// own doc comments, since these are runtime backend limits with no
// validators.go length constraint to verify against). Per-account store
// count (200) is a services/cloudfront control-plane concern (CreateKeyValueStore),
// out of this package's scope.
const (
	maxKVSKeyBytes          = 512
	maxKVSValueBytes        = 1024
	maxUpdateKeysBatchCount = 50
	maxUpdateKeysBatchBytes = 3 * 1024 * 1024
	maxKVSStoreBytes        = 5 * 1024 * 1024
)

// errInvalidMaxResults is returned by parseMaxResults when the MaxResults
// query parameter falls outside the real API's documented bounds.
var errInvalidMaxResults = errors.New("MaxResults must be between 1 and 50")

// errIfMatchRequired is returned when a PutKey/DeleteKey/UpdateKeys request
// carries no If-Match header. IfMatch is a required member on all three
// inputs (validators.go's validateOp{PutKey,DeleteKey,UpdateKeys}Input in
// cloudfrontkeyvaluestore@v1.15.4), so a missing header must be rejected
// rather than treated as "skip the ETag check" -- see requireIfMatch's
// callers in handler.go.
var errIfMatchRequired = errors.New("IfMatch is required")

// Quota sentinel errors -- see checkKeyValueSize/checkStoreSizeQuota/
// checkUpdateKeysBatch in handler.go for where these are raised.
var (
	errKeyTooLarge          = errors.New("key exceeds the maximum length of 512 bytes")
	errValueTooLarge        = errors.New("value exceeds the maximum length of 1024 bytes")
	errUpdateKeysTooManyOps = errors.New("UpdateKeys batch exceeds the maximum of 50 keys")
	errUpdateKeysTooLarge   = errors.New("UpdateKeys batch payload exceeds the maximum of 3 MB")
	errStoreSizeExceeded    = errors.New("key value store size would exceed the maximum of 5 MB")
)

// classifyError maps a backend error to the (status, exceptionType) pair a
// real cloudfrontkeyvaluestore server would return. The backend calls here
// are the same InMemoryBackend methods services/cloudfront's own (removed)
// data-plane handlers used, so the sentinel errors are cloudfront's.
func classifyError(err error) (int, string) {
	switch {
	case errors.Is(err, cloudfrontbackend.ErrKeyValueStoreNotFound),
		errors.Is(err, cloudfrontbackend.ErrNotFound):
		return http.StatusNotFound, exceptionResourceNotFound
	case errors.Is(err, cloudfrontbackend.ErrPreconditionFailed):
		// The real API models ETag mismatches as ConflictException (409), not
		// HTTP 412 -- the removed services/cloudfront data-plane handlers this
		// package replaces got this wrong (see PARITY.md).
		return http.StatusConflict, exceptionConflict
	case errors.Is(err, errKeyTooLarge),
		errors.Is(err, errValueTooLarge),
		errors.Is(err, errUpdateKeysTooManyOps),
		errors.Is(err, errUpdateKeysTooLarge),
		errors.Is(err, errStoreSizeExceeded):
		return http.StatusPaymentRequired, exceptionServiceQuotaExceeded
	case errors.Is(err, cloudfrontbackend.ErrValidation),
		errors.Is(err, errInvalidMaxResults),
		errors.Is(err, errIfMatchRequired):
		return http.StatusBadRequest, exceptionValidation
	default:
		return http.StatusInternalServerError, exceptionInternalServer
	}
}
