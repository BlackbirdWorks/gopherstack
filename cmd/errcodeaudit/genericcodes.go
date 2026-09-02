package main

// genericProtocolCodes are error codes AWS's wire protocols (JSON-RPC,
// Query, REST) recognize at the frontend/gateway layer for every service,
// never modeled as a per-service typed exception -- so a service's own
// types/errors.go and deserializers.go legitimately contain none of them,
// and flagging their absence there would be a false positive by
// construction. Sources: the six named directly in this tool's brief
// (ValidationError, InvalidAction, MissingParameter, Throttling,
// InternalFailure, AccessDenied) plus their common Query/JSON-RPC siblings
// confirmed by the same "gateway rejects the request before any
// operation-specific handler runs" reasoning -- credential/signature
// checking (SignatureDoesNotMatch, InvalidClientTokenId, ExpiredToken,
// RequestExpired, IncompleteSignature, MissingAuthenticationToken,
// UnrecognizedClientException), request-shape checking (InvalidParameterValue,
// InvalidParameterCombination, InvalidQueryParameter, MissingRequiredParameter),
// generic client/server fallbacks the Smithy/JSON-RPC runtime itself emits
// (InternalError, InternalServerError, ServiceUnavailable,
// ServiceUnavailableException, ServerException, ServiceException,
// UnknownOperationException -- confirmed live: ecs/handler.go's own
// errUnknownAction fires exactly on an unrecognized X-Amz-Target, the
// JSON-RPC-protocol scenario this code exists for, and it was untouched by
// commit fa0e68c21's eleven-code fix even though the other ten ecs
// sentinels sitting right next to it were), and account-state gateway
// checks (OptInRequired, PendingVerification, AuthFailure, Blocked), a
// malformed-request-body code every JSON/CBOR-RPC protocol's own runtime
// emits before any operation handler runs (SerializationException --
// confirmed live: services/kinesis's shared CBORToJSON decode-failure path
// emits it identically across every RPCv2-CBOR service, generated
// boilerplate rather than a per-service choice), a routing-layer code REST
// APIs return for a URL matched to no HTTP method (MethodNotAllowedException
// -- confirmed live: services/lambda's capacity-provider REST routing), and
// the classic AWS Query protocol's own "no Action parameter at all" gateway
// check (MissingAction, distinct from MissingParameter's "parameter present
// in the model but missing from the request" -- confirmed live:
// services/autoscaling/services/docdb's own request dispatch), and a
// routing-layer "this HTTP route exists but isn't implemented" fallback
// paralleling MethodNotAllowedException (NotImplementedException --
// confirmed live: services/inspector2's own catch-all route handler).
var genericProtocolCodes = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
	"ValidationError":             true,
	"ValidationException":         true,
	"InvalidAction":               true,
	"MissingParameter":            true,
	"MissingRequiredParameter":    true,
	"MissingAuthenticationToken":  true,
	"Throttling":                  true,
	"ThrottlingException":         true,
	"TooManyRequestsException":    true,
	"RequestLimitExceeded":        true,
	"InternalFailure":             true,
	"InternalError":               true,
	"InternalServerError":         true,
	"ServerException":             true,
	"ServiceException":            true,
	"ServiceUnavailable":          true,
	"ServiceUnavailableException": true,
	"AccessDenied":                true,
	"AccessDeniedException":       true,
	"UnauthorizedException":       true,
	"UnrecognizedClientException": true,
	"SignatureDoesNotMatch":       true,
	"InvalidClientTokenId":        true,
	"ExpiredToken":                true,
	"ExpiredTokenException":       true,
	"RequestExpired":              true,
	"IncompleteSignature":         true,
	"InvalidParameterValue":       true,
	"InvalidParameterCombination": true,
	"InvalidQueryParameter":       true,
	"OptInRequired":               true,
	"PendingVerification":         true,
	"AuthFailure":                 true,
	"Blocked":                     true,
	"UnknownOperationException":   true,
	"UnknownOperation":            true,
	"SerializationException":      true,
	"MethodNotAllowedException":   true,
	"MissingAction":               true,
	"NotImplementedException":     true,
}
