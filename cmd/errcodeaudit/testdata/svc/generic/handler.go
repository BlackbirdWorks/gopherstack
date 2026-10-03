package generic

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/fakesvc"
)

var _ = fakesvc.ServiceID

type errBody struct {
	Code    string
	Message string
}

func writeError(c any, status int, code, msg string) { _ = errBody{Code: code, Message: msg} }

func handle(c any) {
	writeError(c, 400, "ValidationException", "x")
	writeError(c, 400, "ThrottlingException", "x")
	writeError(c, 500, "InternalFailure", "x")
	writeError(c, 503, "ServiceUnavailable", "x")
	writeError(c, 403, "AccessDeniedException", "x")
	writeError(c, 400, "Sender", "x")
	writeError(c, 400, "BogusGenericLookalike", "x")
}
