package alias

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
	writeError(c, 400, "InvalidParameterInput", "aliased wire code")
	writeError(c, 400, "LimitExceeded", "aliased wire code")
	writeError(c, 400, "InventedAliasException", "not in the SDK")
}
