package member

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

type itemError struct {
	TagKey    string
	ErrorCode string
}

type wrapper struct{ Detail itemError }

func handle(c any, keys []string) {
	var failures []itemError

	for _, k := range keys {
		failures = append(failures, itemError{TagKey: k, ErrorCode: "PerItemFailure"})
	}

	_ = wrapper{Detail: itemError{ErrorCode: "NestedItemFailure"}}
	_ = failures
	_ = errBody{Code: "EnvelopeInvented"}
	_ = struct{ ErrorCode string }{ErrorCode: "TopLevelInvented"}
}
