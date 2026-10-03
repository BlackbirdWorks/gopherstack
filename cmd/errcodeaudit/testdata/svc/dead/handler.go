package dead

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

var (
	ErrDeadThing = errors.New("DeadThingException")
	ErrLiveThing = errors.New("LiveThingException")
)

func raise() error { return ErrLiveThing }
