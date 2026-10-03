package fallback

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"

	"github.com/aws/aws-sdk-go-v2/service/fakesvc"
)

var _ = fakesvc.ServiceID

type errBody struct {
	Code    string
	Message string
}

func writeError(c any, status int, code, msg string) { _ = errBody{Code: code, Message: msg} }

var ErrKnown = errors.New("KnownThing")

type row struct {
	err  error
	code string
}

var errUnknownAction = errors.New("unknown action")

var rows = []row{
	{errUnknownAction, "RouterFallbackCode"},
}

func classify(c any, err error, target string) {
	switch {
	case errors.Is(err, ErrKnown):
		writeError(c, 400, "InventedCaseException", "specific sentinel case")
	case errors.Is(err, awserr.ErrNotFound):
		writeError(c, 400, "GenericCategoryCode", "x")
	default:
		writeError(c, 500, "DefaultFallbackCode", "x")
	}

	if target == "" {
		writeError(c, 400, "MissingTargetCode", "missing or invalid X-Amz-Target header")
	}
}

func codeFor(err error) string {
	code := "InitialDefaultCode"

	if errors.Is(err, ErrKnown) {
		code = "OverriddenCode"
	}

	return code
}
