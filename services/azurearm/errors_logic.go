package azurearm

import (
	"errors"
	"fmt"
	"net/http"
)

// Sentinels for the Microsoft.Logic resource provider.
var (
	ErrLogicWorkflowNotFound = errors.New("azurearm: logic workflow not found")
	ErrLogicTriggerNotFound  = errors.New("azurearm: logic trigger not found")
	ErrLogicRunNotFound      = errors.New("azurearm: logic run not found")
	ErrLogicActionNotFound   = errors.New("azurearm: logic run action not found")
	ErrLogicHistoryNotFound  = errors.New("azurearm: logic trigger history not found")
	ErrLogicVersionNotFound  = errors.New("azurearm: logic workflow version not found")
	ErrLogicExpr             = errors.New("logic expression error")
)

// ErrLogicDestinationNotPermitted is returned by the HTTP action's dialer for a host outside loopback.
var ErrLogicDestinationNotPermitted = errors.New(
	"the HTTP action may only reach loopback hosts unless --azure-arm-logic-allow-external is set")

// logicError is a Microsoft.Logic failure that carries its own ARM code, message and status.
type logicError struct {
	code    string
	message string
	status  int
}

func (e *logicError) Error() string { return e.code + ": " + e.message }

func logicFail(status int, code, format string, a ...any) error {
	return &logicError{status: status, code: code, message: fmt.Sprintf(format, a...)}
}

func logicBadRequest(code, format string, a ...any) error {
	return logicFail(http.StatusBadRequest, code, format, a...)
}

func invalidTemplate(format string, a ...any) error {
	return logicBadRequest("InvalidTemplate", "The template validation failed: "+format, a...)
}

func logicErrorDetails(err error) (errorEntry, bool) {
	if le, ok := errors.AsType[*logicError](err); ok {
		return errorEntry{le.code, le.message, le.status}, true
	}

	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrLogicWorkflowNotFound, notFoundEntry("The workflow was not found.")},
		{ErrLogicTriggerNotFound, notFoundEntry("The workflow trigger was not found.")},
		{ErrLogicRunNotFound, notFoundEntry("The workflow run was not found.")},
		{ErrLogicActionNotFound, notFoundEntry("The workflow run action was not found.")},
		{ErrLogicHistoryNotFound, notFoundEntry("The workflow trigger history was not found.")},
		{ErrLogicVersionNotFound, notFoundEntry("The workflow version was not found.")},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}

// logicInputError is a validation failure of an action's evaluated inputs; its text is the action error message.
type logicInputError string

func (e logicInputError) Error() string { return string(e) }

func inputErr(format string, a ...any) error { return logicInputError(fmt.Sprintf(format, a...)) }
