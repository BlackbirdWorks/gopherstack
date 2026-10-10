package cloudformation

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	opUpdateStack               = "UpdateStack"
	insufficientCapabilitiesMsg = "Requires capabilities : [CAPABILITY_IAM]"
	ctxKeyForm                  = "cfn.form"
	ctxKeyAction                = "cfn.action"
)

// refineNotFoundMessage swaps the backend's bare sentinel messages for AWS's
// "Stack with id X does not exist" / "Resource X does not exist for stack Y" /
// "ChangeSet [X] does not exist" wording using the request's own identifiers.
func refineNotFoundMessage(c *echo.Context, msg string) string {
	form, _ := c.Get(ctxKeyForm).(url.Values)
	action, _ := c.Get(ctxKeyAction).(string)

	if form == nil {
		return msg
	}

	switch {
	case msg == ErrStackNotFound.Error() || strings.HasPrefix(msg, ErrStackNotFound.Error()+": "):
		name := strings.TrimPrefix(strings.TrimPrefix(msg, ErrStackNotFound.Error()), ": ")
		if name == "" {
			name = form.Get("StackName")
		}
		if name == "" {
			return msg
		}
		if action == opUpdateStack {
			return fmt.Sprintf("Stack [%s] does not exist", name)
		}

		return fmt.Sprintf("Stack with id %s does not exist", name)
	case msg == ErrResourceNotFound.Error() && form.Get("LogicalResourceId") != "":
		return fmt.Sprintf(
			"Resource %s does not exist for stack %s",
			form.Get("LogicalResourceId"),
			form.Get("StackName"),
		)
	case strings.HasPrefix(msg, ErrChangeSetNotFound.Error()) && form.Get("ChangeSetName") != "":
		return fmt.Sprintf("ChangeSet [%s] does not exist", form.Get("ChangeSetName"))
	}

	return msg
}
