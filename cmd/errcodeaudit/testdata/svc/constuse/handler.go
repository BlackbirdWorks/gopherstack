package constuse

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

const (
	codeScanStatusDone = "STATUSDONE"
	codeNeverUsed      = "NEVERUSED"
	codeRealUse        = "InventedConstCode"
	keyStatus          = "Status"
)

func handle(c any, scan map[string]any) {
	scan[keyStatus] = codeScanStatusDone

	writeError(c, 400, codeRealUse, "x")
}
