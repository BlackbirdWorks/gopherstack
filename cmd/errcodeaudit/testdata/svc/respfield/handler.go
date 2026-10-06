package respfield

import "github.com/aws/aws-sdk-go-v2/service/fakesvc"

var _ = fakesvc.ServiceID

type itemError struct {
	Code    string
	Message string
	ItemID  string
}

type errEnvelope struct {
	Code    string
	Message string
}

type payloadError struct {
	Code   string
	ItemID string
}

func (e payloadError) Error() string { return e.Code }

type failureRow struct {
	FailureCode string
	RecordID    string
}

const codeRowFailed = "RowFailedCode"

func writeError(c any, status int, code, msg string) { _ = errEnvelope{Code: code, Message: msg} }

func newRow(id, code string) failureRow { return failureRow{FailureCode: code, RecordID: id} }

func handle(id string) {
	_ = itemError{Code: "ItemFailedCode", Message: "failed", ItemID: id}
	_ = errEnvelope{Code: "EnvelopeInventedCode", Message: "bad"}
	_ = payloadError{Code: "PayloadErrorCode", ItemID: id}
	_ = newRow(id, codeRowFailed)
}

var _ = handle
