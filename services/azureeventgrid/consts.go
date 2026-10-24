package azureeventgrid

// Delivery outcome labels and error codes.
const (
	outcomeNotFound    = "NotFound"
	outcomeGeneric     = "GenericError"
	outcomeBadRequest  = "BadRequest"
	outcomeSuccess     = "Success"
	attrType           = "type"
	fieldSubject       = "subject"
	fieldEventType     = "eventType"
	fieldEventTime     = "eventTime"
	fieldDataVersion   = "dataVersion"
	opStringNotBegins  = "StringNotBeginsWith"
	opStringNotContain = "StringNotContains"
	opStringNotEnds    = "StringNotEndsWith"
	opStringNotIn      = "StringNotIn"
)
