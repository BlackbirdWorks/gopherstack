package header

import "github.com/aws/aws-sdk-go-v2/service/fakesvc"

var _ = fakesvc.ServiceID

// classifyOutcomeError maps an outcome to the X-Amz-Outcome response
// header value.
func classifyOutcomeError(bad bool) string {
	if bad {
		return "HeaderOutcomeBad"
	}

	return ""
}

// classifyKindError picks the code for a failed request.
func classifyKindError(bad bool) string {
	if bad {
		return "PlainClassifierCode"
	}

	return ""
}

// classifyTypeError returns the type that travels in the X-Amzn-Errortype
// header.
func classifyTypeError(bad bool) string {
	if bad {
		return "ErrortypeHeaderCode"
	}

	return ""
}
