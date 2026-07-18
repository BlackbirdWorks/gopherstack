package cloudformation

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// getExtraResourceAttribute derives Fn::GetAtt attribute values for phase-5 resource types.
// It returns ok=false when resType is not a phase-5 type so the caller can fall back to physID.
func getExtraResourceAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeEC2Volume, resTypeEC2NetworkInterface:
		return physID, true
	case resTypeKMSAlias:
		if attrName == attrNameArn {
			return arn.Build("kms", region, accountID, physID), true
		}

		return physID, true
	case resTypeStepFunctionsActivity:
		if attrName == "Name" {
			return arnResourceTail(physID), true
		}

		return physID, true
	case resTypeEventsConnection:
		if attrName == attrNameArn {
			return arn.Build("events", region, accountID, "connection/"+physID), true
		}

		return physID, true
	case resTypeLogsLogStream, resTypeLogsMetricFilter, resTypeLogsSubscriptionFltr:
		// physID is "<group>|<child>"; GetAtt returns the child name.
		if _, child, ok := splitLogsPhysID(physID); ok {
			return child, true
		}

		return physID, true
	}

	return "", false
}

// arnResourceTail returns the final colon-delimited segment of an ARN (the resource name).
func arnResourceTail(s string) string {
	parts := strings.Split(s, ":")
	if len(parts) == 0 {
		return s
	}

	return parts[len(parts)-1]
}
