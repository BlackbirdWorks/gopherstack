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
		if attrName == attrNameName {
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
	case resTypeLogsDestination:
		if attrName == attrNameArn {
			return logsDestinationArn(physID, accountID, region), true
		}

		return physID, true
	}

	if v, ok := getStreamingResourceAttribute(resType, physID, attrName, accountID, region); ok {
		return v, true
	}

	if v, ok := getServiceDiscoveryAttribute(resType, physID, attrName, accountID, region); ok {
		return v, true
	}

	return getManagedTypesAttribute(resType, physID, attrName, accountID, region)
}

// getStreamingResourceAttribute derives Fn::GetAtt attribute values for
// Kinesis/Firehose/ECS TaskSet types (split out of getExtraResourceAttribute
// to keep its cyclomatic complexity down).
func getStreamingResourceAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeKinesisStreamConsumer:
		return kinesisStreamConsumerAttribute(physID, attrName), true
	case resTypeFirehoseDeliveryStream, resTypeFirehoseDeliveryStreamAlias:
		if attrName == attrNameArn {
			return arn.Build("firehose", region, accountID, "deliverystream/"+physID), true
		}

		return physID, true
	case resTypeECSTaskSet:
		if attrName == "Id" {
			if _, _, id, ok := taskSetInfoFromARN(physID); ok {
				return id, true
			}
		}

		return physID, true
	default:

		return "", false
	}
}

// getServiceDiscoveryAttribute derives Fn::GetAtt attribute values for
// AWS::ServiceDiscovery::* types (split out of getExtraResourceAttribute to
// keep its cyclomatic complexity down).
func getServiceDiscoveryAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeSDPrivateDNSNamespace, resTypeSDHTTPNamespace, resTypeSDPublicDNSNamespace:
		if attrName == attrNameArn {
			return sdNamespaceArn(physID, accountID, region), true
		}

		return physID, true
	case resTypeSDService:
		if attrName == attrNameArn {
			return sdServiceArn(physID, accountID, region), true
		}

		return physID, true
	default:

		return "", false
	}
}

// getManagedTypesAttribute derives Fn::GetAtt attribute values for the
// EC2/AutoScaling/RDS types added in resources_more_managed_types.go.
func getManagedTypesAttribute(resType, physID, attrName, accountID, region string) (string, bool) {
	switch resType {
	case resTypeEC2LaunchTemplate:
		if attrName == "DefaultVersionNumber" || attrName == "LatestVersionNumber" {
			// CreateStack always creates exactly one launch template version.
			return "1", true
		}

		return physID, true
	case resTypeASGScalingPolicy:
		if attrName == "PolicyName" {
			return scalingPolicyNameFromARN(physID), true
		}

		return physID, true
	case resTypeRDSDBProxy:
		if attrName == "DBProxyArn" {
			return rdsDBProxyArn(physID, accountID, region), true
		}

		return physID, true
	case resTypeCWInsightRule:
		// physID is the rule ARN (Ref); RuleName is the "insight-rule/" tail.
		if attrName == "RuleName" {
			if idx := strings.LastIndex(physID, "/"); idx >= 0 {
				return physID[idx+1:], true
			}
		}

		return physID, true
	}

	return "", false
}

// kinesisStreamConsumerAttribute derives StreamARN/ConsumerName from a consumer
// ARN of the form "<streamARN>/consumer/<name>:<timestamp>" -- ConsumerARN
// (the physID/Ref value) already covers the Arn-shaped attrs.
func kinesisStreamConsumerAttribute(physID, attrName string) string {
	const marker = "/consumer/"

	idx := strings.LastIndex(physID, marker)
	if idx < 0 {
		return physID
	}

	switch attrName {
	case "StreamARN":
		return physID[:idx]
	case "ConsumerName":
		name := physID[idx+len(marker):]
		if colon := strings.LastIndex(name, ":"); colon >= 0 {
			return name[:colon]
		}

		return name
	case "ConsumerStatus":
		// RegisterStreamConsumer's in-memory model activates consumers
		// synchronously -- no CREATING transition exists to observe.
		return "ACTIVE"
	default:
		return physID
	}
}

// arnResourceTail returns the final colon-delimited segment of an ARN (the resource name).
func arnResourceTail(s string) string {
	parts := strings.Split(s, ":")
	if len(parts) == 0 {
		return s
	}

	return parts[len(parts)-1]
}
