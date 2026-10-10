package applicationautoscaling

import (
	"fmt"
	"slices"
	"strings"
)

const (
	tablePrefix           = "table/"
	segmentsPair          = 2
	segmentsTriple        = 3
	segmentsQuad          = 4
	colonResourceSegments = 3
	latestAlias           = "$LATEST"
)

// resourceIDRule describes the ResourceId shape for a ScalableDimension
// (api_op_RegisterScalableTarget.go ResourceId doc examples).
type resourceIDRule struct {
	prefix   string
	arnPart  string
	segments int
}

func resourceIDRules() map[string]resourceIDRule {
	return map[string]resourceIDRule{
		"ecs:service:DesiredCount": {prefix: "service/", segments: segmentsTriple},
		"ec2:spot-fleet-request:TargetCapacity": {
			prefix:   "spot-fleet-request/",
			segments: segmentsPair,
		},
		"elasticmapreduce:instancegroup:InstanceCount": {
			prefix:   "instancegroup/",
			segments: segmentsTriple,
		},
		"appstream:fleet:DesiredCapacity":                 {prefix: "fleet/", segments: segmentsPair},
		"dynamodb:table:ReadCapacityUnits":                {prefix: tablePrefix, segments: segmentsPair},
		"dynamodb:table:WriteCapacityUnits":               {prefix: tablePrefix, segments: segmentsPair},
		"dynamodb:index:ReadCapacityUnits":                {prefix: tablePrefix, segments: segmentsQuad},
		"dynamodb:index:WriteCapacityUnits":               {prefix: tablePrefix, segments: segmentsQuad},
		"sagemaker:variant:DesiredInstanceCount":          {prefix: "endpoint/", segments: segmentsQuad},
		"sagemaker:variant:DesiredProvisionedConcurrency": {prefix: "endpoint/", segments: segmentsQuad},
		"sagemaker:inference-component:DesiredCopyCount": {
			prefix:   "inference-component/",
			segments: segmentsPair,
		},
		"cassandra:table:ReadCapacityUnits":  {prefix: "keyspace/", segments: segmentsQuad},
		"cassandra:table:WriteCapacityUnits": {prefix: "keyspace/", segments: segmentsQuad},
		"elasticache:cache-cluster:Nodes": {
			prefix:   "cache-cluster/",
			segments: segmentsPair,
		},
		"elasticache:replication-group:NodeGroups": {
			prefix:   "replication-group/",
			segments: segmentsPair,
		},
		"elasticache:replication-group:Replicas": {
			prefix:   "replication-group/",
			segments: segmentsPair,
		},
		"workspaces:workspacespool:DesiredUserSessions": {
			prefix:   "workspacespool/",
			segments: segmentsPair,
		},
		"comprehend:document-classifier-endpoint:DesiredInferenceUnits": {arnPart: ":document-classifier-endpoint/"},
		"comprehend:entity-recognizer-endpoint:DesiredInferenceUnits":   {arnPart: ":entity-recognizer-endpoint/"},
		"kafka:broker-storage:VolumeSize":                               {arnPart: ":cluster/"},
	}
}

// validateResourceID rejects a ResourceId whose shape does not match its dimension.
func validateResourceID(scalableDimension, resourceID string) error {
	if !resourceIDMatches(scalableDimension, resourceID) {
		return fmt.Errorf("%w: ResourceId %q is not valid for ScalableDimension %q",
			ErrValidation, resourceID, scalableDimension)
	}

	return nil
}

func resourceIDMatches(scalableDimension, resourceID string) bool {
	switch scalableDimension {
	case "rds:cluster:ReadReplicaCount", "neptune:cluster:ReadReplicaCount":
		name, ok := strings.CutPrefix(resourceID, "cluster:")

		return ok && name != ""
	case "lambda:function:ProvisionedConcurrency":
		return lambdaResourceIDValid(resourceID)
	case "custom-resource:ResourceType:Property":
		return resourceID != ""
	}

	rule, ok := resourceIDRules()[scalableDimension]
	if !ok {
		return true
	}

	if rule.arnPart != "" {
		return strings.HasPrefix(resourceID, "arn:") && strings.Contains(resourceID, rule.arnPart)
	}

	return slashedResourceIDValid(scalableDimension, rule, resourceID)
}

// lambdaResourceIDValid accepts function:<name>:<alias or version>, never $LATEST.
func lambdaResourceIDValid(resourceID string) bool {
	parts := strings.Split(resourceID, ":")

	return len(parts) == colonResourceSegments && parts[0] == "function" &&
		parts[1] != "" && parts[2] != "" && parts[2] != latestAlias
}

func slashedResourceIDValid(scalableDimension string, rule resourceIDRule, resourceID string) bool {
	if !strings.HasPrefix(resourceID, rule.prefix) {
		return false
	}

	parts := strings.Split(resourceID, "/")
	if len(parts) != rule.segments || slices.Contains(parts, "") {
		return false
	}

	return indexMarkersValid(scalableDimension, parts)
}

// indexMarkersValid checks the literal middle segments of the multi-part IDs.
func indexMarkersValid(scalableDimension string, parts []string) bool {
	switch {
	case strings.HasPrefix(scalableDimension, "dynamodb:index"):
		return parts[2] == "index"
	case strings.HasPrefix(scalableDimension, "sagemaker:variant"):
		return parts[2] == "variant"
	case strings.HasPrefix(scalableDimension, "cassandra:"):
		return parts[2] == "table"
	}

	return true
}
