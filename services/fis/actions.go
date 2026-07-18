package fis

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyDuration = "duration"
)

const (
	targetTypeIAMRole    = "aws:iam:role"
	targetTypeEC2Inst    = "aws:ec2:instance"
	targetTypeRDSDB      = "aws:rds:db"
	targetTypeRDSClust   = "aws:rds:cluster"
	targetTypeECSTask    = "aws:ecs:task"
	targetTypeEKSNG      = "aws:eks:nodegroup"
	targetTypeDDBTable   = "aws:dynamodb:global-table"
	targetTypeLambdaFunc = "aws:lambda:function"
	targetTypeKinesisStr = "aws:kinesis:stream"
	targetTypeCWAlarm    = "aws:cloudwatch:alarm"
	targetTypeSubnet     = "aws:ec2:subnet"
	targetTypeSpotInst   = "aws:ec2:spot-instance"
	targetTypeSSMMI      = "aws:ssm:managed-instance"

	actionIDWait = "aws:fis:wait"

	keyService     = "service"
	keyOperations  = "operations"
	keyPercentage  = "percentage"
	descPercentage = "Percentage of requests to fault (0-100)"
	descISO8601    = "ISO 8601 duration (e.g. PT5M)"
)

const (
	targetKeyRoles     = "Roles"
	targetKeyInstances = "Instances"
	targetKeyClusters  = "Clusters"
	targetKeyFunctions = "Functions"
)

const (
	descTargetAWSService = "AWS service name (e.g. dynamodb, s3)"
	descCommaSepOps      = "Comma-separated list of operations to fault-inject"
)

const (
	// HTTP status codes for FIS built-in fault actions.
	statusThrottling     = 400
	statusInternalError  = 500
	statusServiceUnavail = 503
	statusNotFound       = 404

	// percentageFull is the maximum percentage value (100%).
	percentageFull = 100
	// percentageDivisor converts a percentage to a probability.
	percentageDivisor = 100.0

	// hoursPerDay is the number of hours in a day.
	hoursPerDay = 24

	// minTargetTypeSegments is the number of colon-separated segments in a
	// fully-qualified FIS target type (aws:service:resource).
	minTargetTypeSegments = 3
)

// ----------------------------------------
// Built-in action definitions
// ----------------------------------------

// injectAPIParams returns the common parameter set for inject-api-* actions.
func injectAPIParams() []service.FISParamDef {
	return []service.FISParamDef{
		{Name: keyService, Description: descTargetAWSService, Required: true},
		{Name: keyOperations, Description: descCommaSepOps, Required: false},
		{Name: keyPercentage, Description: descPercentage, Required: false, Default: "100"},
		{Name: keyDuration, Description: descISO8601, Required: false},
	}
}

// builtinFaultActions returns the inject-api and wait built-in action definitions.
func builtinFaultActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:fis:inject-api-internal-error",
			Description: "Return HTTP 500 InternalServerError for matching API calls",
			TargetType:  targetTypeIAMRole,
			TargetKey:   targetKeyRoles,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    "aws:fis:inject-api-throttle-error",
			Description: "Return HTTP 400 ThrottlingException for matching API calls",
			TargetType:  targetTypeIAMRole,
			TargetKey:   targetKeyRoles,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    "aws:fis:inject-api-unavailable-error",
			Description: "Return HTTP 503 ServiceUnavailable for matching API calls",
			TargetType:  targetTypeIAMRole,
			TargetKey:   targetKeyRoles,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    "aws:fis:inject-api-not-found-error",
			Description: "Return HTTP 404 ResourceNotFoundException for matching API calls",
			TargetType:  targetTypeIAMRole,
			TargetKey:   targetKeyRoles,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    actionIDWait,
			Description: "Pause for a specified duration",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
			},
		},
	}
}

// builtinServiceActions returns the AWS service built-in action definitions.
func builtinServiceActions() []service.FISActionDefinition {
	groups := [][]service.FISActionDefinition{
		ec2ServiceActions(),
		rdsServiceActions(),
		ecsServiceActions(),
		eksServiceActions(),
		dynamoDBServiceActions(),
		lambdaServiceActions(),
		ssmServiceActions(),
		networkServiceActions(),
		cloudWatchServiceActions(),
		kinesisServiceActions(),
	}

	var total int
	for _, g := range groups {
		total += len(g)
	}

	all := make([]service.FISActionDefinition, 0, total)
	for _, g := range groups {
		all = append(all, g...)
	}

	return all
}

// ec2ServiceActions returns the EC2 built-in action definitions.
func ec2ServiceActions() []service.FISActionDefinition {
	const descRestartAfter = "ISO 8601 duration after which instances are restarted"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ec2:reboot-instances",
			Description: "Reboot EC2 instances",
			TargetType:  targetTypeEC2Inst,
			TargetKey:   targetKeyInstances,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: false},
			},
		},
		{
			ActionID:    "aws:ec2:stop-instances",
			Description: "Stop EC2 instances",
			TargetType:  targetTypeEC2Inst,
			TargetKey:   targetKeyInstances,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: false},
				{
					Name:        "startInstancesAfterDuration",
					Description: descRestartAfter,
					Required:    false,
				},
			},
		},
		{
			ActionID:    "aws:ec2:terminate-instances",
			Description: "Terminate EC2 instances",
			TargetType:  targetTypeEC2Inst,
			TargetKey:   targetKeyInstances,
			Parameters:  []service.FISParamDef{},
		},
		{
			ActionID:    "aws:ec2:send-spot-instance-interruptions",
			Description: "Send spot instance interruption notices to EC2 spot instances",
			TargetType:  targetTypeSpotInst,
			TargetKey:   "SpotInstances",
			Parameters: []service.FISParamDef{
				{
					Name:        "durationBeforeInterruption",
					Description: "ISO 8601 duration before interruption (PT2M maximum)",
					Required:    true,
				},
			},
		},
	}
}

// rdsServiceActions returns the RDS built-in action definitions.
func rdsServiceActions() []service.FISActionDefinition {
	const descForceFailover = "Force failover during reboot (true|false)"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:rds:reboot-db-instances",
			Description: "Reboot RDS DB instances",
			TargetType:  targetTypeRDSDB,
			TargetKey:   "DBInstances",
			Parameters: []service.FISParamDef{
				{Name: "forceFailover", Description: descForceFailover, Required: false},
			},
		},
		{
			ActionID:    "aws:rds:failover-db-cluster",
			Description: "Failover an Aurora DB cluster",
			TargetType:  targetTypeRDSClust,
			TargetKey:   targetKeyClusters,
			Parameters:  []service.FISParamDef{},
		},
		{
			ActionID:    "aws:rds:reboot-db-cluster",
			Description: "Reboot an Aurora DB cluster",
			TargetType:  targetTypeRDSClust,
			TargetKey:   targetKeyClusters,
			Parameters: []service.FISParamDef{
				{Name: "forceFailover", Description: descForceFailover, Required: false},
			},
		},
	}
}

// ecsServiceActions returns the ECS built-in action definitions.
func ecsServiceActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ecs:stop-task",
			Description: "Stop an ECS task",
			TargetType:  targetTypeECSTask,
			TargetKey:   "Tasks",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: false},
			},
		},
		{
			ActionID:    "aws:ecs:drain-container-instances",
			Description: "Drain ECS container instances",
			TargetType:  "aws:ecs:cluster",
			TargetKey:   targetKeyClusters,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
				{
					Name:        "drainagePercentage",
					Description: "Percentage of container instances to drain (1-100)",
					Required:    true,
				},
			},
		},
	}
}

// eksServiceActions returns the EKS built-in action definitions.
func eksServiceActions() []service.FISActionDefinition {
	const descTermPct = "Percentage of instances to terminate (1-100)"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:eks:terminate-nodegroup-instances",
			Description: "Terminate instances in an EKS managed node group",
			TargetType:  targetTypeEKSNG,
			TargetKey:   "Nodegroups",
			Parameters: []service.FISParamDef{
				{Name: "instanceTerminationPercentage", Description: descTermPct, Required: true},
			},
		},
		{
			ActionID:    "aws:eks:inject-kubernetes-custom-resource",
			Description: "Inject a Kubernetes custom resource into an EKS cluster",
			TargetType:  "aws:eks:cluster",
			TargetKey:   targetKeyClusters,
			Parameters: []service.FISParamDef{
				{
					Name:        "customResource",
					Description: "JSON-encoded Kubernetes custom resource manifest",
					Required:    true,
				},
				{Name: keyDuration, Description: descISO8601, Required: true},
				{
					Name:        "kubernetesApiVersion",
					Description: "Kubernetes API group and version (e.g. chaos.aws/v1alpha1)",
					Required:    true,
				},
				{Name: "kubernetesKind", Description: "Kubernetes resource kind", Required: true},
				{Name: "kubernetesNamespace", Description: "Kubernetes namespace", Required: false},
				{
					Name:        "kubernetesServiceAccount",
					Description: "Kubernetes service account for the action",
					Required:    false,
				},
			},
		},
	}
}

// dynamoDBServiceActions returns the DynamoDB built-in action definitions.
func dynamoDBServiceActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:dynamodb:global-table-pause-replication",
			Description: "Pause replication for a DynamoDB global table",
			TargetType:  targetTypeDDBTable,
			TargetKey:   "Tables",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
			},
		},
	}
}

// lambdaServiceActions returns the Lambda built-in action definitions.
func lambdaServiceActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:lambda:invocation-error",
			Description: "Force Lambda invocations to return errors for the specified duration",
			TargetType:  targetTypeLambdaFunc,
			TargetKey:   targetKeyFunctions,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
				{
					Name:        keyPercentage,
					Description: "Percentage of invocations to fault (0-100)",
					Required:    false,
					Default:     "100",
				},
			},
		},
		{
			ActionID:    "aws:lambda:invocation-add-delay",
			Description: "Add latency to Lambda invocations for the specified duration",
			TargetType:  targetTypeLambdaFunc,
			TargetKey:   targetKeyFunctions,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
				{
					Name:        "invocationDelayMilliseconds",
					Description: "Milliseconds of delay to add per invocation",
					Required:    true,
				},
				{
					Name:        keyPercentage,
					Description: "Percentage of invocations to delay (0-100)",
					Required:    false,
					Default:     "100",
				},
			},
		},
		{
			ActionID:    "aws:lambda:invocation-http-integration-response",
			Description: "Modify HTTP integration responses in Lambda functions",
			TargetType:  targetTypeLambdaFunc,
			TargetKey:   targetKeyFunctions,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
				{
					Name:        "statusCode",
					Description: "HTTP status code to return (e.g. 503)",
					Required:    true,
				},
				{
					Name:        keyPercentage,
					Description: "Percentage of responses to modify (0-100)",
					Required:    false,
					Default:     "100",
				},
			},
		},
	}
}

// ssmServiceActions returns the SSM built-in action definitions.
func ssmServiceActions() []service.FISActionDefinition {
	const descDocArn = "ARN of the SSM document"
	const descDocParams = "JSON-encoded document parameters"
	const descAutomationDocArn = "ARN of the SSM Automation runbook"
	const descAutomationParams = "JSON-encoded automation parameters"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ssm:send-command",
			Description: "Run an SSM document on managed instances",
			TargetType:  targetTypeEC2Inst,
			TargetKey:   targetKeyInstances,
			Parameters: []service.FISParamDef{
				{Name: "documentArn", Description: descDocArn, Required: true},
				{Name: "documentParameters", Description: descDocParams, Required: false},
				{Name: keyDuration, Description: descISO8601, Required: false},
			},
		},
		{
			ActionID:    "aws:ssm:start-automation-execution",
			Description: "Start an SSM Automation runbook execution",
			TargetType:  "",
			Parameters: []service.FISParamDef{
				{Name: "documentArn", Description: descAutomationDocArn, Required: true},
				{Name: "documentParameters", Description: descAutomationParams, Required: false},
				{Name: "maxDuration", Description: descISO8601, Required: false},
			},
		},
	}
}

// networkServiceActions returns the network built-in action definitions.
func networkServiceActions() []service.FISActionDefinition {
	const descConnectDuration = "ISO 8601 duration for connectivity disruption"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:network:disrupt-connectivity",
			Description: "Disrupt network connectivity for EC2 instances in a subnet",
			TargetType:  targetTypeSubnet,
			TargetKey:   "Subnets",
			Parameters: []service.FISParamDef{
				{
					Name:        "scope",
					Description: "Connectivity scope: availability-zone or vpc",
					Required:    true,
				},
				{Name: keyDuration, Description: descConnectDuration, Required: true},
			},
		},
		{
			ActionID:    "aws:network:route-table-disrupt-routes",
			Description: "Disrupt routes in a VPC route table",
			TargetType:  "aws:ec2:route-table",
			TargetKey:   "RouteTables",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
			},
		},
		{
			ActionID:    "aws:network:transit-gateway-disrupt-cross-region-connectivity",
			Description: "Disrupt cross-region connectivity via Transit Gateway",
			TargetType:  "aws:ec2:transit-gateway",
			TargetKey:   "TransitGateways",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
			},
		},
	}
}

// cloudWatchServiceActions returns the CloudWatch built-in action definitions.
func cloudWatchServiceActions() []service.FISActionDefinition {
	const descAlarmState = "State to assert: ALARM or OK"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:cloudwatch:assert-alarm-state",
			Description: "Assert that a CloudWatch alarm is in the specified state",
			TargetType:  targetTypeCWAlarm,
			TargetKey:   "Alarms",
			Parameters: []service.FISParamDef{
				{Name: "alarmState", Description: descAlarmState, Required: true},
			},
		},
	}
}

// kinesisServiceActions returns the Kinesis built-in action definitions.
func kinesisServiceActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:kinesis:disrupt-shard",
			Description: "Disrupt a Kinesis data stream shard",
			TargetType:  targetTypeKinesisStr,
			TargetKey:   "Streams",
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: true},
			},
		},
	}
}

// builtinActions returns all built-in FIS action definitions.
func builtinActions() []service.FISActionDefinition {
	fault := builtinFaultActions()
	svc := builtinServiceActions()
	all := make([]service.FISActionDefinition, 0, len(fault)+len(svc))
	all = append(all, fault...)
	all = append(all, svc...)

	return all
}

// builtinActionSummaries converts built-in action definitions to ActionSummary values.
func builtinActionSummaries(accountID, region string) []ActionSummary {
	defs := builtinActions()
	result := make([]ActionSummary, len(defs))

	for i, def := range defs {
		result[i] = actionDefToSummary(def, accountID, region)
	}

	return result
}

// ----------------------------------------
// Action discovery
// ----------------------------------------

// ListActions returns all available FIS actions: built-in + service-provided, sorted by ID.
// Built-in actions take precedence over provider-supplied actions with the same ID (dedup by ID).
func (b *InMemoryBackend) ListActions() []ActionSummary {
	b.mu.RLock("ListActions")
	providers := b.actionProviders
	b.mu.RUnlock()

	seen := make(map[string]ActionSummary)

	for _, a := range builtinActionSummaries(b.accountID, b.region) {
		seen[a.ID] = a
	}

	for _, p := range providers {
		for _, def := range p.FISActions() {
			if _, exists := seen[def.ActionID]; !exists {
				seen[def.ActionID] = actionDefToSummary(def, b.accountID, b.region)
			}
		}
	}

	all := make([]ActionSummary, 0, len(seen))
	for _, a := range seen {
		all = append(all, a)
	}

	slices.SortFunc(all, func(a, b ActionSummary) int { return strings.Compare(a.ID, b.ID) })

	return all
}

// GetAction returns a single action by ID.
func (b *InMemoryBackend) GetAction(id string) (*ActionSummary, error) {
	all := b.ListActions()

	for _, a := range all {
		if a.ID == id {
			cp := a

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrActionNotFound, id)
}

// defaultTargetKey returns the default target map key for an action when TargetKey is not set.
// It derives a reasonable key from the TargetType or action ID.
func defaultTargetKey(def service.FISActionDefinition) string {
	if def.TargetType == "" {
		return ""
	}

	// Derive from TargetType resource name (last segment after ":").
	parts := strings.Split(def.TargetType, ":")
	if len(parts) >= minTargetTypeSegments {
		last := parts[len(parts)-1]

		// Capitalize and pluralise.
		if len(last) > 0 {
			return strings.ToUpper(last[:1]) + last[1:] + "s"
		}
	}

	return "Targets"
}

// actionDefToSummary converts a FISActionDefinition to an ActionSummary.
func actionDefToSummary(def service.FISActionDefinition, accountID, region string) ActionSummary {
	arnStr := arn.Build("fis", region, accountID, fmt.Sprintf("action/%s", def.ActionID))

	params := make(map[string]ActionParameter, len(def.Parameters))
	for _, p := range def.Parameters {
		params[p.Name] = ActionParameter{
			Description: p.Description,
			Required:    p.Required,
		}
	}

	var targets map[string]ActionTarget
	if def.TargetType != "" {
		key := def.TargetKey
		if key == "" {
			key = defaultTargetKey(def)
		}

		targets = map[string]ActionTarget{
			key: {ResourceType: def.TargetType},
		}
	}

	return ActionSummary{
		ID:          def.ActionID,
		Arn:         arnStr,
		Description: def.Description,
		Targets:     targets,
		Parameters:  params,
		Tags:        map[string]string{},
	}
}

// ----------------------------------------
// Built-in target resource types
// ----------------------------------------

// builtinTargetResourceTypes returns the well-known FIS target resource types.
func builtinTargetResourceTypes() []TargetResourceTypeSummary {
	return []TargetResourceTypeSummary{
		{
			ResourceType: targetTypeIAMRole,
			Description:  "IAM role (used for API fault injection targeting)",
		},
		{
			ResourceType: targetTypeEC2Inst,
			Description:  "EC2 instance",
			Parameters: map[string]TargetResourceTypeParameter{
				"availabilityZoneIdentifier": {Description: "Filter by availability zone"},
				"placement/tenancy":          {Description: "Filter by instance tenancy"},
				"state/name":                 {Description: "Filter by instance state name"},
			},
		},
		{
			ResourceType: targetTypeSpotInst,
			Description:  "EC2 spot instance",
		},
		{
			ResourceType: targetTypeEKSNG,
			Description:  "EKS managed node group",
		},
		{
			ResourceType: "aws:eks:cluster",
			Description:  "EKS cluster",
		},
		{
			ResourceType: targetTypeLambdaFunc,
			Description:  "Lambda function",
		},
		{
			ResourceType: targetTypeRDSDB,
			Description:  "RDS DB instance",
		},
		{
			ResourceType: targetTypeRDSClust,
			Description:  "RDS Aurora DB cluster",
		},
		{
			ResourceType: targetTypeECSTask,
			Description:  "ECS task",
		},
		{
			ResourceType: "aws:ecs:cluster",
			Description:  "ECS cluster",
		},
		{
			ResourceType: targetTypeKinesisStr,
			Description:  "Kinesis data stream",
		},
		{
			ResourceType: targetTypeDDBTable,
			Description:  "DynamoDB global table",
		},
		{
			ResourceType: "aws:dynamodb:table",
			Description:  "DynamoDB table",
		},
		{
			ResourceType: targetTypeCWAlarm,
			Description:  "CloudWatch alarm",
		},
		{
			ResourceType: targetTypeSubnet,
			Description:  "EC2 VPC subnet",
		},
		{
			ResourceType: "aws:ec2:route-table",
			Description:  "EC2 VPC route table",
		},
		{
			ResourceType: "aws:ec2:transit-gateway",
			Description:  "AWS Transit Gateway",
		},
		{
			ResourceType: targetTypeSSMMI,
			Description:  "SSM managed instance",
		},
	}
}

// ----------------------------------------
// Fault rule building
// ----------------------------------------

// faultErrorForAction returns the chaos.FaultError for a given built-in action ID.
func faultErrorForAction(actionID string) chaos.FaultError {
	switch actionID {
	case "aws:fis:inject-api-throttle-error":
		return chaos.FaultError{Code: "ThrottlingException", StatusCode: statusThrottling}
	case "aws:fis:inject-api-internal-error":
		return chaos.FaultError{Code: "InternalServerError", StatusCode: statusInternalError}
	case "aws:fis:inject-api-not-found-error":
		return chaos.FaultError{Code: "ResourceNotFoundException", StatusCode: statusNotFound}
	default:
		return chaos.FaultError{Code: "ServiceUnavailable", StatusCode: statusServiceUnavail}
	}
}

// buildFaultRules converts an experiment action into one or more chaos.FaultRule values.
// The action must be one of the aws:fis:inject-api-* actions.
func buildFaultRules(action ExperimentTemplateAction) []chaos.FaultRule {
	svcName := action.Parameters[keyService]
	if svcName == "" {
		return nil
	}

	pct := parsePercentage(action.Parameters[keyPercentage])
	faultErr := faultErrorForAction(action.ActionID)
	errCopy := faultErr

	ops := parseOperations(action.Parameters[keyOperations])

	if len(ops) == 0 {
		// No specific operations – inject fault for all calls to this service.
		return []chaos.FaultRule{
			{
				Service:     svcName,
				Probability: pct,
				Error:       &errCopy,
			},
		}
	}

	rules := make([]chaos.FaultRule, len(ops))

	for i, op := range ops {
		e := faultErr

		rules[i] = chaos.FaultRule{
			Service:     svcName,
			Operation:   op,
			Probability: pct,
			Error:       &e,
		}
	}

	return rules
}

// parsePercentage parses a percentage string (0-100) to a 0.0-1.0 probability.
// Returns 1.0 on empty or invalid input.
func parsePercentage(s string) float64 {
	if s == "" {
		return 1.0
	}

	pct, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || pct <= 0 {
		return 1.0
	}

	if pct >= percentageFull {
		return 1.0
	}

	return pct / percentageDivisor
}

// parseOperations splits a comma-separated operation list and trims whitespace.
func parseOperations(s string) []string {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}

	return result
}

// ----------------------------------------
// ISO 8601 duration parser
// ----------------------------------------

// parseISODuration parses a subset of ISO 8601 duration strings (PTxHxMxS).
// Returns 0 on empty or invalid input.
func parseISODuration(s string) time.Duration {
	if s == "" {
		return 0
	}

	s = strings.ToUpper(strings.TrimSpace(s))
	if s == "" || s[0] != 'P' {
		return 0
	}

	s = s[1:] // consume 'P'

	// Advance past the 'T' separator if present.
	inTime := false

	if len(s) > 0 && s[0] == 'T' {
		inTime = true
		s = s[1:]
	}

	var total time.Duration
	numBuf := strings.Builder{}

	for _, ch := range s {
		switch {
		case unicode.IsDigit(ch) || ch == '.':
			numBuf.WriteRune(ch)
		case ch == 'T' && !inTime:
			inTime = true
		default:
			numStr := numBuf.String()
			numBuf.Reset()

			if numStr == "" {
				continue
			}

			val, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				continue
			}

			total += applyISOUnit(ch, val, inTime)
		}
	}

	return total
}

// isValidISODuration returns true if s is a syntactically valid ISO 8601 duration with a positive value.
// Returns false for empty strings.
func isValidISODuration(s string) bool {
	if s == "" {
		return false
	}

	upper := strings.ToUpper(strings.TrimSpace(s))
	if len(upper) == 0 || upper[0] != 'P' {
		return false
	}

	rest := upper[1:]
	if len(rest) == 0 {
		return false
	}

	// Must contain at least one digit.
	for _, ch := range rest {
		if unicode.IsDigit(ch) {
			return parseISODuration(s) > 0
		}
	}

	return false
}

// applyISOUnit converts an ISO 8601 duration unit character and value to a time.Duration.
// AWS FIS only supports PT…H…M…S (hours, minutes, seconds) and P…D (days).
// Years (Y), months (M before T), and weeks (W) are not supported and return 0.
func applyISOUnit(ch rune, val float64, inTime bool) time.Duration {
	switch ch {
	case 'H':
		return time.Duration(val * float64(time.Hour))
	case 'M':
		if inTime {
			return time.Duration(val * float64(time.Minute))
		}
		// 'M' before 'T' would mean months — not supported by AWS FIS.
		return 0
	case 'S':
		return time.Duration(val * float64(time.Second))
	case 'D':
		return time.Duration(val * float64(hoursPerDay*time.Hour))
	}

	// Y (years) and W (weeks) are not supported by AWS FIS durations.
	return 0
}
