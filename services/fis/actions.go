package fis

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyDuration = "duration"
)

const (
	targetTypeIAMRole  = "aws:iam:role"
	targetTypeEC2Inst  = "aws:ec2:instance"
	targetTypeRDSDB    = "aws:rds:db"
	targetTypeRDSClust = "aws:rds:cluster"
	targetTypeECSTask  = "aws:ecs:task"
	targetTypeEKSNG    = "aws:eks:nodegroup"
	targetTypeDDBTable = "aws:dynamodb:global-table"
	actionIDWait       = "aws:fis:wait"
	keyService         = "service"
	keyOperations      = "operations"
	keyPercentage      = "percentage"
	descPercentage     = "Percentage of requests to fault (0-100)"
	descISO8601        = "ISO 8601 duration (e.g. PT5M)"
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

	// percentageFull is the maximum percentage value (100%).
	percentageFull = 100
	// percentageDivisor converts a percentage to a probability.
	percentageDivisor = 100.0

	// hoursPerDay is the number of hours in a day.
	hoursPerDay = 24
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
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    "aws:fis:inject-api-throttle-error",
			Description: "Return HTTP 400 ThrottlingException for matching API calls",
			TargetType:  targetTypeIAMRole,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    "aws:fis:inject-api-unavailable-error",
			Description: "Return HTTP 503 ServiceUnavailable for matching API calls",
			TargetType:  targetTypeIAMRole,
			Parameters:  injectAPIParams(),
		},
		{
			ActionID:    actionIDWait,
			Description: "Pause for a specified duration",
			Parameters:  []service.FISParamDef{{Name: keyDuration, Description: descISO8601, Required: true}},
		},
	}
}

// builtinServiceActions returns the AWS service built-in action definitions.
func builtinServiceActions() []service.FISActionDefinition {
	const descRestartAfter = "ISO 8601 duration after which instances are restarted"
	const descForceFailover = "Force failover during reboot (true|false)"
	const descTermPct = "Percentage of instances to terminate (1-100)"
	const descDocArn = "ARN of the SSM document"
	const descDocParams = "JSON-encoded document parameters"

	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ec2:reboot-instances",
			Description: "Reboot EC2 instances",
			TargetType:  targetTypeEC2Inst,
			Parameters:  []service.FISParamDef{{Name: keyDuration, Description: descISO8601, Required: false}},
		},
		{
			ActionID:    "aws:ec2:stop-instances",
			Description: "Stop EC2 instances",
			TargetType:  targetTypeEC2Inst,
			Parameters: []service.FISParamDef{
				{Name: keyDuration, Description: descISO8601, Required: false},
				{Name: "startInstancesAfterDuration", Description: descRestartAfter, Required: false},
			},
		},
		{
			ActionID:    "aws:ec2:terminate-instances",
			Description: "Terminate EC2 instances",
			TargetType:  targetTypeEC2Inst,
			Parameters:  []service.FISParamDef{},
		},
		{
			ActionID:    "aws:rds:reboot-db-instances",
			Description: "Reboot RDS DB instances",
			TargetType:  targetTypeRDSDB,
			Parameters: []service.FISParamDef{
				{Name: "forceFailover", Description: descForceFailover, Required: false},
			},
		},
		{
			ActionID:    "aws:rds:failover-db-cluster",
			Description: "Failover an Aurora DB cluster",
			TargetType:  targetTypeRDSClust,
			Parameters:  []service.FISParamDef{},
		},
		{
			ActionID:    "aws:ecs:stop-task",
			Description: "Stop an ECS task",
			TargetType:  targetTypeECSTask,
			Parameters:  []service.FISParamDef{{Name: keyDuration, Description: descISO8601, Required: false}},
		},
		{
			ActionID:    "aws:eks:terminate-nodegroup-instances",
			Description: "Terminate instances in an EKS managed node group",
			TargetType:  targetTypeEKSNG,
			Parameters: []service.FISParamDef{
				{Name: "instanceTerminationPercentage", Description: descTermPct, Required: true},
			},
		},
		{
			ActionID:    "aws:dynamodb:global-table-pause-replication",
			Description: "Pause replication for a DynamoDB global table",
			TargetType:  targetTypeDDBTable,
			Parameters:  []service.FISParamDef{{Name: keyDuration, Description: descISO8601, Required: true}},
		},
		{
			ActionID:    "aws:ssm:send-command",
			Description: "Run an SSM document on managed instances",
			TargetType:  targetTypeEC2Inst,
			Parameters: []service.FISParamDef{
				{Name: "documentArn", Description: descDocArn, Required: true},
				{Name: "documentParameters", Description: descDocParams, Required: false},
				{Name: keyDuration, Description: descISO8601, Required: false},
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

// actionDefToSummary converts a FISActionDefinition to an ActionSummary.
func actionDefToSummary(def service.FISActionDefinition, accountID, region string) ActionSummary {
	arnStr := fmt.Sprintf("arn:aws:fis:%s:%s:action/%s", region, accountID, def.ActionID)

	params := make(map[string]ActionParameter, len(def.Parameters))
	for _, p := range def.Parameters {
		params[p.Name] = ActionParameter{
			Description: p.Description,
			Required:    p.Required,
		}
	}

	var targets map[string]ActionTarget
	if def.TargetType != "" {
		targets = map[string]ActionTarget{
			"Roles": {ResourceType: def.TargetType},
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
		{ResourceType: targetTypeIAMRole, Description: "IAM role (used for API fault injection targeting)"},
		{ResourceType: targetTypeEC2Inst, Description: "EC2 instance"},
		{ResourceType: targetTypeEKSNG, Description: "EKS managed node group"},
		{ResourceType: "aws:lambda:function", Description: "Lambda function"},
		{ResourceType: targetTypeRDSDB, Description: "RDS DB instance"},
		{ResourceType: targetTypeRDSClust, Description: "RDS Aurora DB cluster"},
		{ResourceType: targetTypeECSTask, Description: "ECS task"},
		{ResourceType: "aws:kinesis:stream", Description: "Kinesis data stream"},
		{ResourceType: targetTypeDDBTable, Description: "DynamoDB global table"},
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
//
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
