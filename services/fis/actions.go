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
	// daysPerWeek is the number of days in a week.
	daysPerWeek = 7
	// daysPerMonth is the approximate number of days per month for duration calculations.
	daysPerMonth = 30
)

// ----------------------------------------
// Built-in action definitions
// ----------------------------------------

// builtinActions returns the built-in FIS action definitions.
//

func builtinActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:fis:inject-api-internal-error",
			Description: "Return HTTP 500 InternalServerError for matching API calls",
			TargetType:  "aws:iam:role",
			Parameters: []service.FISParamDef{
				{Name: "service", Description: "AWS service name (e.g. dynamodb, s3)", Required: true},
				{
					Name:        "operations",
					Description: "Comma-separated list of operations to fault-inject",
					Required:    false,
				},
				{
					Name:        "percentage",
					Description: "Percentage of requests to fault (0-100)",
					Required:    false,
					Default:     "100",
				},
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: false},
			},
		},
		{
			ActionID:    "aws:fis:inject-api-throttle-error",
			Description: "Return HTTP 400 ThrottlingException for matching API calls",
			TargetType:  "aws:iam:role",
			Parameters: []service.FISParamDef{
				{Name: "service", Description: "AWS service name (e.g. dynamodb, s3)", Required: true},
				{
					Name:        "operations",
					Description: "Comma-separated list of operations to fault-inject",
					Required:    false,
				},
				{
					Name:        "percentage",
					Description: "Percentage of requests to fault (0-100)",
					Required:    false,
					Default:     "100",
				},
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: false},
			},
		},
		{
			ActionID:    "aws:fis:inject-api-unavailable-error",
			Description: "Return HTTP 503 ServiceUnavailable for matching API calls",
			TargetType:  "aws:iam:role",
			Parameters: []service.FISParamDef{
				{Name: "service", Description: "AWS service name (e.g. dynamodb, s3)", Required: true},
				{
					Name:        "operations",
					Description: "Comma-separated list of operations to fault-inject",
					Required:    false,
				},
				{
					Name:        "percentage",
					Description: "Percentage of requests to fault (0-100)",
					Required:    false,
					Default:     "100",
				},
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: false},
			},
		},
		{
			ActionID:    "aws:fis:wait",
			Description: "Pause for a specified duration",
			Parameters: []service.FISParamDef{
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: true},
			},
		},
	}
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
		{ResourceType: "aws:iam:role", Description: "IAM role (used for API fault injection targeting)"},
		{ResourceType: "aws:ec2:instance", Description: "EC2 instance"},
		{ResourceType: "aws:lambda:function", Description: "Lambda function"},
		{ResourceType: "aws:rds:db", Description: "RDS DB instance"},
		{ResourceType: "aws:rds:cluster", Description: "RDS Aurora DB cluster"},
		{ResourceType: "aws:ecs:task", Description: "ECS task"},
		{ResourceType: "aws:kinesis:stream", Description: "Kinesis data stream"},
		{ResourceType: "aws:dynamodb:global-table", Description: "DynamoDB global table"},
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
	svcName := action.Parameters["service"]
	if svcName == "" {
		return nil
	}

	pct := parsePercentage(action.Parameters["percentage"])
	faultErr := faultErrorForAction(action.ActionID)
	errCopy := faultErr

	ops := parseOperations(action.Parameters["operations"])

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
//nolint:gocognit,cyclop // ISO 8601 parsing inherently requires complex character-by-character logic
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

			switch ch {
			case 'H':
				total += time.Duration(val * float64(time.Hour))
			case 'M':
				if inTime {
					total += time.Duration(val * float64(time.Minute))
				} else {
					// 'M' before 'T' means months — not representable as time.Duration;
					// treat as 30 days for approximation.
					total += time.Duration(val * daysPerMonth * float64(hoursPerDay*time.Hour))
				}
			case 'S':
				total += time.Duration(val * float64(time.Second))
			case 'D':
				total += time.Duration(val * float64(hoursPerDay*time.Hour))
			case 'W':
				total += time.Duration(val * daysPerWeek * float64(hoursPerDay*time.Hour))
			}
		}
	}

	return total
}
