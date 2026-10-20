package asl

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

const (
	statesIntegrationPrefix = "arn:aws:states:::"
	serviceStates           = "states"
	arnFieldsWithRegion     = 6
)

// taskSpanInfo names the downstream call a Task state's Resource resolves to.
func taskSpanInfo(resource string) apptrace.ChildInfo {
	info := apptrace.ChildInfo{Service: serviceStates, Operation: "Task", Resource: resource}

	if rest, ok := strings.CutPrefix(resource, statesIntegrationPrefix); ok {
		parts := strings.Split(rest, ":")
		if parts[0] == "aws-sdk" && len(parts) >= 3 {
			parts = parts[1:]
		}

		info.Service = parts[0]
		if len(parts) > 1 {
			op, _, _ := strings.Cut(parts[1], ".")
			info.Operation = upperFirst(op)
		}

		return info
	}

	fields := strings.SplitN(resource, ":", arnFieldsWithRegion)
	if len(fields) >= 4 && fields[0] == "arn" {
		info.Service, info.Region = fields[2], fields[3]
		if fields[2] == "lambda" {
			info.Operation = "Invoke"
		}
	}

	return info
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}
