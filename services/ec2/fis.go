package ec2

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// FISActions returns the FIS action definitions that the EC2 service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:ec2:stop-instances",
			Description: "Stop EC2 instances",
			TargetType:  "aws:ec2:instance",
			Parameters: []service.FISParamDef{
				{
					Name:        "startInstancesAfterDuration",
					Description: "ISO 8601 duration after which to restart the instances (e.g. PT5M)",
					Required:    false,
				},
			},
		},
		{
			ActionID:    "aws:ec2:terminate-instances",
			Description: "Terminate EC2 instances",
			TargetType:  "aws:ec2:instance",
		},
		{
			ActionID:    "aws:ec2:reboot-instances",
			Description: "Reboot EC2 instances",
			TargetType:  "aws:ec2:instance",
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved EC2 targets.
func (h *Handler) ExecuteFISAction(_ context.Context, action service.FISActionExecution) error {
	ids := instanceIDsFromARNs(action.Targets)

	switch action.ActionID {
	case "aws:ec2:stop-instances":
		_, err := h.Backend.StopInstances(ids)

		return err
	case "aws:ec2:terminate-instances":
		_, err := h.Backend.TerminateInstances(ids)

		return err
	case "aws:ec2:reboot-instances":
		return h.Backend.RebootInstances(ids)
	}

	return nil
}

// instanceIDsFromARNs extracts EC2 instance IDs from ARNs.
// ARN format: arn:aws:ec2:{region}:{account}:instance/{id}.
func instanceIDsFromARNs(arns []string) []string {
	ids := make([]string, 0, len(arns))

	for _, a := range arns {
		if idx := strings.LastIndex(a, "/"); idx >= 0 {
			ids = append(ids, a[idx+1:])
		} else {
			// Not an ARN — treat as a bare instance ID.
			ids = append(ids, a)
		}
	}

	return ids
}
