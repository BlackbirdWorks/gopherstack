package rds

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// FISActions returns the FIS action definitions that the RDS service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:rds:reboot-db-instances",
			Description: "Reboot target RDS DB instances",
			TargetType:  "aws:rds:db",
		},
		{
			ActionID:    "aws:rds:failover-db-cluster",
			Description: "Trigger a failover for the target RDS Aurora DB cluster",
			TargetType:  "aws:rds:cluster",
			Parameters: []service.FISParamDef{
				{
					Name:        "duration",
					Description: "ISO 8601 duration the failover simulation remains active (e.g. PT5M)",
					Required:    false,
				},
			},
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved RDS targets.
func (h *Handler) ExecuteFISAction(ctx context.Context, action service.FISActionExecution) error {
	switch action.ActionID {
	case "aws:rds:reboot-db-instances":
		return h.fisRebootDBInstances(action.Targets)
	case "aws:rds:failover-db-cluster":
		return h.fisFailoverDBClusters(ctx, action.Targets, action.Duration)
	}

	return nil
}

// fisRebootDBInstances reboots the given DB instances identified by ARN or bare identifier.
func (h *Handler) fisRebootDBInstances(targets []string) error {
	var errs []string

	for _, t := range targets {
		id := rdsIDFromARN(t)

		if _, err := h.Backend.RebootDBInstance(id); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %s", ErrRebootFailed, strings.Join(errs, "; "))
	}

	return nil
}
