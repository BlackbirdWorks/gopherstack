package redshiftdata

import (
	"context"
	"fmt"
)

const workgroupStatusAvailable = "AVAILABLE"

// WorkgroupResolver looks up a Redshift Serverless workgroup's status in a region.
type WorkgroupResolver interface {
	WorkgroupStatus(region, name string) (status string, found bool)
}

type resolverHolder struct{ r WorkgroupResolver }

// SetWorkgroupResolver wires the Redshift Serverless lookup used to validate WorkgroupName.
func (h *Handler) SetWorkgroupResolver(r WorkgroupResolver) {
	h.workgroups.Store(&resolverHolder{r: r})
}

// checkWorkgroup rejects a WorkgroupName that is unknown (ResourceNotFoundException)
// or not AVAILABLE (ValidationException) once a resolver is wired.
func (h *Handler) checkWorkgroup(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}

	holder := h.workgroups.Load()
	if holder == nil || holder.r == nil {
		return nil
	}

	status, found := holder.r.WorkgroupStatus(getRegion(ctx, h.Region), name)
	if !found {
		return fmt.Errorf("%w: Workgroup %s not found", ErrNotFound, name)
	}

	if status != workgroupStatusAvailable {
		return fmt.Errorf("%w: Workgroup %s is %s, not AVAILABLE", ErrValidation, name, status)
	}

	return nil
}
