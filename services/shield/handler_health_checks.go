package shield

import (
	"encoding/json"
	"fmt"
	"strings"
)

// validateHealthCheckARN checks that the ARN is a Route 53 health check ARN.
func validateHealthCheckARN(arn string) error {
	if !strings.HasPrefix(arn, "arn:aws:route53:::healthcheck/") {
		return fmt.Errorf(
			"%w: HealthCheckArn %q must be a Route 53 health check ARN (arn:aws:route53:::healthcheck/<id>)",
			errInvalidRequest,
			arn,
		)
	}

	return nil
}

// associateHealthCheckRequest is the request body for AssociateHealthCheck.
type associateHealthCheckRequest struct {
	ProtectionID   string `json:"ProtectionId"`
	HealthCheckArn string `json:"HealthCheckArn"`
}

func (h *Handler) handleAssociateHealthCheck(body []byte) error {
	var req associateHealthCheckRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if req.HealthCheckArn == "" {
		return fmt.Errorf("%w: HealthCheckArn is required", errInvalidRequest)
	}

	if err := validateHealthCheckARN(req.HealthCheckArn); err != nil {
		return err
	}

	return h.Backend.AssociateHealthCheck(req.ProtectionID, req.HealthCheckArn)
}

// disassociateHealthCheckRequest is the request body for DisassociateHealthCheck.
type disassociateHealthCheckRequest struct {
	ProtectionID   string `json:"ProtectionId"`
	HealthCheckArn string `json:"HealthCheckArn"`
}

func (h *Handler) handleDisassociateHealthCheck(body []byte) error {
	var req disassociateHealthCheckRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if req.HealthCheckArn == "" {
		return fmt.Errorf("%w: HealthCheckArn is required", errInvalidRequest)
	}

	return h.Backend.DisassociateHealthCheck(req.ProtectionID, req.HealthCheckArn)
}
