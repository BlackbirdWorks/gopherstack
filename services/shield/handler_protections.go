package shield

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// protectedARNEntry describes an ARN service/resource fragment for Shield-supported resource types.
type protectedARNEntry struct {
	service  string
	resource string
}

// shieldSupportedARNs returns the ARN patterns for Shield-supported resource types.
func shieldSupportedARNs() []protectedARNEntry {
	return []protectedARNEntry{
		{"cloudfront", ""},
		{"route53", "hostedzone"},
		{"elasticloadbalancing", "loadbalancer/app/"},
		{"elasticloadbalancing", "loadbalancer/"},
		{"ec2", "eip"},
		{"globalaccelerator", ""},
	}
}

// validateProtectedResourceARN checks that the ARN refers to a Shield-supported resource type.
func validateProtectedResourceARN(arn string) error {
	lower := strings.ToLower(arn)

	for _, entry := range shieldSupportedARNs() {
		if strings.Contains(lower, entry.service) {
			if entry.resource == "" || strings.Contains(lower, strings.ToLower(entry.resource)) {
				return nil
			}
		}
	}

	return fmt.Errorf(
		"%w: ResourceArn %q does not refer to a Shield-supported resource type "+
			"(CloudFront, Route 53 Hosted Zone, ALB, CLB, EIP, Global Accelerator)",
		errInvalidRequest,
		arn,
	)
}

// createProtectionRequest is the request body for CreateProtection.
type createProtectionRequest struct {
	Name        string    `json:"Name"`
	ResourceArn string    `json:"ResourceArn"`
	Tags        []tags.KV `json:"Tags"`
}

func (h *Handler) handleCreateProtection(ctx context.Context, body []byte) ([]byte, error) {
	var req createProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := validateProtectedResourceARN(req.ResourceArn); err != nil {
		return nil, err
	}

	tags := tags.MapFromKV(req.Tags)

	p, err := h.Backend.CreateProtection(req.Name, req.ResourceArn, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: created protection", "name", p.Name, "id", p.ID)

	return json.Marshal(map[string]string{
		"ProtectionId": p.ID,
	})
}

// describeProtectionRequest is the request body for DescribeProtection.
type describeProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
	ResourceArn  string `json:"ResourceArn"`
}

func (h *Handler) handleDescribeProtection(body []byte) ([]byte, error) {
	var req describeProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" && req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ProtectionId or ResourceArn is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProtection(req.ProtectionID, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	alarCfg := h.Backend.GetALARConfig(p.ResourceARN)

	return json.Marshal(map[string]any{
		"Protection": protectionToMap(p, alarCfg),
	})
}

// deleteProtectionRequest is the request body for DeleteProtection.
type deleteProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
}

func (h *Handler) handleDeleteProtection(ctx context.Context, body []byte) error {
	var req deleteProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProtection(req.ProtectionID); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: deleted protection", "id", req.ProtectionID)

	return nil
}

// listProtectionsRequest is the request body for ListProtections.
type listProtectionsRequest struct {
	InclusionFilters *struct {
		ResourceArns    []string `json:"ResourceArns"`
		ProtectionNames []string `json:"ProtectionNames"`
		ResourceTypes   []string `json:"ResourceTypes"`
	} `json:"InclusionFilters,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListProtections(body []byte) ([]byte, error) {
	var req listProtectionsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	protections := h.Backend.ListProtections()

	if f := req.InclusionFilters; f != nil {
		protections = applyProtectionFilters(
			protections,
			sliceToSet(f.ResourceArns),
			sliceToSet(f.ProtectionNames),
			sliceToSet(f.ResourceTypes),
		)
	}

	maxResults := clampMaxResults(req.MaxResults, maxProtectionsPerPage)

	start, err := decodeOffsetToken(req.NextToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
	}

	if start >= len(protections) {
		return json.Marshal(map[string]any{"Protections": []map[string]any{}})
	}

	end := start + maxResults

	var nextToken string

	if end < len(protections) {
		nextToken = encodeOffsetToken(end)
		protections = protections[start:end]
	} else {
		protections = protections[start:]
	}

	items := make([]map[string]any, 0, len(protections))

	for _, p := range protections {
		alarCfg := h.Backend.GetALARConfig(p.ResourceARN)
		items = append(items, protectionToMap(p, alarCfg))
	}

	resp := map[string]any{"Protections": items}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// protectionMatchesFilters returns true if p passes all inclusion filter sets.
func protectionMatchesFilters(
	p *Protection,
	arnSet, nameSet, typeSet map[string]struct{},
) bool {
	if len(arnSet) > 0 {
		if _, ok := arnSet[p.ResourceARN]; !ok {
			return false
		}
	}

	if len(nameSet) > 0 {
		if _, ok := nameSet[p.Name]; !ok {
			return false
		}
	}

	if len(typeSet) > 0 {
		for rt := range typeSet {
			if resourceARNMatchesType(p.ResourceARN, rt) {
				return true
			}
		}

		return false
	}

	return true
}

// applyProtectionFilters filters protections by the given inclusion filter sets.
func applyProtectionFilters(
	protections []*Protection,
	arnSet, nameSet, typeSet map[string]struct{},
) []*Protection {
	if len(arnSet) == 0 && len(nameSet) == 0 && len(typeSet) == 0 {
		return protections
	}

	out := make([]*Protection, 0, len(protections))

	for _, p := range protections {
		if protectionMatchesFilters(p, arnSet, nameSet, typeSet) {
			out = append(out, p)
		}
	}

	return out
}

func protectionToMap(p *Protection, alarCfg *ALARConfig) map[string]any {
	healthChecks := p.HealthCheckIDs
	if healthChecks == nil {
		healthChecks = []string{}
	}

	m := map[string]any{
		"Id":             p.ID,
		"ProtectionArn":  p.ProtectionArn,
		"Name":           p.Name,
		keyResourceArn:   p.ResourceARN,
		"HealthCheckIds": healthChecks,
		"CreationTime":   floatSeconds(p.CreationTime),
	}

	// Gap 4: include ALAR config when present.
	if alarCfg != nil {
		status := "DISABLED"
		if alarCfg.Enabled {
			status = "ENABLED"
		}

		action := map[string]any{}
		if alarCfg.Action == "BLOCK" {
			action["Block"] = map[string]any{}
		} else {
			action["Count"] = map[string]any{}
		}

		m["ApplicationLayerAutomaticResponseConfiguration"] = map[string]any{
			"Status": status,
			"Action": action,
		}
	}

	return m
}
