package networkfirewall

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	flowFlush   = "FLOW_FLUSH"
	flowCapture = "FLOW_CAPTURE"

	flowInProgress = "IN_PROGRESS"
	flowCompleted  = "COMPLETED"
)

// FlowFilter selects the flows a flow operation applies to.
type FlowFilter struct {
	DestinationAddress *Address `json:"DestinationAddress,omitempty"`
	SourceAddress      *Address `json:"SourceAddress,omitempty"`
	DestinationPort    string   `json:"DestinationPort,omitempty"`
	SourcePort         string   `json:"SourcePort,omitempty"`
	Protocols          []string `json:"Protocols,omitempty"`
}

// Flow is one captured flow.
type Flow struct {
	DestinationAddress *Address `json:"DestinationAddress,omitempty"`
	SourceAddress      *Address `json:"SourceAddress,omitempty"`
	Age                *int32   `json:"Age,omitempty"`
	PacketCount        *int32   `json:"PacketCount,omitempty"`
	DestinationPort    string   `json:"DestinationPort,omitempty"`
	SourcePort         string   `json:"SourcePort,omitempty"`
	Protocol           string   `json:"Protocol,omitempty"`
	ByteCount          int64    `json:"ByteCount"`
}

// FlowOperation is the filter set a flow operation was started with.
type FlowOperation struct {
	MinimumFlowAgeInSeconds *int32       `json:"MinimumFlowAgeInSeconds,omitempty"`
	FlowFilters             []FlowFilter `json:"FlowFilters"`
}

// FlowOperationMetadata is a ListFlowOperations entry.
type FlowOperationMetadata struct {
	FlowRequestTimestamp *float64 `json:"FlowRequestTimestamp,omitempty"`
	FlowOperationID      string   `json:"FlowOperationId"`
	FlowOperationStatus  string   `json:"FlowOperationStatus"`
	FlowOperationType    string   `json:"FlowOperationType"`
}

// flowTarget identifies the firewall endpoint a flow call addresses.
type flowTarget struct {
	FirewallArn               string `json:"FirewallArn"`
	AvailabilityZone          string `json:"AvailabilityZone"`
	VpcEndpointAssociationArn string `json:"VpcEndpointAssociationArn"`
	VpcEndpointID             string `json:"VpcEndpointId"`
}

// StartFlowInput is the StartFlowCapture and StartFlowFlush request.
type StartFlowInput struct {
	flowTarget
	MinimumFlowAgeInSeconds *int32       `json:"MinimumFlowAgeInSeconds"`
	FlowFilters             []FlowFilter `json:"FlowFilters"`
}

// StartFlowOutput is the StartFlowCapture and StartFlowFlush response.
type StartFlowOutput struct {
	FirewallArn         string `json:"FirewallArn"`
	FlowOperationID     string `json:"FlowOperationId"`
	FlowOperationStatus string `json:"FlowOperationStatus"`
}

// DescribeFlowOperationInput is the DescribeFlowOperation request.
type DescribeFlowOperationInput struct {
	flowTarget
	FlowOperationID string `json:"FlowOperationId"`
}

// DescribeFlowOperationOutput is the DescribeFlowOperation response.
type DescribeFlowOperationOutput struct {
	FlowRequestTimestamp      *float64      `json:"FlowRequestTimestamp,omitempty"`
	AvailabilityZone          string        `json:"AvailabilityZone,omitempty"`
	FirewallArn               string        `json:"FirewallArn"`
	FlowOperationID           string        `json:"FlowOperationId"`
	FlowOperationStatus       string        `json:"FlowOperationStatus"`
	FlowOperationType         string        `json:"FlowOperationType"`
	VpcEndpointAssociationArn string        `json:"VpcEndpointAssociationArn,omitempty"`
	VpcEndpointID             string        `json:"VpcEndpointId,omitempty"`
	StatusMessage             string        `json:"StatusMessage,omitempty"`
	FlowOperation             FlowOperation `json:"FlowOperation"`
}

// ListFlowOperationsInput is the ListFlowOperations request.
type ListFlowOperationsInput struct {
	flowTarget
	MaxResults        *int32 `json:"MaxResults"`
	FlowOperationType string `json:"FlowOperationType"`
	NextToken         string `json:"NextToken"`
}

// ListFlowOperationsOutput is the ListFlowOperations response.
type ListFlowOperationsOutput struct {
	NextToken      string                  `json:"NextToken,omitempty"`
	FlowOperations []FlowOperationMetadata `json:"FlowOperations"`
}

// ListFlowOperationResultsInput is the ListFlowOperationResults request.
type ListFlowOperationResultsInput struct {
	flowTarget
	MaxResults      *int32 `json:"MaxResults"`
	FlowOperationID string `json:"FlowOperationId"`
	NextToken       string `json:"NextToken"`
}

// ListFlowOperationResultsOutput is the ListFlowOperationResults response.
type ListFlowOperationResultsOutput struct {
	FlowRequestTimestamp      *float64 `json:"FlowRequestTimestamp,omitempty"`
	AvailabilityZone          string   `json:"AvailabilityZone,omitempty"`
	FirewallArn               string   `json:"FirewallArn"`
	FlowOperationID           string   `json:"FlowOperationId"`
	FlowOperationStatus       string   `json:"FlowOperationStatus"`
	NextToken                 string   `json:"NextToken,omitempty"`
	StatusMessage             string   `json:"StatusMessage,omitempty"`
	VpcEndpointAssociationArn string   `json:"VpcEndpointAssociationArn,omitempty"`
	VpcEndpointID             string   `json:"VpcEndpointId,omitempty"`
	Flows                     []Flow   `json:"Flows"`
}

func flowKey(firewallARN, id string) string { return firewallARN + "#" + id }

func (r *flowOpRecord) status(now time.Time) string {
	if now.Before(r.DoneAt) {
		return flowInProgress
	}

	return flowCompleted
}

func (r *flowOpRecord) matches(t flowTarget) bool {
	return (t.AvailabilityZone == "" || t.AvailabilityZone == r.Zone) &&
		(t.VpcEndpointAssociationArn == "" || t.VpcEndpointAssociationArn == r.AssocARN) &&
		(t.VpcEndpointID == "" || t.VpcEndpointID == r.EndpointID)
}

// validateFlowTargetLocked checks the endpoint a flow call names belongs to the firewall.
func (b *InMemoryBackend) validateFlowTargetLocked(fw *firewallRecord, t flowTarget, now time.Time) error {
	switch {
	case t.VpcEndpointAssociationArn != "":
		assoc, err := b.findVpcEndpointLocked(t.VpcEndpointAssociationArn, now)
		if err != nil {
			return err
		}

		if assoc.FirewallARN != fw.ARN || (t.VpcEndpointID != "" && t.VpcEndpointID != assoc.EndpointID) {
			return invalid("VpcEndpointAssociationArn does not belong to firewall %s", fw.Name)
		}
	case t.AvailabilityZone != "":
		if !slices.Contains(slices.Collect(maps.Values(fw.Zones)), t.AvailabilityZone) {
			return invalid("Availability Zone %s is not used by firewall %s", t.AvailabilityZone, fw.Name)
		}
	case t.VpcEndpointID != "":
		if !slices.Contains(endpointIDs(fw), t.VpcEndpointID) {
			return invalid("VpcEndpointId %s does not belong to firewall %s", t.VpcEndpointID, fw.Name)
		}
	default:
		return invalid("One of AvailabilityZone, VpcEndpointId or VpcEndpointAssociationArn is required")
	}

	return nil
}

func endpointIDs(r *firewallRecord) []string { return slices.Collect(maps.Values(r.Endpoints)) }

func validateFlowFilters(filters []FlowFilter, minAge *int32) error {
	if filters == nil {
		return invalid("FlowFilters is required")
	}

	if minAge != nil && *minAge < 0 {
		return invalid("MinimumFlowAgeInSeconds must not be negative")
	}

	return nil
}

func (b *InMemoryBackend) startFlow(op, flowType string, in *StartFlowInput) (*StartFlowOutput, error) {
	if in.FirewallArn == "" {
		return nil, invalid("FirewallArn is required")
	}

	if err := validateFlowFilters(in.FlowFilters, in.MinimumFlowAgeInSeconds); err != nil {
		return nil, err
	}

	b.mu.Lock(op)
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	fw, err := b.findFirewallLocked(in.FirewallArn, "", now)
	if err != nil {
		return nil, err
	}

	if err = b.validateFlowTargetLocked(fw, in.flowTarget, now); err != nil {
		return nil, err
	}

	r := &flowOpRecord{
		FirewallARN: fw.ARN,
		ID:          awsid.New("flow-op-"),
		Type:        flowType,
		Zone:        in.AvailabilityZone,
		AssocARN:    in.VpcEndpointAssociationArn,
		EndpointID:  in.VpcEndpointID,
		Filters:     slices.Clone(in.FlowFilters),
		MinAge:      in.MinimumFlowAgeInSeconds,
		Requested:   now,
		DoneAt:      b.deadline(now),
	}
	r.Key = flowKey(fw.ARN, r.ID)

	b.flowOps.Put(r)

	return &StartFlowOutput{FirewallArn: fw.ARN, FlowOperationID: r.ID, FlowOperationStatus: r.status(now)}, nil
}

// StartFlowCapture starts capturing the flows that match the filters.
func (b *InMemoryBackend) StartFlowCapture(_ context.Context, in *StartFlowInput) (*StartFlowOutput, error) {
	return b.startFlow("StartFlowCapture", flowCapture, in)
}

// StartFlowFlush starts flushing the flows that match the filters.
func (b *InMemoryBackend) StartFlowFlush(_ context.Context, in *StartFlowInput) (*StartFlowOutput, error) {
	return b.startFlow("StartFlowFlush", flowFlush, in)
}

func (b *InMemoryBackend) findFlowOpLocked(firewallARN, id string, t flowTarget, now time.Time) (*flowOpRecord, error) {
	if firewallARN == "" || id == "" {
		return nil, invalid("FirewallArn and FlowOperationId are required")
	}

	if _, err := b.findFirewallLocked(firewallARN, "", now); err != nil {
		return nil, err
	}

	r, err := getOrNotFound(b.flowOps, "flow operation", flowKey(firewallARN, id))
	if err != nil {
		return nil, err
	}

	if !r.matches(t) {
		return nil, notFound("flow operation", id)
	}

	return r, nil
}

// DescribeFlowOperation returns the status of a flow operation.
func (b *InMemoryBackend) DescribeFlowOperation(
	_ context.Context, in *DescribeFlowOperationInput,
) (*DescribeFlowOperationOutput, error) {
	b.mu.RLock("DescribeFlowOperation")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findFlowOpLocked(in.FirewallArn, in.FlowOperationID, in.flowTarget, now)
	if err != nil {
		return nil, err
	}

	return &DescribeFlowOperationOutput{
		FlowOperation: FlowOperation{
			FlowFilters:             slices.Clone(r.Filters),
			MinimumFlowAgeInSeconds: r.MinAge,
		},
		FlowRequestTimestamp:      epoch(r.Requested),
		AvailabilityZone:          r.Zone,
		FirewallArn:               r.FirewallARN,
		FlowOperationID:           r.ID,
		FlowOperationStatus:       r.status(now),
		FlowOperationType:         r.Type,
		VpcEndpointAssociationArn: r.AssocARN,
		VpcEndpointID:             r.EndpointID,
	}, nil
}

// ListFlowOperations lists the flow operations of a firewall.
func (b *InMemoryBackend) ListFlowOperations(
	_ context.Context, in *ListFlowOperationsInput,
) (*ListFlowOperationsOutput, error) {
	if in.FirewallArn == "" {
		return nil, invalid("FirewallArn is required")
	}

	if in.FlowOperationType != "" && in.FlowOperationType != flowCapture && in.FlowOperationType != flowFlush {
		return nil, invalid("FlowOperationType %q is not valid", in.FlowOperationType)
	}

	b.mu.RLock("ListFlowOperations")
	defer b.mu.RUnlock()

	now := time.Now()

	if _, err := b.findFirewallLocked(in.FirewallArn, "", now); err != nil {
		return nil, err
	}

	ops := slices.DeleteFunc(b.flowOps.All(), func(r *flowOpRecord) bool {
		return r.FirewallARN != in.FirewallArn || !r.matches(in.flowTarget) ||
			(in.FlowOperationType != "" && r.Type != in.FlowOperationType)
	})
	slices.SortFunc(ops, func(a, c *flowOpRecord) int {
		return strings.Compare(a.Requested.Format(time.RFC3339Nano)+a.ID, c.Requested.Format(time.RFC3339Nano)+c.ID)
	})

	all := make([]FlowOperationMetadata, 0, len(ops))
	for _, r := range ops {
		all = append(all, FlowOperationMetadata{
			FlowOperationID: r.ID, FlowOperationStatus: r.status(now), FlowOperationType: r.Type,
			FlowRequestTimestamp: epoch(r.Requested),
		})
	}

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	if pg.Data == nil {
		pg.Data = []FlowOperationMetadata{}
	}

	return &ListFlowOperationsOutput{FlowOperations: pg.Data, NextToken: pg.Next}, nil
}

// ListFlowOperationResults returns the flows a capture found; no traffic crosses the emulator, so none.
func (b *InMemoryBackend) ListFlowOperationResults(
	_ context.Context, in *ListFlowOperationResultsInput,
) (*ListFlowOperationResultsOutput, error) {
	b.mu.RLock("ListFlowOperationResults")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findFlowOpLocked(in.FirewallArn, in.FlowOperationID, in.flowTarget, now)
	if err != nil {
		return nil, err
	}

	pg, err := paginate([]Flow{}, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListFlowOperationResultsOutput{
		FlowRequestTimestamp:      epoch(r.Requested),
		AvailabilityZone:          r.Zone,
		FirewallArn:               r.FirewallARN,
		FlowOperationID:           r.ID,
		FlowOperationStatus:       r.status(now),
		NextToken:                 pg.Next,
		VpcEndpointAssociationArn: r.AssocARN,
		VpcEndpointID:             r.EndpointID,
		Flows:                     []Flow{},
	}, nil
}
