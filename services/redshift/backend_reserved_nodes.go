package redshift

import (
	"fmt"
	"time"
)

// ReservedNodeOffering represents an available Redshift reserved node offering.
type ReservedNodeOffering struct {
	ReservedNodeOfferingID   string  `json:"reservedNodeOfferingId"`
	ReservedNodeOfferingType string  `json:"reservedNodeOfferingType"`
	NodeType                 string  `json:"nodeType"`
	Duration                 int     `json:"duration"`
	FixedPrice               float64 `json:"fixedPrice"`
	UsagePrice               float64 `json:"usagePrice"`
	CurrencyCode             string  `json:"currencyCode"`
	OfferingType             string  `json:"offeringType"`
}

// defaultReservedNodeOfferings returns a curated set of built-in reserved node offerings.
func defaultReservedNodeOfferings() []*ReservedNodeOffering {
	return []*ReservedNodeOffering{
		{
			ReservedNodeOfferingID:   "offering-dc2-large-1yr-allupfront",
			ReservedNodeOfferingType: "Regular",
			NodeType:                 "dc2.large",
			Duration:                 31536000, // 1 year in seconds
			FixedPrice:               1000.00,
			UsagePrice:               0.00,
			CurrencyCode:             "USD",
			OfferingType:             "All Upfront",
		},
		{
			ReservedNodeOfferingID:   "offering-dc2-large-1yr-noupfront",
			ReservedNodeOfferingType: "Regular",
			NodeType:                 "dc2.large",
			Duration:                 31536000,
			FixedPrice:               0.00,
			UsagePrice:               0.10,
			CurrencyCode:             "USD",
			OfferingType:             "No Upfront",
		},
		{
			ReservedNodeOfferingID:   "offering-dc2-8xlarge-1yr-allupfront",
			ReservedNodeOfferingType: "Regular",
			NodeType:                 "dc2.8xlarge",
			Duration:                 31536000,
			FixedPrice:               20000.00,
			UsagePrice:               0.00,
			CurrencyCode:             "USD",
			OfferingType:             "All Upfront",
		},
		{
			ReservedNodeOfferingID:   "offering-ra3-xlplus-1yr-allupfront",
			ReservedNodeOfferingType: "Regular",
			NodeType:                 "ra3.xlplus",
			Duration:                 31536000,
			FixedPrice:               5000.00,
			UsagePrice:               0.00,
			CurrencyCode:             "USD",
			OfferingType:             "All Upfront",
		},
		{
			ReservedNodeOfferingID:   "offering-ra3-4xlarge-3yr-allupfront",
			ReservedNodeOfferingType: "Regular",
			NodeType:                 "ra3.4xlarge",
			Duration:                 94608000, // 3 years in seconds
			FixedPrice:               40000.00,
			UsagePrice:               0.00,
			CurrencyCode:             "USD",
			OfferingType:             "All Upfront",
		},
	}
}

// DescribeReservedNodes returns reserved nodes, optionally filtered by reservedNodeID.
func (b *InMemoryBackend) DescribeReservedNodes(reservedNodeID string) ([]ReservedNode, error) {
	b.mu.RLock("DescribeReservedNodes")
	defer b.mu.RUnlock()

	if reservedNodeID != "" {
		node, exists := b.reservedNodes[reservedNodeID]
		if !exists {
			return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
		}

		cp := *node

		return []ReservedNode{cp}, nil
	}

	result := make([]ReservedNode, 0, len(b.reservedNodes))
	for _, node := range b.reservedNodes {
		result = append(result, *node)
	}

	return result, nil
}

// DescribeReservedNodeOfferings returns available reserved node offerings, optionally filtered by offeringID.
func (b *InMemoryBackend) DescribeReservedNodeOfferings(offeringID string) ([]ReservedNodeOffering, error) {
	offerings := defaultReservedNodeOfferings()

	if offeringID != "" {
		for _, o := range offerings {
			if o.ReservedNodeOfferingID == offeringID {
				return []ReservedNodeOffering{*o}, nil
			}
		}

		return nil, fmt.Errorf("%w: reserved node offering %s not found", ErrReservedNodeOfferingNotFound, offeringID)
	}

	result := make([]ReservedNodeOffering, 0, len(offerings))
	for _, o := range offerings {
		result = append(result, *o)
	}

	return result, nil
}

// PurchaseReservedNodeOffering creates a new reserved node from an offering.
func (b *InMemoryBackend) PurchaseReservedNodeOffering(
	offeringID, reservedNodeID string,
	nodeCount int,
) (*ReservedNode, error) {
	if offeringID == "" {
		return nil, fmt.Errorf("%w: ReservedNodeOfferingId is required", ErrInvalidParameter)
	}

	offerings := defaultReservedNodeOfferings()

	var offering *ReservedNodeOffering

	for _, o := range offerings {
		if o.ReservedNodeOfferingID == offeringID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf("%w: reserved node offering %s not found", ErrReservedNodeOfferingNotFound, offeringID)
	}

	if nodeCount <= 0 {
		nodeCount = 1
	}

	if reservedNodeID == "" {
		reservedNodeID = fmt.Sprintf("rn-%d", time.Now().UnixNano())
	}

	b.mu.Lock("PurchaseReservedNodeOffering")
	defer b.mu.Unlock()

	if _, exists := b.reservedNodes[reservedNodeID]; exists {
		return nil, fmt.Errorf("%w: reserved node %s already exists", ErrReservedNodeAlreadyExists, reservedNodeID)
	}

	node := &ReservedNode{
		ReservedNodeID:         reservedNodeID,
		ReservedNodeOfferingID: offeringID,
		NodeType:               offering.NodeType,
		StartTime:              time.Now(),
		Duration:               offering.Duration,
		FixedPrice:             offering.FixedPrice,
		UsagePrice:             offering.UsagePrice,
		CurrencyCode:           offering.CurrencyCode,
		NodeCount:              nodeCount,
		State:                  "payment-pending",
		OfferingType:           offering.OfferingType,
	}
	b.reservedNodes[reservedNodeID] = node

	cp := *node

	return &cp, nil
}

// DescribeReservedNodeExchangeStatus returns the exchange status for a reserved node.
// In this in-memory implementation it returns a placeholder active status.
func (b *InMemoryBackend) DescribeReservedNodeExchangeStatus(reservedNodeID string) (string, error) {
	if reservedNodeID == "" {
		return "", fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeReservedNodeExchangeStatus")
	defer b.mu.RUnlock()

	if _, exists := b.reservedNodes[reservedNodeID]; !exists {
		return "", fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	return "Active", nil
}

// GetReservedNodeExchangeOfferings returns offerings available for exchange of a reserved node.
func (b *InMemoryBackend) GetReservedNodeExchangeOfferings(reservedNodeID string) ([]ReservedNodeOffering, error) {
	if reservedNodeID == "" {
		return nil, fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetReservedNodeExchangeOfferings")
	defer b.mu.RUnlock()

	if _, exists := b.reservedNodes[reservedNodeID]; !exists {
		return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	offerings := defaultReservedNodeOfferings()
	result := make([]ReservedNodeOffering, 0, len(offerings))

	for _, o := range offerings {
		result = append(result, *o)
	}

	return result, nil
}
