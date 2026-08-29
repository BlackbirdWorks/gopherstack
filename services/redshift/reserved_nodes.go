package redshift

import (
	"fmt"
	"time"
)

const (
	offeringClassRegular = "Regular"
	currencyUSD          = "USD"
	reservedAllUpfront   = "All Upfront"
	nodeTypeDC28xlarge   = "dc2.8xlarge"
)

// Duration constants for reserved node offerings (in seconds).
const (
	durationOneYearSec   = 31536000 // 365 * 24 * 60 * 60
	durationThreeYearSec = 94608000 // 3 * 365 * 24 * 60 * 60
)

// Price constants for reserved node offerings (in USD).
const (
	priceDC2LargeAllUpfront   = 1000.00
	priceDC2LargeNoUpfront    = 0.10
	priceDC28XLargeAllUpfront = 20000.00
	priceRA3XLPlusAllUpfront  = 5000.00
	priceRA34XLargeAllUpfront = 40000.00
	priceZeroUpfront          = 0.00
)

// ReservedNodeOffering represents an available Redshift reserved node offering.
type ReservedNodeOffering struct {
	ReservedNodeOfferingID   string  `json:"reservedNodeOfferingId"`
	ReservedNodeOfferingType string  `json:"reservedNodeOfferingType"`
	NodeType                 string  `json:"nodeType"`
	CurrencyCode             string  `json:"currencyCode"`
	OfferingType             string  `json:"offeringType"`
	Duration                 int     `json:"duration"`
	FixedPrice               float64 `json:"fixedPrice"`
	UsagePrice               float64 `json:"usagePrice"`
}

// defaultReservedNodeOfferings returns a curated set of built-in reserved node offerings.
func defaultReservedNodeOfferings() []*ReservedNodeOffering {
	return []*ReservedNodeOffering{
		{
			ReservedNodeOfferingID:   "offering-dc2-large-1yr-allupfront",
			ReservedNodeOfferingType: offeringClassRegular,
			NodeType:                 defaultNodeType,
			Duration:                 durationOneYearSec,
			FixedPrice:               priceDC2LargeAllUpfront,
			UsagePrice:               priceZeroUpfront,
			CurrencyCode:             currencyUSD,
			OfferingType:             reservedAllUpfront,
		},
		{
			ReservedNodeOfferingID:   "offering-dc2-large-1yr-noupfront",
			ReservedNodeOfferingType: offeringClassRegular,
			NodeType:                 defaultNodeType,
			Duration:                 durationOneYearSec,
			FixedPrice:               priceZeroUpfront,
			UsagePrice:               priceDC2LargeNoUpfront,
			CurrencyCode:             currencyUSD,
			OfferingType:             "No Upfront",
		},
		{
			ReservedNodeOfferingID:   "offering-dc2-8xlarge-1yr-allupfront",
			ReservedNodeOfferingType: offeringClassRegular,
			NodeType:                 nodeTypeDC28xlarge,
			Duration:                 durationOneYearSec,
			FixedPrice:               priceDC28XLargeAllUpfront,
			UsagePrice:               priceZeroUpfront,
			CurrencyCode:             currencyUSD,
			OfferingType:             reservedAllUpfront,
		},
		{
			ReservedNodeOfferingID:   "offering-ra3-xlplus-1yr-allupfront",
			ReservedNodeOfferingType: offeringClassRegular,
			NodeType:                 "ra3.xlplus",
			Duration:                 durationOneYearSec,
			FixedPrice:               priceRA3XLPlusAllUpfront,
			UsagePrice:               priceZeroUpfront,
			CurrencyCode:             currencyUSD,
			OfferingType:             reservedAllUpfront,
		},
		{
			ReservedNodeOfferingID:   "offering-ra3-4xlarge-3yr-allupfront",
			ReservedNodeOfferingType: offeringClassRegular,
			NodeType:                 "ra3.4xlarge",
			Duration:                 durationThreeYearSec,
			FixedPrice:               priceRA34XLargeAllUpfront,
			UsagePrice:               priceZeroUpfront,
			CurrencyCode:             currencyUSD,
			OfferingType:             reservedAllUpfront,
		},
	}
}

// DescribeReservedNodes returns reserved nodes, optionally filtered by reservedNodeID.
func (b *InMemoryBackend) DescribeReservedNodes(reservedNodeID string) ([]ReservedNode, error) {
	b.mu.RLock("DescribeReservedNodes")
	defer b.mu.RUnlock()

	if reservedNodeID != "" {
		node, exists := b.reservedNodes.Get(reservedNodeID)
		if !exists {
			return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
		}

		cp := *node

		return []ReservedNode{cp}, nil
	}

	result := make([]ReservedNode, 0, b.reservedNodes.Len())
	for _, node := range b.reservedNodes.All() {
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

	if _, exists := b.reservedNodes.Get(reservedNodeID); exists {
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
	b.reservedNodes.Put(node)

	cp := *node

	return &cp, nil
}

// reservedNodeExchangeStatusSucceeded is this backend's placeholder exchange
// status: ReservedNodeExchangeStatus.Status is types.ReservedNodeExchangeStatusType
// (REQUESTED/PENDING/IN_PROGRESS/RETRYING/SUCCEEDED/FAILED -- redshift@v1.65.4
// types/enums.go:468), and since this backend has no async exchange pipeline
// to simulate, SUCCEEDED is the honest terminal value rather than a
// fabricated in-progress state.
const reservedNodeExchangeStatusSucceeded = "SUCCEEDED"

// DescribeReservedNodeExchangeStatus returns the exchange status for a reserved node.
// In this in-memory implementation it returns a placeholder completed status.
func (b *InMemoryBackend) DescribeReservedNodeExchangeStatus(reservedNodeID string) (string, error) {
	if reservedNodeID == "" {
		return "", fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeReservedNodeExchangeStatus")
	defer b.mu.RUnlock()

	if _, exists := b.reservedNodes.Get(reservedNodeID); !exists {
		return "", fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	return reservedNodeExchangeStatusSucceeded, nil
}

// GetReservedNodeExchangeOfferings returns offerings available for exchange of a reserved node.
func (b *InMemoryBackend) GetReservedNodeExchangeOfferings(reservedNodeID string) ([]ReservedNodeOffering, error) {
	if reservedNodeID == "" {
		return nil, fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetReservedNodeExchangeOfferings")
	defer b.mu.RUnlock()

	if _, exists := b.reservedNodes.Get(reservedNodeID); !exists {
		return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	offerings := defaultReservedNodeOfferings()
	result := make([]ReservedNodeOffering, 0, len(offerings))

	for _, o := range offerings {
		result = append(result, *o)
	}

	return result, nil
}

// AcceptReservedNodeExchange exchanges an existing reserved node for a new offering.
func (b *InMemoryBackend) AcceptReservedNodeExchange(reservedNodeID, targetOfferingID string) (*ReservedNode, error) {
	if reservedNodeID == "" {
		return nil, fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}
	if targetOfferingID == "" {
		return nil, fmt.Errorf("%w: TargetReservedNodeOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptReservedNodeExchange")
	defer b.mu.Unlock()

	existing, exists := b.reservedNodes.Get(reservedNodeID)
	if !exists {
		return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	exchanged := &ReservedNode{
		ReservedNodeID:         existing.ReservedNodeID,
		ReservedNodeOfferingID: targetOfferingID,
		NodeType:               existing.NodeType,
		StartTime:              time.Now(),
		Duration:               existing.Duration,
		FixedPrice:             existing.FixedPrice,
		UsagePrice:             existing.UsagePrice,
		CurrencyCode:           existing.CurrencyCode,
		NodeCount:              existing.NodeCount,
		State:                  endpointStatusActive,
		OfferingType:           existing.OfferingType,
	}
	b.reservedNodes.Put(exchanged)

	cp := *exchanged

	return &cp, nil
}

// AddReservedNodeInternal seeds a reserved node directly into the backend.
func (b *InMemoryBackend) AddReservedNodeInternal(node *ReservedNode) {
	b.mu.Lock("AddReservedNodeInternal")
	defer b.mu.Unlock()
	b.reservedNodes.Put(node)
}
