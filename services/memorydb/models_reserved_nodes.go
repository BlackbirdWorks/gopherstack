package memorydb

// ReservedNode represents a reserved MemoryDB node. StartTime is epoch
// seconds (float64), matching the real ReservedNode TStamp shape; this
// struct is serialized directly as the wire response (see
// describeReservedNodesResponse / purchaseReservedNodesOfferingResponse), so
// its field types must match the wire, not just internal convenience.
type ReservedNode struct {
	ReservedNodeID   string                  `json:"ReservedNodeId,omitempty"`
	ReservationID    string                  `json:"ReservationId,omitempty"`
	NodeType         string                  `json:"NodeType,omitempty"`
	OfferingType     string                  `json:"OfferingType,omitempty"`
	State            string                  `json:"State,omitempty"`
	ARN              string                  `json:"ARN,omitempty"`
	RecurringCharges []recurringChargeObject `json:"RecurringCharges,omitempty"`
	FixedPrice       float64                 `json:"FixedPrice,omitempty"`
	UsagePrice       float64                 `json:"UsagePrice,omitempty"`
	StartTime        float64                 `json:"StartTime,omitempty"`
	NodeCount        int32                   `json:"NodeCount,omitempty"`
	Duration         int32                   `json:"Duration,omitempty"`
}

// ReservedNodesOffering describes a reserved node offering.
type ReservedNodesOffering struct {
	ReservedNodesOfferingID string                  `json:"ReservedNodesOfferingId,omitempty"`
	NodeType                string                  `json:"NodeType,omitempty"`
	OfferingType            string                  `json:"OfferingType,omitempty"`
	RecurringCharges        []recurringChargeObject `json:"RecurringCharges,omitempty"`
	FixedPrice              float64                 `json:"FixedPrice,omitempty"`
	UsagePrice              float64                 `json:"UsagePrice,omitempty"`
	Duration                int32                   `json:"Duration,omitempty"`
}

type recurringChargeObject struct {
	RecurringChargeFrequency string  `json:"RecurringChargeFrequency,omitempty"`
	RecurringChargeAmount    float64 `json:"RecurringChargeAmount,omitempty"`
}

type describeReservedNodesRequest struct {
	MaxResults     *int32 `json:"MaxResults,omitempty"`
	ReservedNodeID string `json:"ReservedNodeId,omitempty"`
	ReservationID  string `json:"ReservationId,omitempty"`
	NodeType       string `json:"NodeType,omitempty"`
	OfferingType   string `json:"OfferingType,omitempty"`
	NextToken      string `json:"NextToken,omitempty"`
}

type describeReservedNodesResponse struct {
	NextToken     string         `json:"NextToken,omitempty"`
	ReservedNodes []ReservedNode `json:"ReservedNodes"`
}

type describeReservedNodesOfferingsRequest struct {
	MaxResults              *int32 `json:"MaxResults,omitempty"`
	ReservedNodesOfferingID string `json:"ReservedNodesOfferingId,omitempty"`
	NodeType                string `json:"NodeType,omitempty"`
	OfferingType            string `json:"OfferingType,omitempty"`
	Duration                string `json:"Duration,omitempty"`
	NextToken               string `json:"NextToken,omitempty"`
}

type describeReservedNodesOfferingsResponse struct {
	NextToken              string                  `json:"NextToken,omitempty"`
	ReservedNodesOfferings []ReservedNodesOffering `json:"ReservedNodesOfferings"`
}

type purchaseReservedNodesOfferingRequest struct {
	ReservedNodesOfferingID string     `json:"ReservedNodesOfferingId"`
	ReservationID           string     `json:"ReservationId,omitempty"`
	NodeCount               *int32     `json:"NodeCount,omitempty"`
	Tags                    []tagEntry `json:"Tags,omitempty"`
}

type purchaseReservedNodesOfferingResponse struct {
	ReservedNode *ReservedNode `json:"ReservedNode,omitempty"`
}

// -- DescribeMultiRegionParameters request/response types --------------------
