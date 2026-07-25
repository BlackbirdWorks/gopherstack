package memorydb

// ReservedNode represents a reserved MemoryDB node. StartTime is epoch
// seconds (float64), matching the real ReservedNode TStamp shape; this
// struct is serialized directly as the wire response (see
// describeReservedNodesResponse / purchaseReservedNodesOfferingResponse), so
// its field types must match the wire, not just internal convenience.
//
// NOTE: the real SDK's ReservedNode type (aws-sdk-go-v2/service/memorydb/types)
// has NO "ReservedNodeId" field -- confirmed against deserializers.go's
// awsAwsjson11_deserializeDocumentReservedNode, which only recognizes ARN,
// Duration, FixedPrice, NodeCount, NodeType, OfferingType, RecurringCharges,
// ReservationId, ReservedNodesOfferingId, StartTime, State. A prior pass
// invented "ReservedNodeId" (and used it as the unique/filter key) while
// omitting the real "ReservedNodesOfferingId" field entirely; fixed here.
// It also invented a "UsagePrice" field on both ReservedNode and
// ReservedNodesOffering -- confirmed absent from both types' real
// deserializers (awsAwsjson11_deserializeDocumentReservedNode and
// awsAwsjson11_deserializeDocumentReservedNodesOffering); deleted.
type ReservedNode struct {
	ReservationID           string                  `json:"ReservationId,omitempty"`
	ReservedNodesOfferingID string                  `json:"ReservedNodesOfferingId,omitempty"`
	NodeType                string                  `json:"NodeType,omitempty"`
	OfferingType            string                  `json:"OfferingType,omitempty"`
	State                   string                  `json:"State,omitempty"`
	ARN                     string                  `json:"ARN,omitempty"`
	RecurringCharges        []recurringChargeObject `json:"RecurringCharges,omitempty"`
	FixedPrice              float64                 `json:"FixedPrice,omitempty"`
	StartTime               float64                 `json:"StartTime,omitempty"`
	NodeCount               int32                   `json:"NodeCount,omitempty"`
	Duration                int32                   `json:"Duration,omitempty"`
}

// ReservedNodesOffering describes a reserved node offering.
type ReservedNodesOffering struct {
	ReservedNodesOfferingID string                  `json:"ReservedNodesOfferingId,omitempty"`
	NodeType                string                  `json:"NodeType,omitempty"`
	OfferingType            string                  `json:"OfferingType,omitempty"`
	RecurringCharges        []recurringChargeObject `json:"RecurringCharges,omitempty"`
	FixedPrice              float64                 `json:"FixedPrice,omitempty"`
	Duration                int32                   `json:"Duration,omitempty"`
}

type recurringChargeObject struct {
	RecurringChargeFrequency string  `json:"RecurringChargeFrequency,omitempty"`
	RecurringChargeAmount    float64 `json:"RecurringChargeAmount,omitempty"`
}

// describeReservedNodesRequest mirrors DescribeReservedNodesInput, which has no
// "ReservedNodeId" field -- only ReservationId (confirmed via
// api_op_DescribeReservedNodes.go). A prior pass invented ReservedNodeId as a
// filter; removed.
type describeReservedNodesRequest struct {
	MaxResults    *int32 `json:"MaxResults,omitempty"`
	ReservationID string `json:"ReservationId,omitempty"`
	NodeType      string `json:"NodeType,omitempty"`
	OfferingType  string `json:"OfferingType,omitempty"`
	NextToken     string `json:"NextToken,omitempty"`
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
