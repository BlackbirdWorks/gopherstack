package ec2

import (
	"encoding/xml"
	"net/url"
)

type describeReservedInstancesResponse struct {
	XMLName              xml.Name `xml:"DescribeReservedInstancesResponse"`
	RequestID            string   `xml:"requestId"`
	ReservedInstancesSet struct {
		Items []reservedInstanceItem `xml:"item"`
	} `xml:"reservedInstancesSet"`
}

type reservedInstancesOfferingItem struct {
	ReservedInstancesOfferingID string  `xml:"reservedInstancesOfferingId"`
	InstanceType                string  `xml:"instanceType,omitempty"`
	AvailabilityZone            string  `xml:"availabilityZone,omitempty"`
	ProductDescription          string  `xml:"productDescription,omitempty"`
	OfferingType                string  `xml:"offeringType,omitempty"`
	Duration                    int64   `xml:"duration"`
	FixedPrice                  float64 `xml:"fixedPrice"`
	UsagePrice                  float64 `xml:"usagePrice"`
}

type describeReservedInstancesOfferingsResponse struct {
	XMLName                       xml.Name `xml:"DescribeReservedInstancesOfferingsResponse"`
	RequestID                     string   `xml:"requestId"`
	ReservedInstancesOfferingsSet struct {
		Items []reservedInstancesOfferingItem `xml:"item"`
	} `xml:"reservedInstancesOfferingsSet"`
}

type purchaseReservedInstancesOfferingResponse struct {
	XMLName             xml.Name `xml:"PurchaseReservedInstancesOfferingResponse"`
	RequestID           string   `xml:"requestId"`
	ReservedInstancesID string   `xml:"reservedInstancesId"`
}

type reservedInstancesListingItem struct {
	ReservedInstancesListingID string `xml:"reservedInstancesListingId"`
	ReservedInstancesID        string `xml:"reservedInstancesId,omitempty"`
	Status                     string `xml:"status,omitempty"`
	StatusMessage              string `xml:"statusMessage,omitempty"`
}

type createReservedInstancesListingResponse struct {
	XMLName                      xml.Name `xml:"CreateReservedInstancesListingResponse"`
	RequestID                    string   `xml:"requestId"`
	ReservedInstancesListingsSet struct {
		Items []reservedInstancesListingItem `xml:"item"`
	} `xml:"reservedInstancesListingsSet"`
}

type describeReservedInstancesListingsResponse struct {
	XMLName                      xml.Name `xml:"DescribeReservedInstancesListingsResponse"`
	RequestID                    string   `xml:"requestId"`
	ReservedInstancesListingsSet struct {
		Items []reservedInstancesListingItem `xml:"item"`
	} `xml:"reservedInstancesListingsSet"`
}

type reservedInstancesModificationItem struct {
	ReservedInstancesModificationID string `xml:"reservedInstancesModificationId"`
	Status                          string `xml:"status,omitempty"`
	StatusMessage                   string `xml:"statusMessage,omitempty"`
}

type describeReservedInstancesModificationsResponse struct {
	XMLName                           xml.Name `xml:"DescribeReservedInstancesModificationsResponse"`
	RequestID                         string   `xml:"requestId"`
	ReservedInstancesModificationsSet struct {
		Items []reservedInstancesModificationItem `xml:"item"`
	} `xml:"reservedInstancesModificationsSet"`
}

type modifyReservedInstancesResponse struct {
	XMLName                         xml.Name `xml:"ModifyReservedInstancesResponse"`
	RequestID                       string   `xml:"requestId"`
	ReservedInstancesModificationID string   `xml:"reservedInstancesModificationId"`
}

type getReservedInstancesExchangeQuoteResponse struct {
	XMLName         xml.Name `xml:"GetReservedInstancesExchangeQuoteResponse"`
	RequestID       string   `xml:"requestId"`
	IsValidExchange bool     `xml:"isValidExchange"`
}

// deleteQueuedRIErrorItem mirrors types.DeleteQueuedReservedInstancesError.
type deleteQueuedRIErrorItem struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// successfulQueuedPurchaseDeletionItem mirrors types.SuccessfulQueuedPurchaseDeletion.
type successfulQueuedPurchaseDeletionItem struct {
	ReservedInstancesID string `xml:"reservedInstancesId"`
}

// failedQueuedPurchaseDeletionItem mirrors types.FailedQueuedPurchaseDeletion.
type failedQueuedPurchaseDeletionItem struct {
	ReservedInstancesID string                  `xml:"reservedInstancesId"`
	Error               deleteQueuedRIErrorItem `xml:"error"`
}

// deleteQueuedReservedInstancesResponse mirrors the real
// DeleteQueuedReservedInstancesOutput: real, non-boolean per-ID results
// instead of a bare {Return: true}.
type deleteQueuedReservedInstancesResponse struct {
	XMLName       xml.Name `xml:"DeleteQueuedReservedInstancesResponse"`
	RequestID     string   `xml:"requestId"`
	SuccessfulSet struct {
		Items []successfulQueuedPurchaseDeletionItem `xml:"item"`
	} `xml:"successfulQueuedPurchaseDeletionSet"`
	FailedSet struct {
		Items []failedQueuedPurchaseDeletionItem `xml:"item"`
	} `xml:"failedQueuedPurchaseDeletionSet"`
}

// ---- Traffic Mirror Filter handlers ----

func toReservedInstanceItem(ri *ReservedInstance, tags map[string]string) reservedInstanceItem {
	return reservedInstanceItem{
		ReservedInstancesID: ri.ReservedInstancesID,
		InstanceType:        ri.InstanceType,
		AvailabilityZone:    ri.AvailabilityZone,
		InstanceCount:       ri.InstanceCount,
		ProductDescription:  ri.ProductDescription,
		State:               ri.State,
		OfferingType:        ri.OfferingType,
		Duration:            ri.Duration,
		FixedPrice:          ri.FixedPrice,
		UsagePrice:          ri.UsagePrice,
		TagSet:              tagItemsFromMap(tags),
	}
}

func toReservedInstancesOfferingItem(o *ReservedInstancesOffering) reservedInstancesOfferingItem {
	return reservedInstancesOfferingItem{
		ReservedInstancesOfferingID: o.ReservedInstancesOfferingID,
		InstanceType:                o.InstanceType,
		AvailabilityZone:            o.AvailabilityZone,
		ProductDescription:          o.ProductDescription,
		OfferingType:                o.OfferingType,
		Duration:                    o.Duration,
		FixedPrice:                  o.FixedPrice,
		UsagePrice:                  o.UsagePrice,
	}
}

func toReservedInstancesListingItem(l *ReservedInstancesListing) reservedInstancesListingItem {
	return reservedInstancesListingItem{
		ReservedInstancesListingID: l.ReservedInstancesListingID,
		ReservedInstancesID:        l.ReservedInstancesID,
		Status:                     l.Status,
		StatusMessage:              l.StatusMessage,
	}
}

func toReservedInstancesModificationItem(
	m *ReservedInstancesModification,
) reservedInstancesModificationItem {
	return reservedInstancesModificationItem{
		ReservedInstancesModificationID: m.ReservedInstancesModificationID,
		Status:                          m.Status,
		StatusMessage:                   m.StatusMessage,
	}
}

func (h *Handler) handleDescribeReservedInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesId")
	ris := h.Backend.DescribeReservedInstances(ids)

	resp := &describeReservedInstancesResponse{RequestID: reqID}
	for _, ri := range ris {
		resp.ReservedInstancesSet.Items = append(
			resp.ReservedInstancesSet.Items,
			toReservedInstanceItem(ri, h.Backend.TagsForResource(ri.ReservedInstancesID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeReservedInstancesOfferings(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceType := vals.Get("InstanceType")
	az := vals.Get("AvailabilityZone")
	productDesc := vals.Get("ProductDescription")

	offerings := h.Backend.DescribeReservedInstancesOfferings(instanceType, az, productDesc)

	resp := &describeReservedInstancesOfferingsResponse{RequestID: reqID}
	for _, o := range offerings {
		resp.ReservedInstancesOfferingsSet.Items = append(
			resp.ReservedInstancesOfferingsSet.Items,
			toReservedInstancesOfferingItem(o),
		)
	}

	return resp, nil
}

func (h *Handler) handlePurchaseReservedInstancesOffering(
	vals url.Values,
	reqID string,
) (any, error) {
	offeringID := vals.Get("ReservedInstancesOfferingId")

	instanceCount := 1
	parseIntValue(vals.Get("InstanceCount"), &instanceCount)

	ri, err := h.Backend.PurchaseReservedInstancesOffering(offeringID, instanceCount)
	if err != nil {
		return nil, err
	}

	return &purchaseReservedInstancesOfferingResponse{
		RequestID:           reqID,
		ReservedInstancesID: ri.ReservedInstancesID,
	}, nil
}

func (h *Handler) handleCreateReservedInstancesListing(vals url.Values, reqID string) (any, error) {
	riID := vals.Get("ReservedInstancesId")

	instanceCount := 1
	parseIntValue(vals.Get("InstanceCount"), &instanceCount)

	listing, err := h.Backend.CreateReservedInstancesListing(riID, instanceCount)
	if err != nil {
		return nil, err
	}

	resp := &createReservedInstancesListingResponse{RequestID: reqID}
	resp.ReservedInstancesListingsSet.Items = append(
		resp.ReservedInstancesListingsSet.Items,
		toReservedInstancesListingItem(listing),
	)

	return resp, nil
}

// cancelReservedInstancesListingResponse matches
// CancelReservedInstancesListingOutput (ec2@v1.319.1
// api_op_CancelReservedInstancesListing.go): reservedInstancesListingsSet,
// no Return member -- the same shape DescribeReservedInstancesListings
// already renders correctly.
type cancelReservedInstancesListingResponse struct {
	XMLName                      xml.Name `xml:"CancelReservedInstancesListingResponse"`
	RequestID                    string   `xml:"requestId"`
	ReservedInstancesListingsSet struct {
		Items []reservedInstancesListingItem `xml:"item"`
	} `xml:"reservedInstancesListingsSet"`
}

func (h *Handler) handleCancelReservedInstancesListing(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ReservedInstancesListingId")

	listing, err := h.Backend.CancelReservedInstancesListing(id)
	if err != nil {
		return nil, err
	}

	resp := &cancelReservedInstancesListingResponse{RequestID: reqID}
	resp.ReservedInstancesListingsSet.Items = append(
		resp.ReservedInstancesListingsSet.Items,
		toReservedInstancesListingItem(listing),
	)

	return resp, nil
}

func (h *Handler) handleDescribeReservedInstancesListings(
	vals url.Values,
	reqID string,
) (any, error) {
	var ids []string
	if id := vals.Get("ReservedInstancesListingId"); id != "" {
		ids = []string{id}
	}

	listings := h.Backend.DescribeReservedInstancesListings(ids)

	resp := &describeReservedInstancesListingsResponse{RequestID: reqID}
	for _, l := range listings {
		resp.ReservedInstancesListingsSet.Items = append(
			resp.ReservedInstancesListingsSet.Items,
			toReservedInstancesListingItem(l),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeReservedInstancesModifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesModificationId")
	mods := h.Backend.DescribeReservedInstancesModifications(ids)

	resp := &describeReservedInstancesModificationsResponse{RequestID: reqID}
	for _, m := range mods {
		resp.ReservedInstancesModificationsSet.Items = append(
			resp.ReservedInstancesModificationsSet.Items,
			toReservedInstancesModificationItem(m),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyReservedInstances(vals url.Values, reqID string) (any, error) {
	riIDs := parseMemberList(vals, "ReservedInstancesId")
	targetInstanceType := vals.Get("ReservedInstancesConfigurationSetItemType.1.InstanceType")

	targetCount := 0
	parseIntValue(
		vals.Get("ReservedInstancesConfigurationSetItemType.1.InstanceCount"),
		&targetCount,
	)

	mod, err := h.Backend.ModifyReservedInstances(riIDs, targetInstanceType, targetCount)
	if err != nil {
		return nil, err
	}

	return &modifyReservedInstancesResponse{
		RequestID:                       reqID,
		ReservedInstancesModificationID: mod.ReservedInstancesModificationID,
	}, nil
}

func (h *Handler) handleDeleteQueuedReservedInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesId")
	results := h.Backend.DeleteQueuedReservedInstances(ids)

	resp := &deleteQueuedReservedInstancesResponse{
		XMLName:   xml.Name{Local: "DeleteQueuedReservedInstancesResponse"},
		RequestID: reqID,
	}

	for _, r := range results {
		if r.Failed {
			resp.FailedSet.Items = append(resp.FailedSet.Items, failedQueuedPurchaseDeletionItem{
				ReservedInstancesID: r.ReservedInstancesID,
				Error: deleteQueuedRIErrorItem{
					Code:    r.ErrorCode,
					Message: r.ErrorMessage,
				},
			})

			continue
		}

		resp.SuccessfulSet.Items = append(
			resp.SuccessfulSet.Items,
			successfulQueuedPurchaseDeletionItem{ReservedInstancesID: r.ReservedInstancesID},
		)
	}

	return resp, nil
}

func (h *Handler) handleGetReservedInstancesExchangeQuote(_ url.Values, reqID string) (any, error) {
	return &getReservedInstancesExchangeQuoteResponse{
		RequestID:       reqID,
		IsValidExchange: true,
	}, nil
}

// registerReservedInstancesOps registers the ReservedInstances operation handlers.
func registerReservedInstancesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeReservedInstances"] = h.handleDescribeReservedInstances
	ops["DescribeReservedInstancesOfferings"] = h.handleDescribeReservedInstancesOfferings
	ops["PurchaseReservedInstancesOffering"] = h.handlePurchaseReservedInstancesOffering
	ops["CreateReservedInstancesListing"] = h.handleCreateReservedInstancesListing
	ops["CancelReservedInstancesListing"] = h.handleCancelReservedInstancesListing
	ops["DescribeReservedInstancesListings"] = h.handleDescribeReservedInstancesListings
	ops["DescribeReservedInstancesModifications"] = h.handleDescribeReservedInstancesModifications
	ops["ModifyReservedInstances"] = h.handleModifyReservedInstances
	ops["DeleteQueuedReservedInstances"] = h.handleDeleteQueuedReservedInstances
	ops["GetReservedInstancesExchangeQuote"] = h.handleGetReservedInstancesExchangeQuote
}
