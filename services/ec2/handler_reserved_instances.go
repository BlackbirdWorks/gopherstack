package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
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
	OfferingClass               string  `xml:"offeringClass,omitempty"`
	Duration                    int64   `xml:"duration"`
	FixedPrice                  float64 `xml:"fixedPrice"`
	UsagePrice                  float64 `xml:"usagePrice"`
}

type describeReservedInstancesOfferingsResponse struct {
	XMLName                       xml.Name `xml:"DescribeReservedInstancesOfferingsResponse"`
	RequestID                     string   `xml:"requestId"`
	NextToken                     string   `xml:"nextToken,omitempty"`
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

// reservationValueItem mirrors types.ReservationValue (ec2@v1.329.0
// types/types.go:19584); HourlyPrice/RemainingTotalValue/RemainingUpfrontValue
// are wire strings there, not numbers.
type reservationValueItem struct {
	HourlyPrice           string `xml:"hourlyPrice,omitempty"`
	RemainingTotalValue   string `xml:"remainingTotalValue,omitempty"`
	RemainingUpfrontValue string `xml:"remainingUpfrontValue,omitempty"`
}

// reservedInstanceReservationValueItem mirrors
// types.ReservedInstanceReservationValue (ec2@v1.329.0 types/types.go:19709).
type reservedInstanceReservationValueItem struct {
	ReservedInstanceID string               `xml:"reservedInstanceId"`
	ReservationValue   reservationValueItem `xml:"reservationValue"`
}

// targetConfigurationItem mirrors types.TargetConfiguration (ec2@v1.329.0
// types/types.go:23847).
type targetConfigurationItem struct {
	OfferingID    string `xml:"offeringId"`
	InstanceCount int    `xml:"instanceCount,omitempty"`
}

// targetReservationValueItem mirrors types.TargetReservationValue
// (ec2@v1.329.0 types/types.go:23926).
type targetReservationValueItem struct {
	ReservationValue    reservationValueItem    `xml:"reservationValue"`
	TargetConfiguration targetConfigurationItem `xml:"targetConfiguration"`
}

// getReservedInstancesExchangeQuoteResponse mirrors
// types.GetReservedInstancesExchangeQuoteOutput (ec2@v1.329.0
// api_op_GetReservedInstancesExchangeQuote.go); wire keys verified against
// deserializers.go:221192 (awsEc2query_deserializeOpDocumentGetReservedInstancesExchangeQuoteOutput).
type getReservedInstancesExchangeQuoteResponse struct {
	XMLName                             xml.Name              `xml:"GetReservedInstancesExchangeQuoteResponse"`
	RequestID                           string                `xml:"requestId"`
	CurrencyCode                        string                `xml:"currencyCode,omitempty"`
	OutputReservedInstancesWillExpireAt string                `xml:"outputReservedInstancesWillExpireAt,omitempty"`
	PaymentDue                          string                `xml:"paymentDue,omitempty"`
	ValidationFailureReason             string                `xml:"validationFailureReason,omitempty"`
	IsValidExchange                     bool                  `xml:"isValidExchange"`
	ReservedInstanceValueRollup         *reservationValueItem `xml:"reservedInstanceValueRollup,omitempty"`
	ReservedInstanceValueSet            struct {
		Items []reservedInstanceReservationValueItem `xml:"item"`
	} `xml:"reservedInstanceValueSet"`
	TargetConfigurationValueRollup *reservationValueItem `xml:"targetConfigurationValueRollup,omitempty"`
	TargetConfigurationValueSet    struct {
		Items []targetReservationValueItem `xml:"item"`
	} `xml:"targetConfigurationValueSet"`
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

// formatEC2Time renders t in the same ISO8601 form used elsewhere in this
// package (e.g. instanceItem.LaunchTime), or "" for a zero time so the
// caller's xml:",omitempty" tag drops the element.
func formatEC2Time(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func toReservedInstanceItem(ri *ReservedInstance, tags map[string]string) reservedInstanceItem {
	return reservedInstanceItem{
		ReservedInstancesID: ri.ReservedInstancesID,
		InstanceType:        ri.InstanceType,
		AvailabilityZone:    ri.AvailabilityZone,
		InstanceCount:       ri.InstanceCount,
		ProductDescription:  ri.ProductDescription,
		State:               ri.State,
		OfferingType:        ri.OfferingType,
		OfferingClass:       ri.OfferingClass,
		Start:               formatEC2Time(ri.Start),
		End:                 formatEC2Time(ri.End),
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
		OfferingClass:               o.OfferingClass,
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
	offeringClass := vals.Get("OfferingClass")

	offerings := h.Backend.DescribeReservedInstancesOfferings(instanceType, az, productDesc, offeringClass)

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinDefault, ec2PageMaxReservedInstancesOfferings, ec2PageMaxReservedInstancesOfferings,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	offerings, nextToken = pageSlice(offerings, offset, maxResults)

	resp := &describeReservedInstancesOfferingsResponse{RequestID: reqID, NextToken: nextToken}
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

// parseTargetConfigurations parses GetReservedInstancesExchangeQuote/
// AcceptReservedInstancesExchangeQuote's TargetConfiguration.N.OfferingId /
// TargetConfiguration.N.InstanceCount (serializers.go:87596-87601,67108-67123:
// wire prefix "TargetConfiguration", not "TargetConfigurationRequest").
func parseTargetConfigurations(vals url.Values) []TargetConfigurationRequest {
	var targets []TargetConfigurationRequest

	for i := 1; ; i++ {
		offeringID := vals.Get(fmt.Sprintf("TargetConfiguration.%d.OfferingId", i))
		if offeringID == "" {
			break
		}

		count := 0
		parseIntValue(vals.Get(fmt.Sprintf("TargetConfiguration.%d.InstanceCount", i)), &count)

		targets = append(targets, TargetConfigurationRequest{OfferingID: offeringID, InstanceCount: count})
	}

	return targets
}

func toReservationValueItem(v ReservationValue) reservationValueItem {
	return reservationValueItem{
		HourlyPrice:           fmt.Sprintf("%.2f", v.HourlyPrice),
		RemainingTotalValue:   fmt.Sprintf("%.2f", v.RemainingTotalValue),
		RemainingUpfrontValue: fmt.Sprintf("%.2f", v.RemainingUpfrontValue),
	}
}

func (h *Handler) handleGetReservedInstancesExchangeQuote(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReservedInstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one ReservedInstanceId is required", ErrInvalidParameter)
	}

	quote, err := h.Backend.GetReservedInstancesExchangeQuote(ids, parseTargetConfigurations(vals))
	if err != nil {
		return nil, err
	}

	resp := &getReservedInstancesExchangeQuoteResponse{
		RequestID:               reqID,
		CurrencyCode:            quote.CurrencyCode,
		IsValidExchange:         quote.IsValidExchange,
		ValidationFailureReason: quote.ValidationFailureReason,
	}

	if !quote.IsValidExchange {
		return resp, nil
	}

	resp.OutputReservedInstancesWillExpireAt = formatEC2Time(quote.OutputReservedInstancesWillExpireAt)
	resp.PaymentDue = fmt.Sprintf("%.2f", quote.PaymentDue)

	rollup := toReservationValueItem(quote.ReservedInstanceValueRollup)
	resp.ReservedInstanceValueRollup = &rollup

	targetRollup := toReservationValueItem(quote.TargetConfigurationValueRollup)
	resp.TargetConfigurationValueRollup = &targetRollup

	for _, v := range quote.ReservedInstanceValueSet {
		resp.ReservedInstanceValueSet.Items = append(resp.ReservedInstanceValueSet.Items,
			reservedInstanceReservationValueItem{
				ReservedInstanceID: v.ReservedInstancesID,
				ReservationValue:   toReservationValueItem(v.Value),
			})
	}

	for _, v := range quote.TargetConfigurationValueSet {
		resp.TargetConfigurationValueSet.Items = append(resp.TargetConfigurationValueSet.Items,
			targetReservationValueItem{
				ReservationValue: toReservationValueItem(v.Value),
				TargetConfiguration: targetConfigurationItem{
					OfferingID:    v.OfferingID,
					InstanceCount: v.InstanceCount,
				},
			})
	}

	return resp, nil
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
