package ec2

import (
	"encoding/xml"
	"net/url"
)

type crModificationQuoteCurrentConfigItem struct {
	ReservationState  string `xml:"reservationState,omitempty"`
	StartDate         string `xml:"startDate,omitempty"`
	OriginalStartDate string `xml:"originalStartDate,omitempty"`
	InstanceCount     int32  `xml:"instanceCount"`
}

type crReservationUpdateItem struct {
	NewStartDate          string `xml:"newStartDate,omitempty"`
	NewCommitmentEndDate  string `xml:"newCommitmentEndDate,omitempty"`
	NewCommitmentDuration int32  `xml:"newCommitmentDuration"`
}

type capacityReservationModificationQuoteItem struct {
	CurrentConfiguration crModificationQuoteCurrentConfigItem `xml:"currentConfiguration"`
	ModificationTerms    struct {
		ReservationUpdate crReservationUpdateItem `xml:"reservationUpdate"`
	} `xml:"modificationTerms"`
	CapacityReservationModificationQuoteID string          `xml:"capacityReservationModificationQuoteId,omitempty"`
	CapacityReservationID                  string          `xml:"capacityReservationId,omitempty"`
	QuoteState                             string          `xml:"quoteState,omitempty"`
	CreateTime                             string          `xml:"createTime,omitempty"`
	ExpirationTime                         string          `xml:"expirationTime,omitempty"`
	TagSet                                 []simpleTagItem `xml:"tagSet>item"`
}

func toCapacityReservationModificationQuoteItem(
	q *CapacityReservationModificationQuote, tags map[string]string,
) capacityReservationModificationQuoteItem {
	item := capacityReservationModificationQuoteItem{
		CapacityReservationModificationQuoteID: q.QuoteID,
		CapacityReservationID:                  q.CapacityReservationID,
		QuoteState:                             q.QuoteState,
		CreateTime:                             formatEC2Time(q.CreateTime),
		ExpirationTime:                         formatEC2Time(q.ExpirationTime),
		TagSet:                                 tagItemsFromMap(tags),
	}
	item.CurrentConfiguration = crModificationQuoteCurrentConfigItem{
		ReservationState:  q.CurrentReservationState,
		StartDate:         formatEC2Time(q.CurrentStartDate),
		OriginalStartDate: formatEC2Time(q.OriginalStartDate),
		InstanceCount:     q.CurrentInstanceCount,
	}
	item.ModificationTerms.ReservationUpdate = crReservationUpdateItem{
		NewStartDate:          formatEC2Time(q.NewStartDate),
		NewCommitmentEndDate:  formatEC2Time(q.NewCommitmentEndDate),
		NewCommitmentDuration: toInt32Clamped(int(q.NewCommitmentDuration)),
	}

	return item
}

type createCapacityReservationDateChangeQuoteResponse struct {
	XMLName   xml.Name                                 `xml:"CreateCapacityReservationDateChangeQuoteResponse"`
	Xmlns     string                                   `xml:"xmlns,attr"`
	RequestID string                                   `xml:"requestId"`
	Quote     capacityReservationModificationQuoteItem `xml:"capacityReservationModificationQuote"`
}

func (h *Handler) handleCreateCapacityReservationDateChangeQuote(vals url.Values, reqID string) (any, error) {
	newStart, err := optionalTimestamp(vals, "NewStartDate")
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "capacity-reservation-modification-quote")

	quote, err := h.Backend.CreateCapacityReservationDateChangeQuote(vals.Get("CapacityReservationId"), newStart, tags)
	if err != nil {
		return nil, err
	}

	return &createCapacityReservationDateChangeQuoteResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Quote:     toCapacityReservationModificationQuoteItem(quote, h.Backend.TagsForResource(quote.QuoteID)),
	}, nil
}

type describeCapacityReservationDateChangeQuotesResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityReservationDateChangeQuotesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Quotes    struct {
		Items []capacityReservationModificationQuoteItem `xml:"item"`
	} `xml:"capacityReservationModificationQuoteSet"`
}

func (h *Handler) handleDescribeCapacityReservationDateChangeQuotes(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationModificationQuoteId")

	resp := &describeCapacityReservationDateChangeQuotesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, q := range h.Backend.DescribeCapacityReservationDateChangeQuotes(ids) {
		resp.Quotes.Items = append(
			resp.Quotes.Items,
			toCapacityReservationModificationQuoteItem(q, h.Backend.TagsForResource(q.QuoteID)),
		)
	}

	return finishPagedFiltered(vals, resp)
}
