package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// toCapacityReservationResult builds the XML result for a capacity reservation.
func toCapacityReservationResult(cr *CapacityReservation) describeCapacityReservationResult {
	result := describeCapacityReservationResult{
		DecreaseRequestsRemaining: cr.DecreaseRequestsRemaining,
	}

	if !cr.LastModifiedTime.IsZero() {
		result.LastModifiedTime = cr.LastModifiedTime.UTC().Format("2006-01-02T15:04:05Z")
	}

	if cr.MinimumCapacityUnits > 0 {
		result.MinimumLoadBalancerCapacity = &xmlMinimumLoadBalancerCapacity{
			CapacityUnits: cr.MinimumCapacityUnits,
		}
	}

	return result
}

func (h *Handler) handleDescribeCapacityReservation(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	cr, err := h.Backend.DescribeCapacityReservation(lbArn)
	if err != nil {
		return nil, err
	}

	return &describeCapacityReservationResponse{
		Xmlns:            elbv2XMLNS,
		Result:           toCapacityReservationResult(cr),
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-capacity-reservation"},
	}, nil
}

func (h *Handler) handleModifyCapacityReservation(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	var minCapacity *int32

	if raw := vals.Get("MinimumLoadBalancerCapacity.CapacityUnits"); raw != "" {
		n, err := parseInt32(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid CapacityUnits %q", ErrInvalidParameter, raw)
		}

		minCapacity = &n
	}

	reset := false
	if raw := vals.Get("ResetCapacityReservation"); raw != "" {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid ResetCapacityReservation %q", ErrInvalidParameter, raw)
		}

		reset = b
	}

	cr, err := h.Backend.ModifyCapacityReservation(lbArn, minCapacity, reset)
	if err != nil {
		return nil, err
	}

	return &modifyCapacityReservationResponse{
		Xmlns:            elbv2XMLNS,
		Result:           toCapacityReservationResult(cr),
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-capacity-reservation"},
	}, nil
}

type xmlMinimumLoadBalancerCapacity struct {
	CapacityUnits int32 `xml:"CapacityUnits"`
}

type describeCapacityReservationResult struct {
	MinimumLoadBalancerCapacity *xmlMinimumLoadBalancerCapacity `xml:"MinimumLoadBalancerCapacity,omitempty"`
	LastModifiedTime            string                          `xml:"LastModifiedTime,omitempty"`
	DecreaseRequestsRemaining   int32                           `xml:"DecreaseRequestsRemaining"`
}

type describeCapacityReservationResponse struct {
	XMLName          xml.Name                          `xml:"DescribeCapacityReservationResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           describeCapacityReservationResult `xml:"DescribeCapacityReservationResult"`
}

type modifyCapacityReservationResponse struct {
	XMLName          xml.Name                          `xml:"ModifyCapacityReservationResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           describeCapacityReservationResult `xml:"ModifyCapacityReservationResult"`
}
