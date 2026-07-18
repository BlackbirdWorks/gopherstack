package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

// ---- spot instances ----

type spotLaunchSpecItem struct {
	ImageID      string `xml:"imageId"`
	InstanceType string `xml:"instanceType"`
	SubnetID     string `xml:"subnetId,omitempty"`
}

type spotInstanceRequestItem struct {
	LaunchSpecification   spotLaunchSpecItem `xml:"launchSpecification"`
	SpotInstanceRequestID string             `xml:"spotInstanceRequestId"`
	InstanceID            string             `xml:"instanceId,omitempty"`
	State                 string             `xml:"state"`
	SpotPrice             string             `xml:"spotPrice"`
	Type                  string             `xml:"type"`
	CreateTime            string             `xml:"createTime"`
}

type spotInstanceRequestSet struct {
	Items []spotInstanceRequestItem `xml:"item"`
}

type requestSpotInstancesResponse struct {
	XMLName                xml.Name               `xml:"RequestSpotInstancesResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	RequestID              string                 `xml:"requestId"`
	SpotInstanceRequestSet spotInstanceRequestSet `xml:"spotInstanceRequestSet"`
}

type describeSpotInstanceRequestsResponse struct {
	XMLName                xml.Name               `xml:"DescribeSpotInstanceRequestsResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	RequestID              string                 `xml:"requestId"`
	SpotInstanceRequestSet spotInstanceRequestSet `xml:"spotInstanceRequestSet"`
}

type cancelledSpotItem struct {
	SpotInstanceRequestID string `xml:"spotInstanceRequestId"`
	State                 string `xml:"state"`
}

type cancelledSpotSet struct {
	Items []cancelledSpotItem `xml:"item"`
}

type cancelSpotInstanceRequestsResponse struct {
	XMLName                xml.Name         `xml:"CancelSpotInstanceRequestsResponse"`
	Xmlns                  string           `xml:"xmlns,attr"`
	RequestID              string           `xml:"requestId"`
	SpotInstanceRequestSet cancelledSpotSet `xml:"spotInstanceRequestSet"`
}

type spotPriceItem struct {
	InstanceType       string `xml:"instanceType"`
	AvailabilityZone   string `xml:"availabilityZone"`
	ProductDescription string `xml:"productDescription"`
	SpotPrice          string `xml:"spotPrice"`
	Timestamp          string `xml:"timestamp"`
}

type spotPriceHistorySet struct {
	Items []spotPriceItem `xml:"item"`
}

type describeSpotPriceHistoryResponse struct {
	XMLName             xml.Name            `xml:"DescribeSpotPriceHistoryResponse"`
	Xmlns               string              `xml:"xmlns,attr"`
	RequestID           string              `xml:"requestId"`
	SpotPriceHistorySet spotPriceHistorySet `xml:"spotPriceHistorySet"`
}

func toSpotRequestItem(req *SpotInstanceRequest) spotInstanceRequestItem {
	return spotInstanceRequestItem{
		SpotInstanceRequestID: req.ID,
		InstanceID:            req.InstanceID,
		State:                 req.State,
		SpotPrice:             req.SpotPrice,
		Type:                  req.Type,
		CreateTime:            req.CreateTime.Format("2006-01-02T15:04:05.000Z"),
		LaunchSpecification: spotLaunchSpecItem{
			ImageID:      req.LaunchSpec.ImageID,
			InstanceType: req.LaunchSpec.InstanceType,
			SubnetID:     req.LaunchSpec.SubnetID,
		},
	}
}

func (h *Handler) handleRequestSpotInstances(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("LaunchSpecification.ImageId")
	instanceType := vals.Get("LaunchSpecification.InstanceType")
	subnetID := vals.Get("LaunchSpecification.SubnetId")
	spotPrice := vals.Get("SpotPrice")

	if imageID == "" {
		return nil, fmt.Errorf("%w: LaunchSpecification.ImageId is required", ErrInvalidParameter)
	}

	if instanceType == "" {
		return nil, fmt.Errorf(
			"%w: LaunchSpecification.InstanceType is required",
			ErrInvalidParameter,
		)
	}

	req, err := h.Backend.RequestSpotInstances(imageID, instanceType, subnetID, spotPrice)
	if err != nil {
		return nil, err
	}

	return &requestSpotInstancesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		SpotInstanceRequestSet: spotInstanceRequestSet{
			Items: []spotInstanceRequestItem{toSpotRequestItem(req)},
		},
	}, nil
}

func (h *Handler) handleDescribeSpotInstanceRequests(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SpotInstanceRequestId")
	reqs := h.Backend.DescribeSpotInstanceRequests(ids)

	filters := parseEC2Filters(vals)
	reqs = applySpotRequestFilters(reqs, filters, h.Backend)

	items := make([]spotInstanceRequestItem, 0, len(reqs))
	for _, req := range reqs {
		items = append(items, toSpotRequestItem(req))
	}

	return &describeSpotInstanceRequestsResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		SpotInstanceRequestSet: spotInstanceRequestSet{Items: items},
	}, nil
}

func (h *Handler) handleCancelSpotInstanceRequests(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SpotInstanceRequestId")
	if len(ids) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one SpotInstanceRequestId is required",
			ErrInvalidParameter,
		)
	}

	if err := h.Backend.CancelSpotInstanceRequests(ids); err != nil {
		return nil, err
	}

	items := make([]cancelledSpotItem, 0, len(ids))
	for _, id := range ids {
		items = append(items, cancelledSpotItem{SpotInstanceRequestID: id, State: stateCancelled})
	}

	return &cancelSpotInstanceRequestsResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		SpotInstanceRequestSet: cancelledSpotSet{Items: items},
	}, nil
}

// handleDescribeSpotPriceHistory returns deterministic spot price history.
func (h *Handler) handleDescribeSpotPriceHistory(vals url.Values, reqID string) (any, error) {
	instanceTypes := parseMemberList(vals, "InstanceType")
	azs := parseMemberList(vals, "AvailabilityZone")
	products := parseMemberList(vals, "ProductDescription")

	var startTime time.Time
	if v := vals.Get("StartTime"); v != "" {
		_ = startTime.UnmarshalText([]byte(v))
	}

	records := GenerateSpotPriceHistory(instanceTypes, azs, products, startTime, h.Region)

	items := make([]spotPriceItem, 0, len(records))
	for _, r := range records {
		items = append(items, spotPriceItem{
			InstanceType:       r.InstanceType,
			AvailabilityZone:   r.AvailabilityZone,
			ProductDescription: r.ProductDescription,
			SpotPrice:          r.SpotPrice,
			Timestamp:          r.Timestamp.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &describeSpotPriceHistoryResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		SpotPriceHistorySet: spotPriceHistorySet{Items: items},
	}, nil
}
