package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handlePutWarmPool(vals url.Values) (any, error) {
	minSize, err := parseIntVal(vals.Get("MinSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
	}

	maxGroupPreparedCapacity, err := parseIntVal(vals.Get("MaxGroupPreparedCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxGroupPreparedCapacity", ErrInvalidParameter)
	}

	input := WarmPoolInput{
		AutoScalingGroupName:     vals.Get("AutoScalingGroupName"),
		PoolState:                vals.Get("PoolState"),
		MinSize:                  minSize,
		MaxGroupPreparedCapacity: maxGroupPreparedCapacity,
	}

	if putErr := h.Backend.PutWarmPool(input); putErr != nil {
		return nil, putErr
	}

	return &putWarmPoolResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-warm-pool"},
	}, nil
}

func (h *Handler) handleDeleteWarmPool(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	if err := h.Backend.DeleteWarmPool(groupName); err != nil {
		return nil, err
	}

	return &deleteWarmPoolResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-warm-pool"},
	}, nil
}

func (h *Handler) handleDescribeWarmPool(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	wp, err := h.Backend.DescribeWarmPool(groupName)
	if err != nil {
		return nil, err
	}

	xmlWP := xmlWarmPoolConfiguration{
		MinSize:                  wp.MinSize,
		PoolState:                wp.PoolState,
		Status:                   wp.Status,
		MaxGroupPreparedCapacity: wp.MaxGroupPreparedCapacity,
	}

	if wp.InstanceReusePolicy.ReuseOnScaleIn {
		xmlWP.InstanceReusePolicy = &struct {
			ReuseOnScaleIn bool `xml:"ReuseOnScaleIn,omitempty"`
		}{ReuseOnScaleIn: true}
	}

	return &describeWarmPoolResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeWarmPoolResult{
			WarmPoolConfiguration: xmlWP,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-warm-pool"},
	}, nil
}

type putWarmPoolResponse struct {
	XMLName          xml.Name            `xml:"PutWarmPoolResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"PutWarmPoolResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteWarmPoolResponse struct {
	XMLName          xml.Name            `xml:"DeleteWarmPoolResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"DeleteWarmPoolResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlWarmPoolConfiguration struct {
	InstanceReusePolicy *struct {
		ReuseOnScaleIn bool `xml:"ReuseOnScaleIn,omitempty"`
	} `xml:"InstanceReusePolicy,omitempty"`
	PoolState                string `xml:"PoolState"`
	Status                   string `xml:"Status"`
	MinSize                  int32  `xml:"MinSize"`
	MaxGroupPreparedCapacity int32  `xml:"MaxGroupPreparedCapacity,omitempty"`
}

type describeWarmPoolResult struct {
	WarmPoolConfiguration xmlWarmPoolConfiguration `xml:"WarmPoolConfiguration"`
}

type describeWarmPoolResponse struct {
	XMLName          xml.Name               `xml:"DescribeWarmPoolResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           describeWarmPoolResult `xml:"DescribeWarmPoolResult"`
}
