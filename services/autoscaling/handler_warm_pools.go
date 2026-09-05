package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

// handleDescribeWarmPool reads and validates MaxRecords/NextToken (real
// DescribeWarmPoolInput carries both, api_op_DescribeWarmPool.go: "The maximum value is 50")
// and returns them wired to a page over the pool's instances. This backend does not model
// individual warm-pool instances (PutWarmPool only tracks pool-level config -- MinSize,
// MaxGroupPreparedCapacity, PoolState), so Instances is always empty and pagination is
// correctly a no-op (nothing to truncate, so NextToken is always absent); the plumbing is
// still real, not a stub, so a client that requests a small MaxRecords or supplies a stale
// NextToken gets a normal empty page rather than an error.
func (h *Handler) handleDescribeWarmPool(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	wp, err := h.Backend.DescribeWarmPool(groupName)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultWPMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), defaultWPMaxRecords)
		}
	}

	instances := make([]xmlWarmPoolInstance, 0)
	p := page.New(instances, vals.Get("NextToken"), maxRecords, defaultWPMaxRecords)

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
			NextToken:             p.Next,
			Instances:             xmlWarmPoolInstanceList{Members: p.Data},
			WarmPoolConfiguration: xmlWP,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-warm-pool"},
	}, nil
}

// defaultWPMaxRecords is DescribeWarmPool's documented max page size
// (api_op_DescribeWarmPool.go: "The maximum value is 50"); no distinct default is documented.
const defaultWPMaxRecords = 50

// xmlWarmPoolInstance mirrors autoscaling@v1.70.4 types.Instance -- unused today (Instances is
// always empty, see handleDescribeWarmPool) but kept wire-accurate for when warm-pool instance
// tracking is added.
type xmlWarmPoolInstance struct {
	InstanceID              string `xml:"InstanceId"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	InstanceType            string `xml:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool   `xml:"ProtectedFromScaleIn,omitempty"`
}

type xmlWarmPoolInstanceList struct {
	Members []xmlWarmPoolInstance `xml:"member"`
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
	NextToken             string                   `xml:"NextToken,omitempty"`
	Instances             xmlWarmPoolInstanceList  `xml:"Instances"`
	WarmPoolConfiguration xmlWarmPoolConfiguration `xml:"WarmPoolConfiguration"`
}

type describeWarmPoolResponse struct {
	XMLName          xml.Name               `xml:"DescribeWarmPoolResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           describeWarmPoolResult `xml:"DescribeWarmPoolResult"`
}
