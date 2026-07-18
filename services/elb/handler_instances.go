package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleRegisterInstances(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	remaining, err := h.Backend.RegisterInstancesWithLoadBalancer(ctx, name, instances)
	if err != nil {
		return nil, err
	}

	xmlInsts := toXMLInstances(remaining)

	return &registerInstancesResponse{
		Xmlns: elbXMLNS,
		Result: registerInstancesResult{
			Instances: xmlInstanceList{Members: xmlInsts},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-register-" + name},
	}, nil
}

func (h *Handler) handleDeregisterInstances(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	remaining, err := h.Backend.DeregisterInstancesFromLoadBalancer(ctx, name, instances)
	if err != nil {
		return nil, err
	}

	xmlInsts := toXMLInstances(remaining)

	return &deregisterInstancesResponse{
		Xmlns: elbXMLNS,
		Result: deregisterInstancesResult{
			Instances: xmlInstanceList{Members: xmlInsts},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deregister-" + name},
	}, nil
}

func (h *Handler) handleDescribeInstanceHealth(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	states, err := h.Backend.DescribeInstanceHealth(ctx, name, instances)
	if err != nil {
		return nil, err
	}

	xmlStates := make([]xmlInstanceState, 0, len(states))
	for _, s := range states {
		xmlStates = append(xmlStates, xmlInstanceState(s))
	}

	return &describeInstanceHealthResponse{
		Xmlns: elbXMLNS,
		Result: describeInstanceHealthResult{
			InstanceStates: xmlInstanceStateList{Members: xmlStates},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-instancehealth-" + name},
	}, nil
}

// parseInstances extracts instance IDs from Instances.member.N.InstanceId form values.
// Uses gap-tolerant scanning so sparse indexes (e.g. 1,3,5) are handled correctly.
func parseInstances(vals url.Values) []Instance {
	indexes := collectMemberIndexes(vals, "Instances.member.")
	result := make([]Instance, 0, len(indexes))

	for _, i := range indexes {
		id := vals.Get(fmt.Sprintf("Instances.member.%d.InstanceId", i))
		if id != "" {
			result = append(result, Instance{InstanceID: id})
		}
	}

	return result
}

type xmlInstance struct {
	InstanceID string `xml:"InstanceId"`
}

type xmlInstanceList struct {
	Members []xmlInstance `xml:"member"`
}

func toXMLInstances(instances []Instance) []xmlInstance {
	xmlInsts := make([]xmlInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInsts = append(xmlInsts, xmlInstance(inst))
	}

	return xmlInsts
}

type registerInstancesResult struct {
	Instances xmlInstanceList `xml:"Instances"`
}

type registerInstancesResponse struct {
	XMLName          xml.Name                `xml:"RegisterInstancesWithLoadBalancerResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           registerInstancesResult `xml:"RegisterInstancesWithLoadBalancerResult"`
}

type deregisterInstancesResult struct {
	Instances xmlInstanceList `xml:"Instances"`
}

type deregisterInstancesResponse struct {
	XMLName          xml.Name                  `xml:"DeregisterInstancesFromLoadBalancerResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           deregisterInstancesResult `xml:"DeregisterInstancesFromLoadBalancerResult"`
}

type xmlInstanceState struct {
	InstanceID  string `xml:"InstanceId"`
	State       string `xml:"State"`
	ReasonCode  string `xml:"ReasonCode"`
	Description string `xml:"Description"`
}

type xmlInstanceStateList struct {
	Members []xmlInstanceState `xml:"member"`
}

type describeInstanceHealthResult struct {
	InstanceStates xmlInstanceStateList `xml:"InstanceStates"`
}

type describeInstanceHealthResponse struct {
	XMLName          xml.Name                     `xml:"DescribeInstanceHealthResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeInstanceHealthResult `xml:"DescribeInstanceHealthResult"`
}
