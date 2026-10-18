package globalaccelerator

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Wire shapes follow the pinned aws-sdk-go-v2/service/globalaccelerator v1.45.3 awsjson1.1 schemas;
// timestamps are epoch-seconds numbers.

type (
	emptyResp struct{}

	listPageReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	arnTagsReq struct {
		ResourceArn string    `json:"ResourceArn"`
		Tags        []tags.KV `json:"Tags"`
		TagKeys     []string  `json:"TagKeys"`
	}

	listTagsResp struct {
		Tags []tags.KV `json:"Tags"`
	}
)

type eventWire struct {
	Message   string  `json:"Message"`
	Timestamp float64 `json:"Timestamp"`
}

func eventsToWire(events []Event) []eventWire {
	if len(events) == 0 {
		return nil
	}

	out := make([]eventWire, len(events))
	for i, e := range events {
		out[i] = eventWire{Message: e.Message, Timestamp: awstime.Epoch(e.Timestamp)}
	}

	return out
}

type ipSetWire struct {
	IPAddressFamily string   `json:"IpAddressFamily"`
	IPAddresses     []string `json:"IpAddresses"`
}

type acceleratorWire struct {
	AcceleratorArn   string      `json:"AcceleratorArn"`
	Name             string      `json:"Name"`
	IPAddressType    string      `json:"IpAddressType"`
	DNSName          string      `json:"DnsName"`
	DualStackDNSName string      `json:"DualStackDnsName,omitempty"`
	Status           string      `json:"Status"`
	IPSets           []ipSetWire `json:"IpSets"`
	Events           []eventWire `json:"Events,omitempty"`
	CreatedTime      float64     `json:"CreatedTime"`
	LastModifiedTime float64     `json:"LastModifiedTime"`
	Enabled          bool        `json:"Enabled"`
}

func acceleratorToWire(a *Accelerator) acceleratorWire {
	sets := []ipSetWire{{IPAddressFamily: "IPv4", IPAddresses: a.IPv4}}
	if len(a.IPv6) > 0 {
		sets = append(sets, ipSetWire{IPAddressFamily: "IPv6", IPAddresses: a.IPv6})
	}

	return acceleratorWire{
		AcceleratorArn:   a.ARN,
		Name:             a.Name,
		IPAddressType:    a.IPAddressType,
		DNSName:          a.DNSName,
		DualStackDNSName: a.DualStackDNSName(),
		Status:           a.Status,
		IPSets:           sets,
		Events:           eventsToWire(a.Events),
		CreatedTime: awstime.Epoch(
			a.CreatedTime,
		),
		LastModifiedTime: awstime.Epoch(a.LastModifiedTime),
		Enabled:          a.Enabled,
	}
}

type (
	createAcceleratorReq struct {
		Enabled          *bool     `json:"Enabled"`
		Name             string    `json:"Name"`
		IPAddressType    string    `json:"IpAddressType"`
		IdempotencyToken string    `json:"IdempotencyToken"`
		IPAddresses      []string  `json:"IpAddresses"`
		Tags             []tags.KV `json:"Tags"`
	}

	acceleratorArnReq struct {
		AcceleratorArn string `json:"AcceleratorArn"`
	}

	updateAcceleratorReq struct {
		Enabled        *bool    `json:"Enabled"`
		Name           *string  `json:"Name"`
		IPAddressType  *string  `json:"IpAddressType"`
		AcceleratorArn string   `json:"AcceleratorArn"`
		IPAddresses    []string `json:"IpAddresses"`
	}

	acceleratorResp struct {
		Accelerator acceleratorWire `json:"Accelerator"`
	}

	listAcceleratorsResp struct {
		NextToken    string            `json:"NextToken,omitempty"`
		Accelerators []acceleratorWire `json:"Accelerators"`
	}

	attributesWire struct {
		FlowLogsS3Bucket string `json:"FlowLogsS3Bucket,omitempty"`
		FlowLogsS3Prefix string `json:"FlowLogsS3Prefix,omitempty"`
		FlowLogsEnabled  bool   `json:"FlowLogsEnabled"`
	}

	attributesResp struct {
		AcceleratorAttributes attributesWire `json:"AcceleratorAttributes"`
	}

	updateAttributesReq struct {
		FlowLogsEnabled  *bool   `json:"FlowLogsEnabled"`
		FlowLogsS3Bucket *string `json:"FlowLogsS3Bucket"`
		FlowLogsS3Prefix *string `json:"FlowLogsS3Prefix"`
		AcceleratorArn   string  `json:"AcceleratorArn"`
	}
)

func attributesToWire(a *Accelerator) attributesResp {
	return attributesResp{AcceleratorAttributes: attributesWire{
		FlowLogsEnabled: a.FlowLogsEnabled, FlowLogsS3Bucket: a.FlowLogsS3Bucket, FlowLogsS3Prefix: a.FlowLogsS3Prefix,
	}}
}

type listenerWire struct {
	ListenerArn    string      `json:"ListenerArn"`
	Protocol       string      `json:"Protocol,omitempty"`
	ClientAffinity string      `json:"ClientAffinity,omitempty"`
	PortRanges     []PortRange `json:"PortRanges"`
}

func listenerToWire(l *Listener) listenerWire {
	return listenerWire{
		ListenerArn: l.ARN, Protocol: l.Protocol, ClientAffinity: l.ClientAffinity, PortRanges: l.PortRanges,
	}
}

type (
	createListenerReq struct {
		AcceleratorArn   string      `json:"AcceleratorArn"`
		IdempotencyToken string      `json:"IdempotencyToken"`
		Protocol         string      `json:"Protocol"`
		ClientAffinity   string      `json:"ClientAffinity"`
		PortRanges       []PortRange `json:"PortRanges"`
	}

	listenerArnReq struct {
		ListenerArn string `json:"ListenerArn"`
	}

	updateListenerReq struct {
		ListenerArn    string      `json:"ListenerArn"`
		Protocol       string      `json:"Protocol"`
		ClientAffinity string      `json:"ClientAffinity"`
		PortRanges     []PortRange `json:"PortRanges"`
	}

	listenerResp struct {
		Listener listenerWire `json:"Listener"`
	}

	listByAcceleratorReq struct {
		AcceleratorArn string `json:"AcceleratorArn"`
		listPageReq
	}

	listListenersResp struct {
		NextToken string         `json:"NextToken,omitempty"`
		Listeners []listenerWire `json:"Listeners"`
	}
)

type endpointDescWire struct {
	ClientIPPreservationEnabled *bool  `json:"ClientIPPreservationEnabled,omitempty"`
	Weight                      *int32 `json:"Weight,omitempty"`
	EndpointID                  string `json:"EndpointId"`
	HealthState                 string `json:"HealthState,omitempty"`
}

type destinationWire struct {
	Protocols []string `json:"Protocols"`
	FromPort  int32    `json:"FromPort"`
	ToPort    int32    `json:"ToPort"`
}

type endpointGroupWire struct {
	HealthCheckPort            *int32             `json:"HealthCheckPort,omitempty"`
	HealthCheckIntervalSeconds *int32             `json:"HealthCheckIntervalSeconds,omitempty"`
	ThresholdCount             *int32             `json:"ThresholdCount,omitempty"`
	TrafficDialPercentage      *float32           `json:"TrafficDialPercentage,omitempty"`
	EndpointGroupArn           string             `json:"EndpointGroupArn"`
	EndpointGroupRegion        string             `json:"EndpointGroupRegion"`
	HealthCheckProtocol        string             `json:"HealthCheckProtocol,omitempty"`
	HealthCheckPath            string             `json:"HealthCheckPath,omitempty"`
	EndpointDescriptions       []endpointDescWire `json:"EndpointDescriptions"`
	PortOverrides              []PortOverride     `json:"PortOverrides,omitempty"`
	DestinationDescriptions    []destinationWire  `json:"DestinationDescriptions,omitempty"`
}

func endpointDescriptions(eps []Endpoint, custom bool) []endpointDescWire {
	out := make([]endpointDescWire, len(eps))

	for i := range eps {
		out[i] = endpointDescWire{EndpointID: eps[i].ID}
		if !custom {
			out[i].HealthState = eps[i].HealthState
			out[i].Weight = &eps[i].Weight
			out[i].ClientIPPreservationEnabled = &eps[i].ClientIPPreservation
		}
	}

	return out
}

func groupToWire(g *EndpointGroup) endpointGroupWire {
	w := endpointGroupWire{
		EndpointGroupArn: g.ARN, EndpointGroupRegion: g.Region,
		EndpointDescriptions: endpointDescriptions(g.Endpoints, g.Custom),
	}

	if g.Custom {
		w.DestinationDescriptions = make([]destinationWire, len(g.Destinations))
		for i, d := range g.Destinations {
			w.DestinationDescriptions[i] = destinationWire(d)
		}

		return w
	}

	w.HealthCheckPort, w.HealthCheckIntervalSeconds = &g.HealthCheckPort, &g.HealthCheckInterval
	w.ThresholdCount, w.TrafficDialPercentage = &g.ThresholdCount, &g.TrafficDial
	w.HealthCheckProtocol, w.HealthCheckPath = g.HealthCheckProtocol, g.HealthCheckPath
	w.PortOverrides = g.PortOverrides

	return w
}

type (
	endpointConfigWire struct {
		ClientIPPreservationEnabled *bool  `json:"ClientIPPreservationEnabled"`
		Weight                      *int32 `json:"Weight"`
		EndpointID                  string `json:"EndpointId"`
		AttachmentArn               string `json:"AttachmentArn"`
	}

	endpointGroupSettingsReq struct {
		HealthCheckIntervalSeconds *int32               `json:"HealthCheckIntervalSeconds"`
		HealthCheckPath            *string              `json:"HealthCheckPath"`
		HealthCheckPort            *int32               `json:"HealthCheckPort"`
		ThresholdCount             *int32               `json:"ThresholdCount"`
		TrafficDialPercentage      *float32             `json:"TrafficDialPercentage"`
		HealthCheckProtocol        string               `json:"HealthCheckProtocol"`
		EndpointConfigurations     []endpointConfigWire `json:"EndpointConfigurations"`
		PortOverrides              []PortOverride       `json:"PortOverrides"`
	}

	createEndpointGroupReq struct {
		ListenerArn         string `json:"ListenerArn"`
		EndpointGroupRegion string `json:"EndpointGroupRegion"`
		IdempotencyToken    string `json:"IdempotencyToken"`
		endpointGroupSettingsReq
	}

	updateEndpointGroupReq struct {
		EndpointGroupArn string `json:"EndpointGroupArn"`
		endpointGroupSettingsReq
	}

	endpointGroupArnReq struct {
		EndpointGroupArn string `json:"EndpointGroupArn"`
	}

	endpointGroupResp struct {
		EndpointGroup endpointGroupWire `json:"EndpointGroup"`
	}

	listByListenerReq struct {
		ListenerArn string `json:"ListenerArn"`
		listPageReq
	}

	listEndpointGroupsResp struct {
		NextToken      string              `json:"NextToken,omitempty"`
		EndpointGroups []endpointGroupWire `json:"EndpointGroups"`
	}

	addEndpointsReq struct {
		EndpointGroupArn       string               `json:"EndpointGroupArn"`
		EndpointConfigurations []endpointConfigWire `json:"EndpointConfigurations"`
	}

	addEndpointsResp struct {
		EndpointGroupArn     string             `json:"EndpointGroupArn"`
		EndpointDescriptions []endpointDescWire `json:"EndpointDescriptions"`
	}

	endpointIdentifierWire struct {
		EndpointID string `json:"EndpointId"`
	}

	removeEndpointsReq struct {
		EndpointGroupArn    string                   `json:"EndpointGroupArn"`
		EndpointIdentifiers []endpointIdentifierWire `json:"EndpointIdentifiers"`
	}

	createCustomEndpointGroupReq struct {
		ListenerArn               string            `json:"ListenerArn"`
		EndpointGroupRegion       string            `json:"EndpointGroupRegion"`
		IdempotencyToken          string            `json:"IdempotencyToken"`
		DestinationConfigurations []destinationWire `json:"DestinationConfigurations"`
	}

	removeCustomEndpointsReq struct {
		EndpointGroupArn string   `json:"EndpointGroupArn"`
		EndpointIDs      []string `json:"EndpointIds"`
	}

	trafficReq struct {
		AllowAll         *bool    `json:"AllowAllTrafficToEndpoint"`
		DenyAll          *bool    `json:"DenyAllTrafficToEndpoint"`
		EndpointGroupArn string   `json:"EndpointGroupArn"`
		EndpointID       string   `json:"EndpointId"`
		Addresses        []string `json:"DestinationAddresses"`
		Ports            []int32  `json:"DestinationPorts"`
	}

	portMappingsReq struct {
		AcceleratorArn   string `json:"AcceleratorArn"`
		EndpointGroupArn string `json:"EndpointGroupArn"`
		listPageReq
	}

	socketWire struct {
		IPAddress string `json:"IpAddress"`
		Port      int32  `json:"Port"`
	}

	portMappingWire struct {
		DestinationSocketAddress socketWire `json:"DestinationSocketAddress"`
		DestinationTrafficState  string     `json:"DestinationTrafficState"`
		EndpointGroupArn         string     `json:"EndpointGroupArn"`
		EndpointID               string     `json:"EndpointId"`
		Protocols                []string   `json:"Protocols"`
		AcceleratorPort          int32      `json:"AcceleratorPort"`
	}

	portMappingsResp struct {
		NextToken    string            `json:"NextToken,omitempty"`
		PortMappings []portMappingWire `json:"PortMappings"`
	}

	byDestinationReq struct {
		EndpointID         string `json:"EndpointId"`
		DestinationAddress string `json:"DestinationAddress"`
		listPageReq
	}

	destinationMappingWire struct {
		AcceleratorArn             string       `json:"AcceleratorArn"`
		DestinationSocketAddress   socketWire   `json:"DestinationSocketAddress"`
		DestinationTrafficState    string       `json:"DestinationTrafficState"`
		EndpointGroupArn           string       `json:"EndpointGroupArn"`
		EndpointGroupRegion        string       `json:"EndpointGroupRegion"`
		EndpointID                 string       `json:"EndpointId"`
		IPAddressType              string       `json:"IpAddressType"`
		AcceleratorSocketAddresses []socketWire `json:"AcceleratorSocketAddresses"`
	}

	byDestinationResp struct {
		NextToken               string                   `json:"NextToken,omitempty"`
		DestinationPortMappings []destinationMappingWire `json:"DestinationPortMappings"`
	}
)

type (
	attachmentWire struct {
		AttachmentArn    string     `json:"AttachmentArn"`
		Name             string     `json:"Name"`
		Principals       []string   `json:"Principals"`
		Resources        []Resource `json:"Resources"`
		CreatedTime      float64    `json:"CreatedTime"`
		LastModifiedTime float64    `json:"LastModifiedTime"`
	}

	attachmentResp struct {
		CrossAccountAttachment attachmentWire `json:"CrossAccountAttachment"`
	}

	createAttachmentReq struct {
		Name             string     `json:"Name"`
		IdempotencyToken string     `json:"IdempotencyToken"`
		Principals       []string   `json:"Principals"`
		Resources        []Resource `json:"Resources"`
		Tags             []tags.KV  `json:"Tags"`
	}

	attachmentArnReq struct {
		AttachmentArn string `json:"AttachmentArn"`
	}

	updateAttachmentReq struct {
		Name             *string    `json:"Name"`
		AttachmentArn    string     `json:"AttachmentArn"`
		AddPrincipals    []string   `json:"AddPrincipals"`
		RemovePrincipals []string   `json:"RemovePrincipals"`
		AddResources     []Resource `json:"AddResources"`
		RemoveResources  []Resource `json:"RemoveResources"`
	}

	listAttachmentsResp struct {
		NextToken   string           `json:"NextToken,omitempty"`
		Attachments []attachmentWire `json:"CrossAccountAttachments"`
	}

	resourceAccountsResp struct {
		OwnerAccountIDs []string `json:"ResourceOwnerAwsAccountIds"`
	}

	listCrossAccountResourcesReq struct {
		OwnerAccountID string `json:"ResourceOwnerAwsAccountId"`
		AcceleratorArn string `json:"AcceleratorArn"`
		listPageReq
	}

	crossAccountResourceWire struct {
		AttachmentArn string `json:"AttachmentArn"`
		Cidr          string `json:"Cidr,omitempty"`
		EndpointID    string `json:"EndpointId,omitempty"`
	}

	listCrossAccountResourcesResp struct {
		NextToken string                     `json:"NextToken,omitempty"`
		Resources []crossAccountResourceWire `json:"CrossAccountResources"`
	}
)

func attachmentToWire(a *Attachment) attachmentWire {
	return attachmentWire{
		AttachmentArn: a.ARN, Name: a.Name, Principals: nonNil(a.Principals), Resources: nonNil(a.Resources),
		CreatedTime: awstime.Epoch(a.CreatedTime), LastModifiedTime: awstime.Epoch(a.LastModifiedTime),
	}
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

type (
	byoipWire struct {
		Cidr   string      `json:"Cidr"`
		State  string      `json:"State"`
		Events []eventWire `json:"Events,omitempty"`
	}

	byoipResp struct {
		ByoipCidr byoipWire `json:"ByoipCidr"`
	}

	cidrReq struct {
		Cidr string `json:"Cidr"`
	}

	provisionByoipReq struct {
		AuthorizationContext *struct {
			Message   string `json:"Message"`
			Signature string `json:"Signature"`
		} `json:"CidrAuthorizationContext"`
		Cidr string `json:"Cidr"`
	}

	listByoipResp struct {
		NextToken  string      `json:"NextToken,omitempty"`
		ByoipCidrs []byoipWire `json:"ByoipCidrs"`
	}
)

func byoipToWire(c *ByoipCidr) byoipWire {
	return byoipWire{Cidr: c.Cidr, State: c.State, Events: eventsToWire(c.Events)}
}
