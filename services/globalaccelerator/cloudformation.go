package globalaccelerator

import (
	"slices"
	"strings"
)

// CloudFormation resource types provisioned through CreateCFNResource.
const (
	CFNAccelerator            = "AWS::GlobalAccelerator::Accelerator"
	CFNListener               = "AWS::GlobalAccelerator::Listener"
	CFNEndpointGroup          = "AWS::GlobalAccelerator::EndpointGroup"
	CFNCrossAccountAttachment = "AWS::GlobalAccelerator::CrossAccountAttachment"
)

// IsCFNType reports whether t is a Global Accelerator CloudFormation type.
func IsCFNType(t string) bool {
	switch t {
	case CFNAccelerator, CFNListener, CFNEndpointGroup, CFNCrossAccountAttachment:
		return true
	}

	return false
}

// CreateCFNResource provisions the resource a CloudFormation type describes and returns its physical ID
// (the Ref value, an ARN) and its Fn::GetAtt attributes. Property names are the CloudFormation spellings.
func (b *InMemoryBackend) CreateCFNResource(
	resourceType string, props map[string]any,
) (string, map[string]string, error) {
	switch resourceType {
	case CFNAccelerator:
		return b.createCFNAccelerator(props)
	case CFNListener:
		return b.createCFNListener(props)
	case CFNEndpointGroup:
		return b.createCFNEndpointGroup(props)
	case CFNCrossAccountAttachment:
		return b.createCFNAttachment(props)
	}

	return "", nil, fail(ErrInvalidArgument, "unsupported CloudFormation type %s", resourceType)
}

// CFNAcceleratorDeployed reports whether the accelerator has left IN_PROGRESS.
func (b *InMemoryBackend) CFNAcceleratorDeployed(arn string) (bool, error) {
	a, err := b.GetAccelerator(arn, false)
	if err != nil {
		return false, err
	}

	return a.Status == statusDeployed, nil
}

func (b *InMemoryBackend) createCFNAccelerator(props map[string]any) (string, map[string]string, error) {
	enabled := true
	if v, ok := cfnBool(props["Enabled"]); ok {
		enabled = v
	}

	a, err := b.CreateAccelerator(AcceleratorInput{
		Name:          cfnString(props["Name"]),
		IPAddressType: cfnString(props["IpAddressType"]),
		IPAddresses:   cfnStrings(props["IpAddresses"]),
		Tags:          cfnTags(props["Tags"]),
		Enabled:       enabled,
	})
	if err != nil {
		return "", nil, err
	}

	return a.ARN, acceleratorAttrs(a), nil
}

func acceleratorAttrs(a *Accelerator) map[string]string {
	return map[string]string{
		"AcceleratorArn":   a.ARN,
		"DnsName":          a.DNSName,
		"DualStackDnsName": a.DualStackDNSName(),
		"Ipv4Addresses":    strings.Join(a.IPv4, ","),
		"Ipv6Addresses":    strings.Join(a.IPv6, ","),
	}
}

func (b *InMemoryBackend) createCFNListener(props map[string]any) (string, map[string]string, error) {
	l, err := b.CreateListener(cfnString(props["AcceleratorArn"]), listenerInputFromCFN(props))
	if err != nil {
		return "", nil, err
	}

	return l.ARN, map[string]string{"ListenerArn": l.ARN}, nil
}

func listenerInputFromCFN(props map[string]any) ListenerInput {
	return ListenerInput{
		Protocol:       cfnString(props["Protocol"]),
		ClientAffinity: cfnString(props["ClientAffinity"]),
		PortRanges:     cfnPortRanges(props["PortRanges"]),
	}
}

func (b *InMemoryBackend) createCFNEndpointGroup(props map[string]any) (string, map[string]string, error) {
	in := endpointGroupInputFromCFN(props)

	g, err := b.CreateEndpointGroup(cfnString(props["ListenerArn"]), in)
	if err != nil {
		return "", nil, err
	}

	return g.ARN, map[string]string{"EndpointGroupArn": g.ARN}, nil
}

func endpointGroupInputFromCFN(props map[string]any) EndpointGroupInput {
	return EndpointGroupInput{
		Region:              cfnString(props["EndpointGroupRegion"]),
		HealthCheckProtocol: cfnString(props["HealthCheckProtocol"]),
		HealthCheckPath:     cfnOptString(props["HealthCheckPath"]),
		HealthCheckPort:     cfnOptInt32(props["HealthCheckPort"]),
		HealthCheckInterval: cfnOptInt32(props["HealthCheckIntervalSeconds"]),
		ThresholdCount:      cfnOptInt32(props["ThresholdCount"]),
		TrafficDial:         cfnOptFloat32(props["TrafficDialPercentage"]),
		Endpoints:           cfnEndpointConfigs(props["EndpointConfigurations"]),
		PortOverrides:       cfnPortOverrides(props["PortOverrides"]),
	}
}

func (b *InMemoryBackend) createCFNAttachment(props map[string]any) (string, map[string]string, error) {
	a, err := b.CreateCrossAccountAttachment(AttachmentInput{
		Name:       cfnString(props["Name"]),
		Owner:      b.accountID,
		Principals: cfnStrings(props["Principals"]),
		Resources:  cfnResources(props["Resources"]),
		Tags:       cfnTags(props["Tags"]),
	})
	if err != nil {
		return "", nil, err
	}

	return a.ARN, map[string]string{"AttachmentArn": a.ARN}, nil
}

// UpdateCFNResource applies changed properties in place; replacement-only properties are the caller's concern.
func (b *InMemoryBackend) UpdateCFNResource(resourceType, id string, props map[string]any) error {
	switch resourceType {
	case CFNAccelerator:
		return b.updateCFNAccelerator(id, props)
	case CFNListener:
		_, err := b.UpdateListener(id, listenerInputFromCFN(props))

		return err
	case CFNEndpointGroup:
		return b.updateCFNEndpointGroup(id, props)
	case CFNCrossAccountAttachment:
		return b.updateCFNAttachment(id, props)
	}

	return fail(ErrInvalidArgument, "unsupported CloudFormation type %s", resourceType)
}

func (b *InMemoryBackend) updateCFNAccelerator(id string, props map[string]any) error {
	enabled := true
	if v, ok := cfnBool(props["Enabled"]); ok {
		enabled = v
	}

	patch := AcceleratorPatch{
		Name:        cfnOptString(props["Name"]),
		Enabled:     &enabled,
		IPAddresses: cfnStrings(props["IpAddresses"]),
	}

	if t := cfnString(props["IpAddressType"]); t != "" {
		patch.IPAddressType = &t
	}

	if _, err := b.UpdateAccelerator(id, false, patch); err != nil {
		return err
	}

	return b.syncTags(id, cfnTags(props["Tags"]))
}

func (b *InMemoryBackend) syncTags(arn string, want map[string]string) error {
	cur, err := b.ListTagsForResource(arn)
	if err != nil {
		return err
	}

	var drop []string

	for k := range cur {
		if _, ok := want[k]; !ok {
			drop = append(drop, k)
		}
	}

	if len(drop) > 0 {
		if err = b.UntagResource(arn, drop); err != nil {
			return err
		}
	}

	if len(want) == 0 {
		return nil
	}

	return b.TagResource(arn, want)
}

func (b *InMemoryBackend) updateCFNEndpointGroup(id string, props map[string]any) error {
	in := endpointGroupInputFromCFN(props)
	if in.Endpoints == nil {
		in.Endpoints = []EndpointConfig{}
	}

	_, err := b.UpdateEndpointGroup(id, in)

	return err
}

func (b *InMemoryBackend) updateCFNAttachment(id string, props map[string]any) error {
	cur, err := b.GetCrossAccountAttachment(id)
	if err != nil {
		return err
	}

	wantP, wantR := cfnStrings(props["Principals"]), cfnResources(props["Resources"])
	patch := AttachmentPatch{Name: cfnOptString(props["Name"])}

	for _, p := range cur.Principals {
		if !slices.Contains(wantP, p) {
			patch.RemovePrincipals = append(patch.RemovePrincipals, p)
		}
	}

	patch.AddPrincipals = wantP

	for _, r := range cur.Resources {
		if !slices.Contains(wantR, r) {
			patch.RemoveResources = append(patch.RemoveResources, r)
		}
	}

	patch.AddResources = wantR

	if _, err = b.UpdateCrossAccountAttachment(id, patch); err != nil {
		return err
	}

	return b.syncTags(id, cfnTags(props["Tags"]))
}

// DeleteCFNResource removes a CloudFormation-provisioned resource; an already-missing one is success.
func (b *InMemoryBackend) DeleteCFNResource(resourceType, id string) error {
	var err error

	switch resourceType {
	case CFNAccelerator:
		err = b.deleteCFNAccelerator(id)
	case CFNListener:
		err = b.DeleteListener(id, false)
	case CFNEndpointGroup:
		err = b.DeleteEndpointGroup(id, false)
	case CFNCrossAccountAttachment:
		err = b.DeleteCrossAccountAttachment(id)
	default:
		err = fail(ErrInvalidArgument, "unsupported CloudFormation type %s", resourceType)
	}

	if isNotFound(err) {
		return nil
	}

	return err
}

func (b *InMemoryBackend) deleteCFNAccelerator(id string) error {
	off := false
	if _, err := b.UpdateAccelerator(id, false, AcceleratorPatch{Enabled: &off}); err != nil {
		return err
	}

	return b.DeleteAccelerator(id, false)
}

func isNotFound(err error) bool {
	return isErr(err, ErrAcceleratorNotFound) || isErr(err, ErrListenerNotFound) ||
		isErr(err, ErrEndpointGroupNotFound) || isErr(err, ErrAttachmentNotFound)
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every tagged standard and custom routing accelerator and attachment.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.accelerators.Range(func(a *Accelerator) bool {
		if len(a.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: a.ARN, Tags: cloneTags(a.Tags)})
		}

		return true
	})
	b.attachments.Range(func(a *Attachment) bool {
		if len(a.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: a.ARN, Tags: cloneTags(a.Tags)})
		}

		return true
	})

	return out
}
