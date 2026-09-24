package cloudformation

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	sdbackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

const (
	resTypeSDPrivateDNSNamespace = "AWS::ServiceDiscovery::PrivateDnsNamespace"
	resTypeSDHTTPNamespace       = "AWS::ServiceDiscovery::HttpNamespace"
	resTypeSDPublicDNSNamespace  = "AWS::ServiceDiscovery::PublicDnsNamespace"
	resTypeSDService             = "AWS::ServiceDiscovery::Service"
	resTypeSDInstance            = "AWS::ServiceDiscovery::Instance"
)

// namespaceIDFromOperation resolves a Cloud Map operation ID (what
// CreateXxxNamespace returns) to the namespace ID it created, via the
// operation's Targets map -- CreateHTTPNamespace/CreatePrivateDnsNamespace/
// CreatePublicDnsNamespace all return an async operation ID, not the
// namespace ID itself (servicediscovery/namespaces.go createNamespace).
func namespaceIDFromOperation(mem *sdbackend.InMemoryBackend, opID string) (string, error) {
	op, err := mem.GetOperation(opID)
	if err != nil {
		return "", err
	}

	nsID := op.Targets["NAMESPACE"]
	if nsID == "" {
		return "", fmt.Errorf("%w: operation %s has no NAMESPACE target", sdbackend.ErrNamespaceNotFound, opID)
	}

	return nsID, nil
}

// ---- ServiceDiscovery namespaces ----

func (rc *ResourceCreator) createSDPrivateDNSNamespace(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ServiceDiscovery == nil {
		return logicalID + "-stub", nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	vpc := strProp(props, "Vpc", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	opID, err := mem.CreatePrivateDNSNamespace(name, description, vpc, 0, tags)
	if err != nil {
		return "", fmt.Errorf("create ServiceDiscovery PrivateDnsNamespace %s: %w", name, err)
	}

	nsID, err := namespaceIDFromOperation(mem, opID)
	if err != nil {
		return "", err
	}

	stashSDHostedZoneID(mem, logicalID, nsID, physicalIDs)

	return nsID, nil
}

func (rc *ResourceCreator) createSDHTTPNamespace(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ServiceDiscovery == nil {
		return logicalID + "-stub", nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	opID, err := mem.CreateHTTPNamespace(name, description, tags)
	if err != nil {
		return "", fmt.Errorf("create ServiceDiscovery HttpNamespace %s: %w", name, err)
	}

	return namespaceIDFromOperation(mem, opID)
}

func (rc *ResourceCreator) createSDPublicDNSNamespace(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ServiceDiscovery == nil {
		return logicalID + "-stub", nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	opID, err := mem.CreatePublicDNSNamespace(name, description, 0, tags)
	if err != nil {
		return "", fmt.Errorf("create ServiceDiscovery PublicDnsNamespace %s: %w", name, err)
	}

	nsID, err := namespaceIDFromOperation(mem, opID)
	if err != nil {
		return "", err
	}

	stashSDHostedZoneID(mem, logicalID, nsID, physicalIDs)

	return nsID, nil
}

// stashSDHostedZoneID stores a DNS namespace's backend-computed HostedZoneId
// under a synthetic physicalIDs key (same convention as IAM::AccessKey's
// SecretAccessKey) so Fn::GetAtt can retrieve it without a backend lookup at
// resolve time -- the ID depends on hash/sequence state the pure
// getExtraResourceAttribute function has no access to.
func stashSDHostedZoneID(mem *sdbackend.InMemoryBackend, logicalID, nsID string, physicalIDs map[string]string) {
	ns, err := mem.GetNamespace(nsID)
	if err != nil || ns.Properties == nil || ns.Properties.DNSProperties == nil {
		return
	}

	physicalIDs[logicalID+"/HostedZoneId"] = ns.Properties.DNSProperties.HostedZoneID
}

func (rc *ResourceCreator) deleteSDNamespace(id string) error {
	if rc.backends.ServiceDiscovery == nil {
		return nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	_, err := mem.DeleteNamespace(id)
	if errors.Is(err, sdbackend.ErrNamespaceNotFound) {
		return nil
	}

	return err
}

// sdNamespaceArn reproduces the ARN formula store.go's namespaceARN computes,
// letting Fn::GetAtt derive Arn without a backend lookup.
func sdNamespaceArn(id, accountID, region string) string {
	return arn.Build("servicediscovery", region, accountID, "namespace/"+id)
}

// sdServiceArn reproduces the ARN formula store.go's serviceARN computes.
func sdServiceArn(id, accountID, region string) string {
	return arn.Build("servicediscovery", region, accountID, "service/"+id)
}

// ---- ServiceDiscovery Service ----

func (rc *ResourceCreator) createSDService(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ServiceDiscovery == nil {
		return logicalID + "-stub", nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	namespaceID := strProp(props, "NamespaceId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	svcType := strProp(props, "Type", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	dnsConfig := sdDNSConfigProp(props, params, physicalIDs)
	hcc := sdHealthCheckConfigProp(props, params, physicalIDs)
	hccc := sdHealthCheckCustomConfigProp(props)

	svc, err := mem.CreateService(name, namespaceID, description, svcType, dnsConfig, hcc, hccc, tags)
	if err != nil {
		return "", fmt.Errorf("create ServiceDiscovery Service %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Name"] = svc.Name

	return svc.ID, nil
}

func (rc *ResourceCreator) deleteSDService(id string) error {
	if rc.backends.ServiceDiscovery == nil {
		return nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	err := mem.DeleteService(id)
	if errors.Is(err, sdbackend.ErrServiceNotFound) {
		return nil
	}

	return err
}

func sdDNSConfigProp(props map[string]any, params, physicalIDs map[string]string) *sdbackend.DNSConfig {
	m, ok := props["DnsConfig"].(map[string]any)
	if !ok {
		return nil
	}

	cfg := &sdbackend.DNSConfig{
		RoutingPolicy: strProp(m, "RoutingPolicy", params, physicalIDs),
	}

	if raw, recOK := m["DnsRecords"].([]any); recOK {
		for _, item := range raw {
			rm, itemOK := item.(map[string]any)
			if !itemOK {
				continue
			}

			cfg.DNSRecords = append(cfg.DNSRecords, sdbackend.DNSRecord{
				Type: strProp(rm, "Type", params, physicalIDs),
				TTL:  int64Prop(rm, "TTL", params, physicalIDs),
			})
		}
	}

	return cfg
}

func sdHealthCheckConfigProp(
	props map[string]any, params, physicalIDs map[string]string,
) *sdbackend.HealthCheckConfig {
	m, ok := props["HealthCheckConfig"].(map[string]any)
	if !ok {
		return nil
	}

	cfg := &sdbackend.HealthCheckConfig{
		Type:         strProp(m, "Type", params, physicalIDs),
		ResourcePath: strProp(m, "ResourcePath", params, physicalIDs),
	}

	if v, vOK := m["FailureThreshold"].(float64); vOK {
		cfg.FailureThreshold = int(v)
	}

	return cfg
}

func sdHealthCheckCustomConfigProp(props map[string]any) *sdbackend.HealthCheckCustomConfig {
	m, ok := props["HealthCheckCustomConfig"].(map[string]any)
	if !ok {
		return nil
	}

	cfg := &sdbackend.HealthCheckCustomConfig{}
	if v, vOK := m["FailureThreshold"].(float64); vOK {
		cfg.FailureThreshold = int(v)
	}

	return cfg
}

// ---- ServiceDiscovery Instance ----

func (rc *ResourceCreator) createSDInstance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ServiceDiscovery == nil {
		return logicalID + "-stub", nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	serviceID := strProp(props, "ServiceId", params, physicalIDs)

	instanceID := strProp(props, "InstanceId", params, physicalIDs)
	if instanceID == "" {
		instanceID = logicalID
	}

	attrs := map[string]string{}
	if m, attrOK := props["InstanceAttributes"].(map[string]any); attrOK {
		for k, v := range m {
			attrs[k] = resolve(v, params, physicalIDs)
		}
	}

	if _, err := mem.RegisterInstance(serviceID, instanceID, attrs); err != nil {
		return "", fmt.Errorf("create ServiceDiscovery Instance %s: %w", instanceID, err)
	}

	// Ref returns just the InstanceId (real AWS's documented Return values),
	// so ServiceId -- needed by DeregisterInstance -- is stashed the same way
	// as IAM::AccessKey's SecretAccessKey (see createIAMAccessKey).
	physicalIDs[logicalID+"/ServiceId"] = serviceID

	return instanceID, nil
}

// deleteSDInstance resolves ServiceId from the resource's own props, using
// stackPhysicalIDs (a snapshot of every logical ID -> physical ID in the
// stack, built by the caller from live StackResources) so a
// {"Ref": "MyService"} property resolves to the real service ID rather than
// the logical ID literal.
func (rc *ResourceCreator) deleteSDInstance(
	props map[string]any, stackPhysicalIDs map[string]string, instanceID string,
) error {
	if rc.backends.ServiceDiscovery == nil {
		return nil
	}

	mem, ok := rc.backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	serviceID := strProp(props, "ServiceId", nil, stackPhysicalIDs)
	if serviceID == "" {
		return nil
	}

	_, err := mem.DeregisterInstance(serviceID, instanceID)
	if errors.Is(err, sdbackend.ErrInstanceNotFound) || errors.Is(err, sdbackend.ErrServiceNotFound) {
		return nil
	}

	return err
}
