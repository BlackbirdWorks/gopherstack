package main

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	directconnectbackend "github.com/blackbirdworks/gopherstack/services/directconnect"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
	medialivebackend "github.com/blackbirdworks/gopherstack/services/medialive"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	mediastoredatabackend "github.com/blackbirdworks/gopherstack/services/mediastoredata"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ec2SubnetAdapter serves subnet lookups for services that derive VPC, AZ and network-type data from EC2.
type ec2SubnetAdapter struct {
	regions ec2Regions
}

func (a ec2SubnetAdapter) find(id string) (ec2backend.Backend, *ec2backend.Subnet) {
	for _, b := range a.regions.handler.RegionBackends() {
		if subnets := b.DescribeSubnets([]string{id}); len(subnets) > 0 {
			return b, subnets[0]
		}
	}

	return nil, nil
}

// SubnetNetwork implements docdb.SubnetResolver and neptune.SubnetResolver.
func (a ec2SubnetAdapter) SubnetNetwork(id string) (string, bool, bool) {
	b, sn := a.find(id)
	if sn == nil {
		return "", false, false
	}

	return sn.VPCID, !sn.Ipv6Native && b.SubnetHasIPv6Block(id), true
}

// ResolveSubnets implements elasticsearch.SubnetResolver for the subnets of one region.
func (a ec2SubnetAdapter) ResolveSubnets(region string, subnetIDs []string) (string, []string) {
	b := a.regions.handler.BackendFor(region)
	if b == nil {
		return "", nil
	}

	var (
		vpcID string
		zones []string
	)

	for _, sn := range b.DescribeSubnets(subnetIDs) {
		vpcID = sn.VPCID

		if !slices.Contains(zones, sn.AvailabilityZone) {
			zones = append(zones, sn.AvailabilityZone)
		}
	}

	slices.Sort(zones)

	return vpcID, zones
}

// SubnetAZ implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) SubnetAZ(id string) (string, bool) {
	_, sn := a.find(id)
	if sn == nil {
		return "", false
	}

	return sn.AvailabilityZone, true
}

// CreateNetworkInterface implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) CreateNetworkInterface(subnetID, description string) (string, error) {
	b, sn := a.find(subnetID)
	if sn == nil {
		return "", ec2backend.ErrSubnetNotFound
	}

	eni, err := b.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return "", err
	}

	return eni.ID, nil
}

// DeleteNetworkInterface implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) DeleteNetworkInterface(id string) {
	for _, b := range a.regions.handler.RegionBackends() {
		if b.DeleteNetworkInterface(id) == nil {
			return
		}
	}
}

// wireSubnetLookups gives DocumentDB, Neptune and MediaLive their EC2 subnet accessors.
func wireSubnetLookups(byName map[string]service.Registerable) {
	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return
	}

	adapter := ec2SubnetAdapter{regions: ec2Regions{handler: ec2H}}

	if h, hok := byName["DocDB"].(*docdbbackend.Handler); hok {
		h.Backend.SetSubnetResolver(adapter)
	}

	if h, hok := byName["Neptune"].(*neptunebackend.Handler); hok {
		if bk, bok := h.Backend.(*neptunebackend.InMemoryBackend); bok {
			bk.SetSubnetResolver(adapter)
		}
	}

	if h, hok := byName["Elasticsearch"].(*elasticsearchbackend.Handler); hok {
		h.Backend.SetSubnetResolver(adapter)
	}

	if h, hok := byName["MediaLive"].(*medialivebackend.Handler); hok {
		if bk, bok := h.Backend.(*medialivebackend.InMemoryBackend); bok {
			bk.SetVPCNetwork(adapter)
		}
	}
}

// macSecSecretAdapter backs AssociateMacSecKey's raw CAK/CKN with a Secrets Manager secret.
type macSecSecretAdapter struct {
	secrets *secretsmanagerbackend.InMemoryBackend
}

func (a macSecSecretAdapter) CreateMacSecSecret(region, name, secretString string) (string, error) {
	return a.secrets.CreateManagedSecret(region, name, "", secretString)
}

// wireDirectConnectMacSecSecrets gives Direct Connect a Secrets Manager store for MACsec keys.
func wireDirectConnectMacSecSecrets(byName map[string]service.Registerable) {
	dcH, ok := byName["DirectConnect"].(*directconnectbackend.Handler)
	if !ok {
		return
	}

	smH, ok := byName["SecretsManager"].(*secretsmanagerbackend.Handler)
	if !ok {
		return
	}

	if smBk, bok := smH.Backend.(*secretsmanagerbackend.InMemoryBackend); bok {
		dcH.Backend.SetMacSecSecretCreator(macSecSecretAdapter{secrets: smBk})
	}
}

// ramShareAdapter resolves RAM resource shares for services that reference them by ARN.
type ramShareAdapter struct {
	handler *rambackend.Handler
}

// ResourceShareResources implements mq.ResourceShareResolver.
func (a ramShareAdapter) ResourceShareResources(shareARN string) ([]string, bool) {
	b := a.handler.BackendFor(arnRegion(shareARN))

	if _, err := b.GetResourceShare(shareARN); err != nil {
		return nil, false
	}

	var arns []string

	for _, assoc := range b.ListResources("SELF", []string{shareARN}, "") {
		arns = append(arns, assoc.AssociatedEntity)
	}

	return arns, true
}

// wireMQResourceShares lets Amazon MQ expand applied resource shares through RAM.
func wireMQResourceShares(byName map[string]service.Registerable) {
	ramH, ok := byName["RAM"].(*rambackend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["MQ"].(*mqbackend.Handler); hok {
		if bk, bok := h.Backend.(*mqbackend.InMemoryBackend); bok {
			bk.SetResourceShareResolver(ramShareAdapter{handler: ramH})
		}
	}
}

// mediaStoreContainers resolves MediaStore containers for per-container data endpoints.
type mediaStoreContainers struct {
	backend mediastorebackend.StorageBackend
}

func (m mediaStoreContainers) ContainerExists(region, name string) bool {
	_, err := m.backend.DescribeContainer(mediastorebackend.WithRegion(context.Background(), region), name)

	return err == nil
}

// wireMediaStoreDataContainers lets MediaStore Data reject unknown containers.
func wireMediaStoreDataContainers(byName map[string]service.Registerable) {
	msH, ok := byName["MediaStore"].(*mediastorebackend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["MediaStoreData"].(*mediastoredatabackend.Handler); hok {
		h.SetContainerResolver(mediaStoreContainers{backend: msH.Backend})
	}
}
