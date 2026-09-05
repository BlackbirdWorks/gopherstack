package lightsail

// This file backs family C (8 ops: GetInstanceAccessDetails,
// GetInstancePortStates, OpenInstancePublicPorts, CloseInstancePublicPorts,
// PutInstancePublicPorts, SetupInstanceHttps, GetSetupHistory,
// DeleteKnownHostKeys).

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	opTypeOpenInstancePublicPorts  = "OpenInstancePublicPorts"
	opTypeCloseInstancePublicPorts = "CloseInstancePublicPorts"
	opTypePutInstancePublicPorts   = "PutInstancePublicPorts"
	opTypeSetupInstanceHTTPS       = "SetupInstanceHttps"
	opTypeDeleteKnownHostKeys      = "DeleteKnownHostKeys"

	instanceAccessExpiry = 30 * time.Minute
)

// SetupHistoryEntry mirrors types.SetupHistory.
type SetupHistoryEntry struct {
	CreatedAt           time.Time
	Location            ResourceLocation
	OperationID         string
	ResourceName        string
	Status              string
	CertificateProvider string
	InstanceName        string
	DomainNames         []string
	ExecutionDetails    []SetupExecutionDetail
}

// SetupExecutionDetail mirrors types.SetupExecutionDetails.
type SetupExecutionDetail struct {
	DateTime       time.Time
	Name           string
	Command        string
	Status         string
	Version        string
	StandardOutput string
	StandardError  string
}

func (s *SetupHistoryEntry) clone() *SetupHistoryEntry {
	cp := *s
	cp.DomainNames = cloneStrings(s.DomainNames)
	cp.ExecutionDetails = append([]SetupExecutionDetail(nil), s.ExecutionDetails...)

	return &cp
}

// InstanceAccessDetails is the ephemeral response GetInstanceAccessDetails
// builds -- never stored, since real access credentials are issued fresh
// per request (PARITY.md family C).
type InstanceAccessDetails struct {
	InstanceName string
	Username     string
	IPAddress    string
	Protocol     string
	PrivateKey   string
	CertKey      string
	Password     string
	ExpiresAt    time.Time
	HostKeys     []HostKeyAttributes
}

// GetInstanceAccessDetails returns connection details for the named
// instance. SSH PrivateKey material is only returned when the instance uses
// this backend's own default key pair (the one case a real private key is
// still retrievable, PARITY.md 4.1/G) -- otherwise it is empty, matching
// CreateKeyPair's write-once-readable private key never being retained.
// Windows instances get an RDP Password this backend itself generates and
// remembers per-instance (see models.go's Instance.windowsPassword doc
// comment) rather than any claim of decrypting real AWS-side data.
func (b *InMemoryBackend) GetInstanceAccessDetails(name, protocol string) (*InstanceAccessDetails, error) {
	b.mu.Lock("GetInstanceAccessDetails")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	proto := protocol
	if proto == "" {
		proto = "ssh"

		if i.Username == "Administrator" {
			proto = "rdp"
		}
	}

	details := &InstanceAccessDetails{
		InstanceName: name,
		Username:     i.Username,
		IPAddress:    i.PublicIPAddress,
		Protocol:     proto,
		ExpiresAt:    nowUTC().Add(instanceAccessExpiry),
		HostKeys:     append([]HostKeyAttributes(nil), i.HostKeys...),
	}

	if proto == "rdp" {
		if i.windowsPassword == "" {
			i.windowsPassword = newSupportCode()
		}

		details.Password = i.windowsPassword

		return details, nil
	}

	if i.SSHKeyName == "" || (b.defaultKeyPair != nil && i.SSHKeyName == b.defaultKeyPair.Name) {
		details.PrivateKey = b.defaultKeyPairPrivateKeyLocked()
		details.CertKey = "cert-" + newUUID()
	}

	return details, nil
}

// GetInstancePortStates returns the named instance's configured firewall
// rules, each hardcoded PortStateOpen -- PortState's own SDK doc comment
// says the port state is always open (PARITY.md 4.1).
func (b *InMemoryBackend) GetInstancePortStates(name string) ([]InstancePortInfo, error) {
	b.mu.RLock("GetInstancePortStates")
	defer b.mu.RUnlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	return clonePortInfos(i.Ports), nil
}

// OpenInstancePublicPorts adds one firewall rule to the named instance.
func (b *InMemoryBackend) OpenInstancePublicPorts(name string, rule InstancePortInfo) (*Operation, error) {
	b.mu.Lock("OpenInstancePublicPorts")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	i.Ports = append(i.Ports, rule)
	ops := b.newOperationsLocked(opTypeOpenInstancePublicPorts, ResourceTypeInstance, []string{name})

	return &ops[0], nil
}

// CloseInstancePublicPorts removes the firewall rule matching rule's
// FromPort/ToPort/Protocol from the named instance.
func (b *InMemoryBackend) CloseInstancePublicPorts(name string, rule InstancePortInfo) (*Operation, error) {
	b.mu.Lock("CloseInstancePublicPorts")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	out := make([]InstancePortInfo, 0, len(i.Ports))

	for _, p := range i.Ports {
		if p.FromPort == rule.FromPort && p.ToPort == rule.ToPort && p.Protocol == rule.Protocol {
			continue
		}

		out = append(out, p)
	}

	i.Ports = out
	ops := b.newOperationsLocked(opTypeCloseInstancePublicPorts, ResourceTypeInstance, []string{name})

	return &ops[0], nil
}

// PutInstancePublicPorts replaces the entire firewall rule set on the named
// instance in one call.
func (b *InMemoryBackend) PutInstancePublicPorts(name string, rules []InstancePortInfo) (*Operation, error) {
	b.mu.Lock("PutInstancePublicPorts")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	i.Ports = clonePortInfos(rules)
	ops := b.newOperationsLocked(opTypePutInstancePublicPorts, ResourceTypeInstance, []string{name})

	return &ops[0], nil
}

// SetupInstanceHTTPS records a Bitnami-style HTTPS auto-configuration
// attempt as an audited SetupHistory entry (PARITY.md family C) rather than
// a boolean flag. This backend cannot honestly perform real Let's Encrypt
// issuance against a caller-owned DNS name, so ExecutionDetails records a
// single, clearly-synthetic "succeeded" step -- the audited-history SHAPE is
// real, its content is a documented simplification, not a fabricated
// external validation result.
func (b *InMemoryBackend) SetupInstanceHTTPS(
	instanceName, certificateProvider string, domainNames []string,
) ([]Operation, error) {
	b.mu.Lock("SetupInstanceHttps")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(instanceName)
	if !ok {
		return nil, notFoundError("Instance", instanceName)
	}

	ops := b.newOperationsLocked(opTypeSetupInstanceHTTPS, ResourceTypeInstance, []string{instanceName})
	op := ops[0]

	entry := &SetupHistoryEntry{
		OperationID:         op.ID,
		ResourceName:        instanceName,
		Status:              OperationStatusSucceeded,
		CertificateProvider: certificateProvider,
		InstanceName:        instanceName,
		DomainNames:         cloneStrings(domainNames),
		CreatedAt:           nowUTC(),
		Location:            i.Location,
		ExecutionDetails: []SetupExecutionDetail{
			{
				Name: "configure-https", Command: "bitnami-https-configure", Status: "SUCCEEDED",
				Version: "1", DateTime: nowUTC(),
			},
		},
	}
	b.setupHistory.Put(entry)

	return ops, nil
}

// GetSetupHistory returns SetupHistory entries for resourceName, paginated.
func (b *InMemoryBackend) GetSetupHistory(resourceName, token string) (page.Page[*SetupHistoryEntry], error) {
	b.mu.RLock("GetSetupHistory")
	defer b.mu.RUnlock()

	var all []*SetupHistoryEntry
	if resourceName != "" {
		// Index.Get returns the index's own backing slice (its doc comment:
		// "the caller must not mutate it"); copy before sort.Slice reorders
		// it in place, or concurrent readers of the same resourceName race
		// on it.
		all = append([]*SetupHistoryEntry(nil), b.setupHistoryByResource.Get(resourceName)...)
	} else {
		all = b.setupHistory.All()
	}

	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.Before(all[j].CreatedAt)
		}

		return all[i].OperationID < all[j].OperationID
	})

	out := make([]*SetupHistoryEntry, len(all))
	for i, e := range all {
		out[i] = e.clone()
	}

	return paginateGeneric(out, token)
}

// DeleteKnownHostKeys clears the named instance's tracked SSH host key
// fingerprints (e.g. after a restore/rebuild changed them).
func (b *InMemoryBackend) DeleteKnownHostKeys(name string) ([]Operation, error) {
	b.mu.Lock("DeleteKnownHostKeys")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	i.HostKeys = nil
	i.KnownHostKeysDeleted = true

	return b.newOperationsLocked(opTypeDeleteKnownHostKeys, ResourceTypeInstance, []string{name}), nil
}
