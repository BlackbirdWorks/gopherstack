package quicksight

import (
	"maps"
	"sort"
	"time"
)

const (
	vpcConnectionAvailabilityAvailable = "AVAILABLE"
)

// storedVPCConnection is the persisted representation of a QuickSight VPC connection.
type storedVPCConnection struct {
	CreatedTime        time.Time `json:"createdTime"`
	LastUpdatedTime    time.Time `json:"lastUpdatedTime"`
	VPCConnectionID    string    `json:"vpcConnectionId"`
	Arn                string    `json:"arn"`
	Name               string    `json:"name"`
	VPCID              string    `json:"vpcId"`
	RoleArn            string    `json:"roleArn"`
	Status             string    `json:"status"`
	AvailabilityStatus string    `json:"availabilityStatus"`
	SubnetIDs          []string  `json:"subnetIds"`
	SecurityGroupIDs   []string  `json:"securityGroupIds"`
	DNSResolvers       []string  `json:"dnsResolvers"`
}

func (v *storedVPCConnection) toVPCConnection() *VPCConnection {
	return &VPCConnection{
		CreatedTime:        v.CreatedTime,
		LastUpdatedTime:    v.LastUpdatedTime,
		VPCConnectionID:    v.VPCConnectionID,
		Arn:                v.Arn,
		Name:               v.Name,
		VPCID:              v.VPCID,
		SubnetIDs:          append([]string(nil), v.SubnetIDs...),
		SecurityGroupIDs:   append([]string(nil), v.SecurityGroupIDs...),
		DNSResolvers:       append([]string(nil), v.DNSResolvers...),
		RoleArn:            v.RoleArn,
		Status:             v.Status,
		AvailabilityStatus: v.AvailabilityStatus,
	}
}

func vpcConnectionKey(accountID, vpcConnectionID string) string {
	return accountID + "/" + vpcConnectionID
}

// ---- VPC Connections ----

func (b *InMemoryBackend) CreateVPCConnection(
	accountID, vpcConnectionID, name, vpcID string,
	subnetIDs, securityGroupIDs, dnsResolvers []string,
	roleArn string,
	tags map[string]string,
) (*VPCConnection, error) {
	if vpcConnectionID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateVPCConnection")
	defer b.mu.Unlock()

	key := vpcConnectionKey(accountID, vpcConnectionID)
	if b.vpcConnections.Has(key) {
		return nil, ErrVPCConnectionAlreadyExists
	}

	now := time.Now().UTC()
	v := &storedVPCConnection{
		CreatedTime:        now,
		LastUpdatedTime:    now,
		VPCConnectionID:    vpcConnectionID,
		Arn:                b.buildARN("vpc-connection", vpcConnectionID),
		Name:               name,
		VPCID:              vpcID,
		SubnetIDs:          subnetIDs,
		SecurityGroupIDs:   securityGroupIDs,
		DNSResolvers:       dnsResolvers,
		RoleArn:            roleArn,
		Status:             statusCreationSuccessful,
		AvailabilityStatus: vpcConnectionAvailabilityAvailable,
	}
	b.vpcConnections.Put(v)

	if len(tags) > 0 {
		b.tags[v.Arn] = maps.Clone(tags)
	}

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) DescribeVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error) {
	b.mu.RLock("DescribeVPCConnection")
	defer b.mu.RUnlock()

	v, ok := b.vpcConnections.Get(vpcConnectionKey(accountID, vpcConnectionID))
	if !ok {
		return nil, ErrVPCConnectionNotFound
	}

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) UpdateVPCConnection(
	accountID, vpcConnectionID, name string,
	subnetIDs, securityGroupIDs, dnsResolvers []string,
	roleArn string,
) (*VPCConnection, error) {
	b.mu.Lock("UpdateVPCConnection")
	defer b.mu.Unlock()

	key := vpcConnectionKey(accountID, vpcConnectionID)
	v, ok := b.vpcConnections.Get(key)
	if !ok {
		return nil, ErrVPCConnectionNotFound
	}

	if name != "" {
		v.Name = name
	}
	if subnetIDs != nil {
		v.SubnetIDs = subnetIDs
	}
	if securityGroupIDs != nil {
		v.SecurityGroupIDs = securityGroupIDs
	}
	if dnsResolvers != nil {
		v.DNSResolvers = dnsResolvers
	}
	if roleArn != "" {
		v.RoleArn = roleArn
	}
	v.LastUpdatedTime = time.Now().UTC()
	v.Status = statusUpdateSuccessful

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) DeleteVPCConnection(accountID, vpcConnectionID string) error {
	b.mu.Lock("DeleteVPCConnection")
	defer b.mu.Unlock()

	key := vpcConnectionKey(accountID, vpcConnectionID)
	v, ok := b.vpcConnections.Get(key)
	if !ok {
		return ErrVPCConnectionNotFound
	}

	delete(b.tags, v.Arn)
	b.vpcConnections.Delete(key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListVPCConnections(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*VPCConnection, string, error) {
	b.mu.RLock("ListVPCConnections")
	defer b.mu.RUnlock()

	all := b.vpcConnections.All()
	sort.Slice(all, func(i, j int) bool { return all[i].VPCConnectionID < all[j].VPCConnectionID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = encodePageToken(end)
	} else {
		end = len(all)
	}

	result := make([]*VPCConnection, 0, end-start)
	for _, v := range all[start:end] {
		result = append(result, v.toVPCConnection())
	}

	return result, next, nil
}
