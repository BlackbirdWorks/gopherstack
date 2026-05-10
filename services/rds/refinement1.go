package rds

import (
	"fmt"
	"slices"
	"time"
)

// CreateEventSubscription creates a new event notification subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	name, snsTopicARN, sourceType string,
	sourceIDs, eventCategories []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if _, exists := b.eventSubscriptions[name]; exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrEventSubscriptionAlreadyExists, name)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	cats := make([]string, len(eventCategories))
	copy(cats, eventCategories)
	sub := &EventSubscription{
		SubscriptionName: name,
		SnsTopicArn:      snsTopicARN,
		Status:           subscriptionStatusActive,
		SourceType:       sourceType,
		SourceIDs:        ids,
		EventCategories:  cats,
		Enabled:          true,
	}
	b.eventSubscriptions[name] = sub
	cp := *sub

	return &cp, nil
}

// DeleteEventSubscription deletes the named event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	cp := *sub
	delete(b.eventSubscriptions, name)

	return &cp, nil
}

// DescribeEventSubscriptions returns all subscriptions or the named one.
func (b *InMemoryBackend) DescribeEventSubscriptions(name string) ([]EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	if name != "" {
		sub, exists := b.eventSubscriptions[name]
		if !exists {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
		}
		cp := *sub

		return []EventSubscription{cp}, nil
	}
	result := make([]EventSubscription, 0, len(b.eventSubscriptions))
	for _, sub := range b.eventSubscriptions {
		result = append(result, *sub)
	}

	return result, nil
}

// ModifyEventSubscription modifies an existing event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	name, snsTopicARN, sourceType string,
	sourceIDs, eventCategories []string,
	enabled *bool,
) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicArn = snsTopicARN
	}
	if sourceType != "" {
		sub.SourceType = sourceType
	}
	if len(sourceIDs) > 0 {
		ids := make([]string, len(sourceIDs))
		copy(ids, sourceIDs)
		sub.SourceIDs = ids
	}
	if len(eventCategories) > 0 {
		cats := make([]string, len(eventCategories))
		copy(cats, eventCategories)
		sub.EventCategories = cats
	}
	if enabled != nil {
		sub.Enabled = *enabled
	}
	cp := *sub

	return &cp, nil
}

// DescribeEvents returns published events filtered by sourceID, sourceType, and/or duration minutes.
// Results are sorted by creation time ascending, matching AWS behaviour.
func (b *InMemoryBackend) DescribeEvents(sourceID, sourceType string, durationMinutes int) ([]Event, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()
	var cutoff time.Time
	if durationMinutes > 0 {
		cutoff = time.Now().Add(-time.Duration(durationMinutes) * time.Minute)
	}
	result := make([]Event, 0, len(b.events))
	for _, ev := range b.events {
		if sourceID != "" && ev.SourceIdentifier != sourceID {
			continue
		}
		if sourceType != "" && ev.SourceType != sourceType {
			continue
		}
		if durationMinutes > 0 && ev.CreatedAt.Before(cutoff) {
			continue
		}
		result = append(result, ev)
	}
	slices.SortFunc(result, func(a, b Event) int {
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		if a.CreatedAt.After(b.CreatedAt) {
			return 1
		}

		return 0
	})

	return result, nil
}

// DescribeEventCategories returns event category names, optionally filtered by source type.
func (b *InMemoryBackend) DescribeEventCategories(_ string) ([]string, error) {
	return []string{
		"availability",
		"backup",
		"configuration change",
		"creation",
		"deletion",
		"failover",
		"failure",
		"maintenance",
		"notification",
		"recovery",
		"restoration",
	}, nil
}

// DescribeBlueGreenDeployments returns blue/green deployments, optionally by ID.
func (b *InMemoryBackend) DescribeBlueGreenDeployments(id string) ([]BlueGreenDeployment, error) {
	b.mu.RLock("DescribeBlueGreenDeployments")
	defer b.mu.RUnlock()
	if id != "" {
		d, exists := b.blueGreenDeployments[id]
		if !exists {
			return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
		}
		cp := *d

		return []BlueGreenDeployment{cp}, nil
	}
	result := make([]BlueGreenDeployment, 0, len(b.blueGreenDeployments))
	for _, d := range b.blueGreenDeployments {
		result = append(result, *d)
	}

	return result, nil
}

// DeleteBlueGreenDeployment deletes the named blue/green deployment.
func (b *InMemoryBackend) DeleteBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("DeleteBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments[id]
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	cp := *d
	delete(b.blueGreenDeployments, id)

	return &cp, nil
}

// SwitchoverBlueGreenDeployment switches over a blue/green deployment.
func (b *InMemoryBackend) SwitchoverBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("SwitchoverBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments[id]
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	d.Status = "switchover-completed"
	cp := *d

	return &cp, nil
}

// DescribeDBSecurityGroups returns security groups, optionally by name.
func (b *InMemoryBackend) DescribeDBSecurityGroups(name string) ([]DBSecurityGroup, error) {
	b.mu.RLock("DescribeDBSecurityGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.dbSecurityGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: security group %s not found", ErrDBSecurityGroupNotFound, name)
		}
		cp := *sg

		return []DBSecurityGroup{cp}, nil
	}
	result := make([]DBSecurityGroup, 0, len(b.dbSecurityGroups))
	for _, sg := range b.dbSecurityGroups {
		result = append(result, *sg)
	}

	return result, nil
}

// DeleteDBSecurityGroup removes the named security group.
func (b *InMemoryBackend) DeleteDBSecurityGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DBSecurityGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBSecurityGroup")
	defer b.mu.Unlock()
	if _, exists := b.dbSecurityGroups[name]; !exists {
		return fmt.Errorf("%w: security group %s not found", ErrDBSecurityGroupNotFound, name)
	}
	delete(b.dbSecurityGroups, name)

	return nil
}

// RevokeDBSecurityGroupIngress revokes a CIDR IP from a security group.
func (b *InMemoryBackend) RevokeDBSecurityGroupIngress(groupName, cidrIP string) (*DBSecurityGroup, error) {
	if groupName == "" || cidrIP == "" {
		return nil, fmt.Errorf("%w: DBSecurityGroupName and CIDRIP are required", ErrInvalidParameter)
	}
	b.mu.Lock("RevokeDBSecurityGroupIngress")
	defer b.mu.Unlock()
	sg, exists := b.dbSecurityGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: security group %s not found", ErrDBSecurityGroupNotFound, groupName)
	}
	idx := -1
	for i, r := range sg.IPRanges {
		if r.CIDRIP == cidrIP {
			idx = i

			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("%w: CIDR %s not found in security group %s", ErrInvalidParameter, cidrIP, groupName)
	}
	sg.IPRanges = slices.Delete(sg.IPRanges, idx, idx+1)
	cp := *sg

	return &cp, nil
}

// FailoverDBCluster triggers a failover on an Aurora DB cluster.
func (b *InMemoryBackend) FailoverDBCluster(clusterID, _ string) (*DBCluster, error) {
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if cluster.Status != instanceStatusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state", ErrInvalidDBClusterStateFault, clusterID)
	}
	cluster.Status = "failing-over"
	b.publishClusterEventLocked(clusterID, "DB cluster failover started")
	cluster.Status = instanceStatusAvailable
	cp := *cluster

	return &cp, nil
}

// RebootDBCluster reboots the named Aurora DB cluster.
// The cluster transitions to "rebooting" status and reverts to "available" after a brief delay.
func (b *InMemoryBackend) RebootDBCluster(clusterID string) (*DBCluster, error) {
	b.mu.Lock("RebootDBCluster")
	cluster, exists := b.clusters[clusterID]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if cluster.Status != instanceStatusAvailable {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: cluster %s is not in available state", ErrInvalidDBClusterStateFault, clusterID)
	}
	cluster.Status = "rebooting"
	cp := *cluster
	b.mu.Unlock()

	go func() {
		time.Sleep(instanceTransitionDelay)
		b.mu.Lock("RebootDBCluster-complete")
		if c, ok := b.clusters[clusterID]; ok && c.Status == "rebooting" {
			c.Status = instanceStatusAvailable
		}
		b.mu.Unlock()
	}()

	return &cp, nil
}

// DeleteDBClusterParameterGroup deletes a cluster parameter group.
func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(b.clusterParameterGroups, name)
	arn := "arn:aws:rds:" + b.region + ":" + b.accountID + ":cluster-pg:" + name
	delete(b.tags, arn)

	return nil
}

// ModifyDBClusterParameterGroup modifies parameters in a cluster parameter group.
func (b *InMemoryBackend) ModifyDBClusterParameterGroup(name string, params []DBParameter) (string, error) {
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return "", fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if pg.Parameters == nil {
		pg.Parameters = make(map[string]DBParameter)
	}
	for _, p := range params {
		pg.Parameters[p.ParameterName] = p
	}

	return name, nil
}

// DescribeDBClusterParameters returns parameters for a cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	pg, exists := b.clusterParameterGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	result := make([]DBParameter, 0, len(pg.Parameters))
	for _, p := range pg.Parameters {
		result = append(result, p)
	}
	slices.SortFunc(result, func(a, b DBParameter) int {
		if a.ParameterName < b.ParameterName {
			return -1
		}
		if a.ParameterName > b.ParameterName {
			return 1
		}

		return 0
	})

	return result, nil
}

// ResetDBClusterParameterGroup resets parameters in a cluster parameter group.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	groupName string,
	resetAllParameters bool,
	params []string,
) (string, error) {
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[groupName]
	if !exists {
		return "", fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	if resetAllParameters {
		pg.Parameters = make(map[string]DBParameter)
	} else {
		for _, name := range params {
			delete(pg.Parameters, name)
		}
	}

	return groupName, nil
}

// ModifyDBSubnetGroup modifies an existing DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(name, description string, subnetIDs []string) (*DBSubnetGroup, error) {
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	if description != "" {
		sg.DBSubnetGroupDescription = description
	}
	if len(subnetIDs) > 0 {
		ids := make([]string, len(subnetIDs))
		copy(ids, subnetIDs)
		sg.SubnetIDs = ids
	}
	cp := *sg

	return &cp, nil
}

// ModifyDBClusterEndpoint modifies a custom DB cluster endpoint.
func (b *InMemoryBackend) ModifyDBClusterEndpoint(endpointID, endpointType string) (*DBClusterEndpoint, error) {
	b.mu.Lock("ModifyDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpoints[endpointID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	if endpointType != "" {
		ep.EndpointType = endpointType
	}
	cp := *ep

	return &cp, nil
}

// publishClusterEventLocked publishes an event for a DB cluster.
// Must be called with the write lock held.
func (b *InMemoryBackend) publishClusterEventLocked(clusterID, msg string) {
	event := Event{
		Message:          msg,
		SourceIdentifier: clusterID,
		SourceType:       "db-cluster",
		CreatedAt:        time.Now(),
	}
	b.events = append(b.events, event)
	if len(b.events) > maxEvents {
		b.events = b.events[len(b.events)-maxEvents:]
	}
}
