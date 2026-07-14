package ec2

import "fmt"

// DescribeAccountAttributes returns fixed account attributes.
func (b *InMemoryBackend) DescribeAccountAttributes(names []string) []AccountAttribute {
	b.mu.RLock("DescribeAccountAttributes")
	defer b.mu.RUnlock()

	all := []AccountAttribute{
		{Name: "supported-platforms", Values: []string{"VPC"}},
		{Name: "default-vpc", Values: []string{"vpc-default"}},
		{Name: "max-instances", Values: []string{"20"}},
		{Name: "vpc-max-security-groups-per-interface", Values: []string{"5"}},
		{Name: "max-elastic-ips", Values: []string{"5"}},
		{Name: "vpc-max-elastic-ips", Values: []string{"5"}},
	}

	if len(names) == 0 {
		return all
	}

	filter := make(map[string]bool, len(names))
	for _, n := range names {
		filter[n] = true
	}

	var out []AccountAttribute
	for _, attr := range all {
		if filter[attr.Name] {
			out = append(out, attr)
		}
	}

	return out
}

// ---- DescribePrefixLists ----

// PrefixList represents an AWS-managed prefix list.
type PrefixList struct {
	PrefixListID   string   `json:"prefixListID,omitempty"`
	PrefixListName string   `json:"prefixListName,omitempty"`
	CIDRs          []string `json:"cidRs,omitempty"`
}

// DescribePrefixLists returns AWS-managed prefix lists (static).
func (b *InMemoryBackend) DescribePrefixLists(ids []string) []PrefixList {
	b.mu.RLock("DescribePrefixLists")
	defer b.mu.RUnlock()

	all := []PrefixList{
		{
			PrefixListID:   "pl-63a5400a",
			PrefixListName: "com.amazonaws." + b.Region + ".s3",
			CIDRs:          []string{"52.216.0.0/15", "54.231.0.0/17"},
		},
		{
			PrefixListID:   "pl-02cd2c6b",
			PrefixListName: "com.amazonaws." + b.Region + ".dynamodb",
			CIDRs:          []string{"52.119.224.0/20"},
		},
	}

	if len(ids) == 0 {
		return all
	}

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []PrefixList
	for _, pl := range all {
		if filter[pl.PrefixListID] {
			out = append(out, pl)
		}
	}

	return out
}

// ---- ID format ----

// IDFormatItem represents an ID format setting for a resource type.
type IDFormatItem struct {
	Resource   string `json:"resource,omitempty"`
	Deadline   string `json:"deadline,omitempty"`
	UseLongIDs bool   `json:"useLongIDs,omitempty"`
}

// DescribeIDFormat returns ID format settings for the account.
func (b *InMemoryBackend) DescribeIDFormat(resources []string) []IDFormatItem {
	b.mu.RLock("DescribeIDFormat")
	defer b.mu.RUnlock()

	defaults := []string{"instance", "reservation", "snapshot", "volume"}
	filter := make(map[string]bool, len(resources))
	for _, r := range resources {
		filter[r] = true
	}

	var out []IDFormatItem
	for _, r := range defaults {
		if len(filter) > 0 && !filter[r] {
			continue
		}
		useLong := b.idFormatSettings[r]
		out = append(out, IDFormatItem{Resource: r, UseLongIDs: useLong})
	}

	return out
}

// ModifyIDFormat updates the long-ID setting for a resource type.
func (b *InMemoryBackend) ModifyIDFormat(resource string, useLongIDs bool) error {
	if resource == "" {
		return fmt.Errorf("%w: Resource is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIDFormat")
	defer b.mu.Unlock()

	b.idFormatSettings[resource] = useLongIDs

	return nil
}

// DescribeIdentityIDFormat returns ID format settings for a given principal (same as account-level here).
func (b *InMemoryBackend) DescribeIdentityIDFormat(
	_ string,
	resources []string,
) []IDFormatItem {
	return b.DescribeIDFormat(resources)
}

// ModifyIdentityIDFormat updates the long-ID setting for a resource type (identity-scoped; same store as account).
func (b *InMemoryBackend) ModifyIdentityIDFormat(
	_ string, resource string,
	useLongIDs bool,
) error {
	return b.ModifyIDFormat(resource, useLongIDs)
}

// DescribeAggregateIDFormat returns ID format settings for all resource types.
func (b *InMemoryBackend) DescribeAggregateIDFormat() []IDFormatItem {
	return b.DescribeIDFormat(nil)
}

// DescribePrincipalIDFormat returns ID format for a principal (same as aggregate here).
func (b *InMemoryBackend) DescribePrincipalIDFormat(_ string) []IDFormatItem {
	return b.DescribeIDFormat(nil)
}

// ---- Instance event notification attributes ----

// InstanceEventNotificationAttributes holds account-level event notification settings.
type InstanceEventNotificationAttributes struct {
	IncludeAllTagsOfInstance bool `json:"includeAllTagsOfInstance,omitempty"`
}

// DescribeInstanceEventNotificationAttributes returns account-level settings.
func (b *InMemoryBackend) DescribeInstanceEventNotificationAttributes() *InstanceEventNotificationAttributes {
	b.mu.RLock("DescribeInstanceEventNotificationAttributes")
	defer b.mu.RUnlock()

	if b.instanceEventNotifAttrs == nil {
		return &InstanceEventNotificationAttributes{}
	}
	cp := *b.instanceEventNotifAttrs

	return &cp
}

// DeregisterInstanceEventNotificationAttributes clears event notification attributes.
func (b *InMemoryBackend) DeregisterInstanceEventNotificationAttributes() {
	b.mu.Lock("DeregisterInstanceEventNotificationAttributes")
	defer b.mu.Unlock()

	b.instanceEventNotifAttrs = nil
}

// RegisterInstanceEventNotificationAttributes enables all-tag notification for instance events.
func (b *InMemoryBackend) RegisterInstanceEventNotificationAttributes(includeAllTags bool) {
	b.mu.Lock("RegisterInstanceEventNotificationAttributes")
	defer b.mu.Unlock()

	b.instanceEventNotifAttrs = &InstanceEventNotificationAttributes{
		IncludeAllTagsOfInstance: includeAllTags,
	}
}

// ---- ResetEbsDefaultKmsKeyID ----
