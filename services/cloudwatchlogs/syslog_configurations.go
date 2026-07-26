package cloudwatchlogs

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// PutSyslogConfiguration creates or replaces the syslog configuration for a
// log group (see the SyslogConfiguration doc comment in models.go for the
// per-log-group-identifier keying rationale). Unlike the pre-existing
// IndexPolicy/Transformer completeness-pass ops, which accept any
// logGroupIdentifier string without checking it resolves to a real log
// group, this validates the log group actually exists in the caller's
// region and returns ResourceNotFoundException otherwise -- syslog
// ingestion cannot meaningfully attach to a log group that doesn't exist.
func (b *InMemoryBackend) PutSyslogConfiguration(
	ctx context.Context,
	logGroupIdentifier, vpcEndpointID string,
) (*SyslogConfiguration, error) {
	if logGroupIdentifier == "" {
		return nil, fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	name := normalizeLogGroupIdentifier(logGroupIdentifier)
	region := getRegion(ctx, b.region)

	b.mu.Lock("PutSyslogConfiguration")
	defer b.mu.Unlock()

	group, ok := b.groupGet(region, name)
	if !ok {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, name)
	}

	createdAt := time.Now().UnixMilli()
	if existing, had := b.syslogConfigurations.Get(name); had {
		createdAt = existing.CreatedAt
	}

	cfg := &SyslogConfiguration{
		LogGroupIdentifier: name,
		LogGroupArn:        group.Arn,
		SourceType:         syslogSourceTypeVPCE,
		VpcEndpointID:      vpcEndpointID,
		CreatedAt:          createdAt,
	}
	b.syslogConfigurations.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// DeleteSyslogConfiguration removes the syslog configuration for a log
// group. When vpcEndpointID is non-empty it must match the stored
// configuration's VPC endpoint, or the delete is treated as not-found
// (mirrors DeleteSyslogConfigurationInput accepting an optional
// VpcEndpointId scoping parameter).
func (b *InMemoryBackend) DeleteSyslogConfiguration(logGroupIdentifier, vpcEndpointID string) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	name := normalizeLogGroupIdentifier(logGroupIdentifier)

	b.mu.Lock("DeleteSyslogConfiguration")
	defer b.mu.Unlock()

	existing, ok := b.syslogConfigurations.Get(name)
	if !ok || (vpcEndpointID != "" && existing.VpcEndpointID != vpcEndpointID) {
		return fmt.Errorf("%w: syslog configuration for %s not found", ErrSyslogConfigurationNotFound, name)
	}

	b.syslogConfigurations.Delete(name)

	return nil
}

// ListSyslogConfigurations returns syslog configurations optionally filtered
// by log group identifier and/or VPC endpoint ID, with pagination.
func (b *InMemoryBackend) ListSyslogConfigurations(
	logGroupIdentifier, vpcEndpointID, nextToken string, limit int,
) ([]SyslogConfiguration, string) {
	b.mu.RLock("ListSyslogConfigurations")
	defer b.mu.RUnlock()

	nameFilter := ""
	if logGroupIdentifier != "" {
		nameFilter = normalizeLogGroupIdentifier(logGroupIdentifier)
	}

	all := make([]SyslogConfiguration, 0, b.syslogConfigurations.Len())

	for _, c := range b.syslogConfigurations.All() {
		if nameFilter != "" && c.LogGroupIdentifier != nameFilter {
			continue
		}

		if vpcEndpointID != "" && c.VpcEndpointID != vpcEndpointID {
			continue
		}

		all = append(all, *c)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LogGroupIdentifier < all[j].LogGroupIdentifier })

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []SyslogConfiguration{}, ""
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

// deleteSyslogConfigurationForGroup removes any syslog configuration for
// name, ignoring not-found (used by DeleteLogGroup's cascade). Caller must
// already hold b.mu.
func (b *InMemoryBackend) deleteSyslogConfigurationForGroup(name string) {
	b.syslogConfigurations.Delete(name)
}
