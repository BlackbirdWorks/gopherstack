package awsconfig

import (
	"fmt"
	"slices"
)

// aggregationAuthKey returns a composite key for an aggregation authorization.
func aggregationAuthKey(accountID, region string) string {
	return accountID + "#" + region
}

// PutAggregationAuthorization creates or updates an aggregation authorization.
func (b *InMemoryBackend) PutAggregationAuthorization(accountID, region string) error {
	b.mu.Lock("PutAggregationAuthorization")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:aggregation-authorization/%s/%s",
		b.region, b.accountID, accountID, region,
	)

	b.aggregationAuths.Put(&AggregationAuthorization{
		AuthorizedAccountID:         accountID,
		AuthorizedAwsRegion:         region,
		AggregationAuthorizationArn: arn,
	})

	return nil
}

// DescribeAggregationAuthorizations returns all aggregation authorizations sorted by
// account ID then region.
func (b *InMemoryBackend) DescribeAggregationAuthorizations() []AggregationAuthorization {
	b.mu.RLock("DescribeAggregationAuthorizations")
	defer b.mu.RUnlock()

	all := b.aggregationAuths.All()
	out := make([]AggregationAuthorization, 0, len(all))

	for _, a := range all {
		out = append(out, *a)
	}

	slices.SortFunc(out, func(a, b AggregationAuthorization) int {
		if a.AuthorizedAccountID != b.AuthorizedAccountID {
			if a.AuthorizedAccountID < b.AuthorizedAccountID {
				return -1
			}

			return 1
		}

		if a.AuthorizedAwsRegion < b.AuthorizedAwsRegion {
			return -1
		}

		if a.AuthorizedAwsRegion > b.AuthorizedAwsRegion {
			return 1
		}

		return 0
	})

	return out
}

// DeleteAggregationAuthorization deletes an aggregation authorization by account ID
// and region. Real AWS Config's DeleteAggregationAuthorization is idempotent -- its
// error model (verified against aws-sdk-go-v2/service/configservice's deserializer)
// only lists InvalidParameterValueException, never a not-found exception -- so
// deleting a nonexistent authorization succeeds silently, matching AWS.
func (b *InMemoryBackend) DeleteAggregationAuthorization(accountID, region string) error {
	if accountID == "" {
		return fmt.Errorf("%w: AuthorizedAccountId is required", ErrValidation)
	}

	if region == "" {
		return fmt.Errorf("%w: AuthorizedAwsRegion is required", ErrValidation)
	}

	b.mu.Lock("DeleteAggregationAuthorization")
	defer b.mu.Unlock()

	b.aggregationAuths.Delete(aggregationAuthKey(accountID, region))

	return nil
}

// PutConfigurationAggregator creates or updates a configuration aggregator.
func (b *InMemoryBackend) PutConfigurationAggregator(
	name string,
	accountSources []AccountAggregationSource,
	orgSource *OrganizationAggregationSource,
) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationAggregatorName is required", ErrValidation)
	}

	b.mu.Lock("PutConfigurationAggregator")
	defer b.mu.Unlock()

	b.aggregatorCounter++
	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:config-aggregator/config-aggregator-%08d",
		b.region, b.accountID, b.aggregatorCounter,
	)

	b.aggregators.Put(&ConfigurationAggregator{
		ConfigurationAggregatorName:   name,
		ConfigurationAggregatorArn:    arn,
		AccountAggregationSources:     accountSources,
		OrganizationAggregationSource: orgSource,
	})

	return nil
}

// DeleteConfigurationAggregator deletes a configuration aggregator by name.
func (b *InMemoryBackend) DeleteConfigurationAggregator(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationAggregatorName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationAggregator")
	defer b.mu.Unlock()

	if !b.aggregators.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchAggregator, name)
	}

	b.aggregators.Delete(name)

	return nil
}

// DescribeConfigurationAggregators returns all aggregators sorted by name.
func (b *InMemoryBackend) DescribeConfigurationAggregators() []ConfigurationAggregator {
	b.mu.RLock("DescribeConfigurationAggregators")
	defer b.mu.RUnlock()

	all := b.aggregators.All()
	out := make([]ConfigurationAggregator, 0, len(all))

	for _, a := range all {
		out = append(out, *a)
	}

	return out
}

// DescribeConfigurationAggregatorSourcesStatus returns an empty list.
func (b *InMemoryBackend) DescribeConfigurationAggregatorSourcesStatus() []any {
	return []any{}
}

// DeletePendingAggregationRequest is a no-op stub.
func (b *InMemoryBackend) DeletePendingAggregationRequest(_, _ string) error { return nil }

// DescribePendingAggregationRequests returns an empty list.
func (b *InMemoryBackend) DescribePendingAggregationRequests() []any {
	return []any{}
}
