package emr

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) securityConfigGet(region, name string) (*SecurityConfiguration, bool) {
	return b.securityConfigs.Get(regionKey(region, name))
}

func (b *InMemoryBackend) securityConfigPut(v *SecurityConfiguration) { b.securityConfigs.Put(v) }

func (b *InMemoryBackend) securityConfigDelete(region, name string) {
	b.securityConfigs.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) securityConfigsInRegion(region string) []*SecurityConfiguration {
	return b.securityConfigsByRegion.Get(region)
}

// ListSecurityConfigurations returns all security configurations, sorted by name.
func (b *InMemoryBackend) ListSecurityConfigurations(
	ctx context.Context,
	marker string,
) ([]SecurityConfigSummary, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSecurityConfigurations")
	defer b.mu.RUnlock()

	configs := b.securityConfigsInRegion(region)
	summaries := make([]SecurityConfigSummary, 0, len(configs))

	for _, sc := range configs {
		summaries = append(summaries, SecurityConfigSummary{
			Name:             sc.Name,
			CreationDateTime: sc.CreationDateTime,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	p := page.New(summaries, marker, listSecConfigsPageSize, listSecConfigsPageSize)

	return p.Data, p.Next
}

// CreateSecurityConfiguration creates a new security configuration.
func (b *InMemoryBackend) CreateSecurityConfiguration(
	ctx context.Context,
	name, securityConfig string,
) (*SecurityConfiguration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if !json.Valid([]byte(securityConfig)) {
		return nil, fmt.Errorf("%w: SecurityConfiguration must be valid JSON", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateSecurityConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.securityConfigGet(region, name); exists {
		return nil, fmt.Errorf(
			"%w: security configuration %s already exists",
			ErrAlreadyExists,
			name,
		)
	}

	sc := &SecurityConfiguration{
		Name:             name,
		SecurityConfig:   securityConfig,
		CreationDateTime: awstime.Epoch(time.Now()),
		region:           region,
	}

	b.securityConfigPut(sc)

	cp := *sc

	return &cp, nil
}

// DeleteSecurityConfiguration deletes a security configuration by name.
func (b *InMemoryBackend) DeleteSecurityConfiguration(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSecurityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.securityConfigGet(region, name); !ok {
		return fmt.Errorf("%w: security configuration %s not found", ErrNotFound, name)
	}

	b.securityConfigDelete(region, name)

	return nil
}

// DescribeSecurityConfiguration returns the details of a security configuration.
func (b *InMemoryBackend) DescribeSecurityConfiguration(
	ctx context.Context,
	name string,
) (*SecurityConfiguration, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSecurityConfiguration")
	defer b.mu.RUnlock()

	sc, ok := b.securityConfigGet(region, name)
	if !ok {
		return nil, fmt.Errorf("%w: security configuration %s not found", ErrNotFound, name)
	}

	cp := *sc

	return &cp, nil
}

// AddSecurityConfigInternal seeds a security configuration directly into the backend for testing.
func (b *InMemoryBackend) AddSecurityConfigInternal(ctx context.Context, sc SecurityConfiguration) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddSecurityConfigInternal")
	defer b.mu.Unlock()

	cp := sc
	cp.region = region
	b.securityConfigPut(&cp)
}
