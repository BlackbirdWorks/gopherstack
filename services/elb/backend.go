// Package elb provides an in-memory implementation of the AWS Classic Elastic
// Load Balancing (ELB) service.
package elb

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrLoadBalancerNotFound is returned when the requested load balancer does not exist.
	ErrLoadBalancerNotFound = awserr.New("LoadBalancerNotFound", awserr.ErrNotFound)
	// ErrLoadBalancerAlreadyExists is returned when a load balancer with that name already exists.
	ErrLoadBalancerAlreadyExists = awserr.New("DuplicateLoadBalancerName", awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned when a request parameter is invalid or missing.
	ErrInvalidParameter = awserr.New("ValidationError", awserr.ErrInvalidParameter)
	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
)

// Listener is a single protocol/port mapping on a load balancer.
type Listener struct {
	Protocol         string
	InstanceProtocol string
	LoadBalancerPort int32
	InstancePort     int32
}

// HealthCheck holds health-check configuration for a load balancer.
type HealthCheck struct {
	Target             string
	Interval           int32
	Timeout            int32
	UnhealthyThreshold int32
	HealthyThreshold   int32
}

// Instance is an EC2 instance registered with a load balancer.
type Instance struct {
	InstanceID string
}

// LoadBalancer represents a Classic ELB load balancer.
type LoadBalancer struct {
	CreatedTime               time.Time
	HealthCheck               *HealthCheck
	Tags                      *tags.Tags
	LoadBalancerName          string
	DNSName                   string
	CanonicalHostedZoneName   string
	CanonicalHostedZoneNameID string
	Scheme                    string
	VPCId                     string
	AccountID                 string
	Region                    string
	Listeners                 []Listener
	Instances                 []Instance
	AvailabilityZones         []string
	SecurityGroups            []string
	Subnets                   []string
}

// CreateLoadBalancerInput holds input for CreateLoadBalancer.
type CreateLoadBalancerInput struct {
	LoadBalancerName  string
	Scheme            string
	AvailabilityZones []string
	SecurityGroups    []string
	Subnets           []string
	Listeners         []Listener
}

// StorageBackend is the interface for the ELB in-memory store.
type StorageBackend interface {
	CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error)
	DeleteLoadBalancer(name string) error
	DescribeLoadBalancers(names []string) ([]LoadBalancer, error)

	RegisterInstancesWithLoadBalancer(name string, instances []Instance) ([]Instance, error)
	DeregisterInstancesFromLoadBalancer(name string, instances []Instance) ([]Instance, error)

	ConfigureHealthCheck(name string, hc HealthCheck) (*HealthCheck, error)

	AddTags(names []string, kvs []tags.KV) error
	DescribeTags(names []string) (map[string][]tags.KV, error)
	RemoveTags(names []string, keys []string) error
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	lbs       map[string]*LoadBalancer
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		lbs:       make(map[string]*LoadBalancer),
		mu:        lockmetrics.New("elb"),
		accountID: accountID,
		region:    region,
	}
}

// CreateLoadBalancer creates a new Classic ELB load balancer.
func (b *InMemoryBackend) CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error) {
	b.mu.Lock("CreateLoadBalancer")
	defer b.mu.Unlock()

	if input.LoadBalancerName == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	if _, exists := b.lbs[input.LoadBalancerName]; exists {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerAlreadyExists, input.LoadBalancerName)
	}

	scheme := input.Scheme
	if scheme == "" {
		scheme = "internet-facing"
	}

	dnsName := input.LoadBalancerName + "." + b.region + ".elb.amazonaws.com"
	lbARN := arn.Build("elasticloadbalancing", b.region, b.accountID, "loadbalancer/"+input.LoadBalancerName)

	lb := &LoadBalancer{
		LoadBalancerName:          input.LoadBalancerName,
		DNSName:                   dnsName,
		CanonicalHostedZoneName:   dnsName,
		CanonicalHostedZoneNameID: lbARN,
		CreatedTime:               time.Now(),
		Scheme:                    scheme,
		AvailabilityZones:         input.AvailabilityZones,
		SecurityGroups:            input.SecurityGroups,
		Subnets:                   input.Subnets,
		Listeners:                 input.Listeners,
		Instances:                 []Instance{},
		Tags:                      tags.New("elb." + input.LoadBalancerName),
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	b.lbs[input.LoadBalancerName] = lb

	cp := *lb

	return &cp, nil
}

// DeleteLoadBalancer removes a load balancer by name.
func (b *InMemoryBackend) DeleteLoadBalancer(name string) error {
	b.mu.Lock("DeleteLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Tags.Close()
	delete(b.lbs, name)

	return nil
}

// DescribeLoadBalancers returns load balancers, optionally filtered by name.
func (b *InMemoryBackend) DescribeLoadBalancers(names []string) ([]LoadBalancer, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		result := make([]LoadBalancer, 0, len(names))

		for _, name := range names {
			lb, ok := b.lbs[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
			}

			cp := *lb
			result = append(result, cp)
		}

		return result, nil
	}

	result := make([]LoadBalancer, 0, len(b.lbs))
	for _, lb := range b.lbs {
		cp := *lb
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LoadBalancerName < result[j].LoadBalancerName
	})

	return result, nil
}

// RegisterInstancesWithLoadBalancer registers EC2 instances with a load balancer.
func (b *InMemoryBackend) RegisterInstancesWithLoadBalancer(name string, instances []Instance) ([]Instance, error) {
	b.mu.Lock("RegisterInstancesWithLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	existing := make(map[string]bool, len(lb.Instances))
	for _, inst := range lb.Instances {
		existing[inst.InstanceID] = true
	}

	for _, inst := range instances {
		if !existing[inst.InstanceID] {
			lb.Instances = append(lb.Instances, inst)
			existing[inst.InstanceID] = true
		}
	}

	result := make([]Instance, len(lb.Instances))
	copy(result, lb.Instances)

	return result, nil
}

// DeregisterInstancesFromLoadBalancer removes EC2 instances from a load balancer.
func (b *InMemoryBackend) DeregisterInstancesFromLoadBalancer(name string, instances []Instance) ([]Instance, error) {
	b.mu.Lock("DeregisterInstancesFromLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[string]bool, len(instances))
	for _, inst := range instances {
		remove[inst.InstanceID] = true
	}

	kept := lb.Instances[:0]
	for _, inst := range lb.Instances {
		if !remove[inst.InstanceID] {
			kept = append(kept, inst)
		}
	}

	lb.Instances = kept

	result := make([]Instance, len(lb.Instances))
	copy(result, lb.Instances)

	return result, nil
}

// ConfigureHealthCheck sets the health-check configuration on a load balancer.
func (b *InMemoryBackend) ConfigureHealthCheck(name string, hc HealthCheck) (*HealthCheck, error) {
	b.mu.Lock("ConfigureHealthCheck")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.HealthCheck = &hc
	cp := hc

	return &cp, nil
}

// AddTags adds or updates tags on one or more load balancers.
func (b *InMemoryBackend) AddTags(names []string, kvs []tags.KV) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		for _, kv := range kvs {
			lb.Tags.Set(kv.Key, kv.Value)
		}
	}

	return nil
}

// DescribeTags returns the tags for the given load balancers.
func (b *InMemoryBackend) DescribeTags(names []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string][]tags.KV, len(names))

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		kvs := make([]tags.KV, 0, lb.Tags.Len())
		lb.Tags.Range(func(k, v string) bool {
			kvs = append(kvs, tags.KV{Key: k, Value: v})

			return true
		})

		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

		result[name] = kvs
	}

	return result, nil
}

// RemoveTags removes the specified tag keys from one or more load balancers.
func (b *InMemoryBackend) RemoveTags(names []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		lb.Tags.DeleteKeys(keys)
	}

	return nil
}
