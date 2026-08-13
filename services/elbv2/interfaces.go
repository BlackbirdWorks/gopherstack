package elbv2

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend is the interface for ELBv2 storage operations.
type StorageBackend interface {
	CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error)
	DescribeLoadBalancers(arns []string, names []string) ([]LoadBalancer, error)
	DeleteLoadBalancer(lbArn string) error
	ModifyLoadBalancerAttributes(lbArn string, attrs map[string]string) (*LoadBalancer, error)
	SetSecurityGroups(lbArn string, sgs []string) (*LoadBalancer, error)
	SetSubnets(lbArn string, mappings []SubnetMapping) (*LoadBalancer, error)
	SetIPAddressType(lbArn string, ipType string) (*LoadBalancer, error)
	CreateTargetGroup(input CreateTargetGroupInput) (*TargetGroup, error)
	DescribeTargetGroups(arns []string, names []string, lbArn string) ([]TargetGroup, error)
	DeleteTargetGroup(tgArn string) error
	ModifyTargetGroup(input ModifyTargetGroupInput) (*TargetGroup, error)
	ModifyTargetGroupAttributes(tgArn string, attrs map[string]string) (*TargetGroup, error)
	DescribeTargetGroupAttributes(tgArn string) (map[string]string, error)
	RegisterTargets(tgArn string, targets []Target) error
	DeregisterTargets(tgArn string, targets []Target) error
	DescribeTargetHealth(tgArn string) ([]TargetHealthDescription, error)
	CreateListener(input CreateListenerInput) (*Listener, error)
	DescribeListeners(lbArn string, listenerArns []string) ([]Listener, error)
	DeleteListener(listenerArn string) error
	ModifyListener(input ModifyListenerInput) (*Listener, error)
	ModifyListenerAttributes(listenerArn string, attrs map[string]string) (*Listener, error)
	DescribeListenerAttributes(listenerArn string) (map[string]string, error)
	CreateRule(input CreateRuleInput) (*Rule, error)
	DescribeRules(listenerArn string, ruleArns []string) ([]Rule, error)
	DeleteRule(ruleArn string) error
	ModifyRule(
		ruleArn string, actions []Action, conditions []Condition,
		transforms []RuleTransform, resetTransforms bool,
	) (*Rule, error)
	AddTags(resourceArns []string, kvs []tags.KV) error
	RemoveTags(resourceArns []string, keys []string) error
	DescribeTags(resourceArns []string) (map[string][]tags.KV, error)
	// TrustStore operations.
	CreateTrustStore(name string, kvs []tags.KV) (*TrustStore, error)
	DescribeTrustStores(arns []string, names []string) ([]TrustStore, error)
	DeleteTrustStore(trustStoreArn string) error
	ModifyTrustStore(trustStoreArn string) (*TrustStore, error)
	AddTrustStoreRevocations(
		trustStoreArn string,
		contents []RevocationContentInput,
	) ([]TrustStoreRevocation, error)
	RemoveTrustStoreRevocations(trustStoreArn string, revocationIDs []int64) error
	DescribeTrustStoreRevocations(trustStoreArn string) ([]TrustStoreRevocation, error)
	DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error)
	DeleteSharedTrustStoreAssociation(trustStoreArn, resourceArn string) error
	// Capacity reservation operations.
	ModifyCapacityReservation(lbArn string, minimumCapacityUnits *int32, reset bool) (*CapacityReservation, error)
	DescribeCapacityReservation(lbArn string) (*CapacityReservation, error)
	// IP pool operations.
	ModifyIPPools(lbArn string, ipv4PoolID *string, removeIPv4 bool) (*LoadBalancer, error)
	// Resource policy operations.
	GetResourcePolicy(resourceArn string) (string, error)
	PutResourcePolicy(resourceArn, policy string) error
	// Rule priority operations.
	SetRulePriorities(priorities []RulePriority) ([]Rule, error)
	// Listener certificate operations.
	AddListenerCertificates(listenerArn string, certs []Certificate) error
	DescribeListenerCertificates(listenerArn string) ([]Certificate, error)
	RemoveListenerCertificates(listenerArn string, certArns []string) error
}
