package redshift

import "fmt"

// Real AWS's NamespaceRegistrationStatus enum (redshift@v1.65.4
// types/enums.go) only declares these two values: Register/DeregisterNamespace
// each report the in-flight transition, never a terminal "Registered" state --
// there's no describe/list op in this SDK for a client to observe past that.
const (
	namespaceRegistrationStatusRegistering   = "Registering"
	namespaceRegistrationStatusDeregistering = "Deregistering"
)

// NamespaceRegistration is the Glue Data Catalog registration state of a
// cluster or Redshift Serverless namespace/workgroup for a set of consumer
// accounts (RegisterNamespace/DeregisterNamespace; redshift@v1.65.4
// api_op_RegisterNamespace.go, api_op_DeregisterNamespace.go).
// NamespaceIdentifier on the wire is a union (NamespaceIdentifierUnion,
// verified against serializers.go's
// awsAwsquery_serializeDocumentNamespaceIdentifierUnion): exactly one of
// ClusterIdentifier or the ServerlessNamespaceName/ServerlessWorkgroupName
// pair is populated per record, matching the ProvisionedIdentifier/
// ServerlessIdentifier variants.
type NamespaceRegistration struct {
	NamespaceKey            string   `json:"namespaceKey"`
	ClusterIdentifier       string   `json:"clusterIdentifier,omitempty"`
	ServerlessNamespaceName string   `json:"serverlessNamespaceName,omitempty"`
	ServerlessWorkgroupName string   `json:"serverlessWorkgroupName,omitempty"`
	Status                  string   `json:"status"`
	ConsumerIdentifiers     []string `json:"consumerIdentifiers"`
}

func cloneNamespaceRegistration(r *NamespaceRegistration) *NamespaceRegistration {
	out := *r
	out.ConsumerIdentifiers = cloneStrings(r.ConsumerIdentifiers)

	return &out
}

// RegisterNamespace registers a cluster or Redshift Serverless namespace to
// the Glue Data Catalog for consumerIdentifiers. Real RegisterNamespaceOutput
// carries only a Status field (always "Registering", the in-flight
// transition state; see the const doc comment above) -- so what makes this
// operation "real" rather than a canned response is that it validates
// NamespaceIdentifier against actual backend state before accepting it
// (ClusterNotFound/InvalidClusterState/InvalidNamespaceFault, the only
// exceptions declared in awsAwsquery_deserializeOpErrorRegisterNamespace) and
// persists the registration so DeregisterNamespace has real prior state to
// mutate.
func (b *InMemoryBackend) RegisterNamespace(
	consumerIdentifiers []string, clusterIdentifier, serverlessNamespace, serverlessWorkgroup string,
) (*NamespaceRegistration, error) {
	if len(consumerIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: ConsumerIdentifiers is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterNamespace")
	defer b.mu.Unlock()

	key, err := b.resolveNamespaceIdentityLocked(clusterIdentifier, serverlessNamespace, serverlessWorkgroup)
	if err != nil {
		return nil, err
	}

	reg := &NamespaceRegistration{
		ConsumerIdentifiers:     cloneStrings(consumerIdentifiers),
		NamespaceKey:            key,
		ClusterIdentifier:       clusterIdentifier,
		ServerlessNamespaceName: serverlessNamespace,
		ServerlessWorkgroupName: serverlessWorkgroup,
		Status:                  namespaceRegistrationStatusRegistering,
	}
	b.namespaceRegistrations.Put(reg)

	return cloneNamespaceRegistration(reg), nil
}

// DeregisterNamespace deregisters consumerIdentifiers from the cluster or
// Redshift Serverless namespace's Glue Data Catalog registration, removing
// exactly those consumers from the previously-registered set (real AWS scopes
// deregistration to the given ConsumerIdentifiers, not the whole namespace).
// A namespace with no prior registration deregisters against an empty
// consumer set rather than erroring: the declared error switch has no
// "registration not found" fault, only NamespaceIdentifier-shape errors.
func (b *InMemoryBackend) DeregisterNamespace(
	consumerIdentifiers []string, clusterIdentifier, serverlessNamespace, serverlessWorkgroup string,
) (*NamespaceRegistration, error) {
	if len(consumerIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: ConsumerIdentifiers is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeregisterNamespace")
	defer b.mu.Unlock()

	key, err := b.resolveNamespaceIdentityLocked(clusterIdentifier, serverlessNamespace, serverlessWorkgroup)
	if err != nil {
		return nil, err
	}

	var remaining []string
	if existing, ok := b.namespaceRegistrations.Get(key); ok {
		remaining = removeStrings(existing.ConsumerIdentifiers, consumerIdentifiers)
	}

	reg := &NamespaceRegistration{
		ConsumerIdentifiers:     remaining,
		NamespaceKey:            key,
		ClusterIdentifier:       clusterIdentifier,
		ServerlessNamespaceName: serverlessNamespace,
		ServerlessWorkgroupName: serverlessWorkgroup,
		Status:                  namespaceRegistrationStatusDeregistering,
	}
	b.namespaceRegistrations.Put(reg)

	return cloneNamespaceRegistration(reg), nil
}

// resolveNamespaceIdentityLocked validates NamespaceIdentifier against real
// backend state and returns the registration's storage key. Must be called
// with b.mu already held. clusterIdentifier takes precedence when both are
// somehow set, matching NamespaceIdentifierUnion's "exactly one member"
// contract -- the wire can only ever populate one branch per parseNamespaceIdentifier.
func (b *InMemoryBackend) resolveNamespaceIdentityLocked(
	clusterIdentifier, serverlessNamespace, serverlessWorkgroup string,
) (string, error) {
	switch {
	case clusterIdentifier != "":
		cluster, ok := b.clusters.Get(clusterIdentifier)
		if !ok {
			return "", fmt.Errorf("%w: cluster %q not found", ErrClusterNotFound, clusterIdentifier)
		}
		if cluster.Status != clusterStatusAvailable {
			return "", fmt.Errorf(
				"%w: cluster %q is not in a registerable state (status %q)",
				ErrNamespaceRegistrationInvalidClusterState, clusterIdentifier, cluster.Status,
			)
		}

		return "cluster:" + clusterIdentifier, nil

	case serverlessNamespace != "" && serverlessWorkgroup != "":
		if _, ok := b.slNamespaces.Get(serverlessNamespace); !ok {
			return "", fmt.Errorf("%w: serverless namespace %q not found", ErrInvalidNamespace, serverlessNamespace)
		}
		if _, ok := b.slWorkgroups.Get(serverlessWorkgroup); !ok {
			return "", fmt.Errorf("%w: serverless workgroup %q not found", ErrInvalidNamespace, serverlessWorkgroup)
		}

		return "serverless:" + serverlessNamespace + "/" + serverlessWorkgroup, nil

	default:
		return "", fmt.Errorf(
			"%w: NamespaceIdentifier did not resolve to a cluster or serverless namespace",
			ErrInvalidNamespace,
		)
	}
}

// removeStrings returns list with every element also present in remove
// dropped, preserving list's original order.
func removeStrings(list, remove []string) []string {
	if len(list) == 0 {
		return nil
	}

	drop := make(map[string]struct{}, len(remove))
	for _, r := range remove {
		drop[r] = struct{}{}
	}

	out := make([]string, 0, len(list))
	for _, v := range list {
		if _, ok := drop[v]; !ok {
			out = append(out, v)
		}
	}

	return out
}
