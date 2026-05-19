package cloudwatchlogs

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

var (
	ErrResourcePolicyNotFound      = fmt.Errorf("ResourceNotFoundException")
	ErrDeliveryDestinationNotFound = fmt.Errorf("ResourceNotFoundException")
	ErrDeliverySourceNotFound      = fmt.Errorf("ResourceNotFoundException")
	ErrDestinationNotFound         = fmt.Errorf("ResourceNotFoundException")
	ErrIndexPolicyNotFound         = fmt.Errorf("ResourceNotFoundException")
	ErrTransformerNotFound         = fmt.Errorf("ResourceNotFoundException")
	ErrIntegrationNotFound         = fmt.Errorf("ResourceNotFoundException")
)

// ---- ResourcePolicy ----

// ResourcePolicy represents a CloudWatch Logs resource policy.
type ResourcePolicy struct {
	PolicyName     string
	PolicyDocument string
	LastUpdated    time.Time
}

// PutResourcePolicy creates or updates a resource-based policy.
func (b *InMemoryBackend) PutResourcePolicy(policyName, policyDocument string) (*ResourcePolicy, error) {
	if policyName == "" {
		return nil, fmt.Errorf("%w: policyName is required", ErrValidation)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	p := ResourcePolicy{
		PolicyName:     policyName,
		PolicyDocument: policyDocument,
		LastUpdated:    time.Now().UTC(),
	}
	b.resourcePolicies[policyName] = p

	return &p, nil
}

// DescribeResourcePolicies returns all resource policies, sorted by name.
func (b *InMemoryBackend) DescribeResourcePolicies() []ResourcePolicy {
	b.mu.RLock("DescribeResourcePolicies")
	defer b.mu.RUnlock()

	out := make([]ResourcePolicy, 0, len(b.resourcePolicies))
	for _, p := range b.resourcePolicies {
		out = append(out, p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	return out
}

// DeleteResourcePolicy removes a resource policy by name.
func (b *InMemoryBackend) DeleteResourcePolicy(policyName string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[policyName]; !ok {
		return fmt.Errorf("%w: resource policy %q not found", ErrResourcePolicyNotFound, policyName)
	}

	delete(b.resourcePolicies, policyName)

	return nil
}

// ---- DeliveryDestination ----

// DeliveryDestination represents a CloudWatch Logs delivery destination.
type DeliveryDestination struct {
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	OutputFormat string            `json:"outputFormat,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	CreatedAt    time.Time         `json:"-"`
	// DeliveryDestinationConfiguration holds the target ARN.
	TargetArn string `json:"deliveryDestinationConfiguration,omitempty"`
	// Policy stored separately
	Policy string `json:"-"`
}

// PutDeliveryDestination creates or updates a delivery destination.
func (b *InMemoryBackend) PutDeliveryDestination(
	name, targetArn, outputFormat string,
	tags map[string]string,
) (*DeliveryDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("PutDeliveryDestination")
	defer b.mu.Unlock()

	existing, exists := b.deliveryDestinations[name]
	if exists {
		existing.TargetArn = targetArn
		existing.OutputFormat = outputFormat
		if tags != nil {
			existing.Tags = tags
		}
		b.deliveryDestinations[name] = existing

		return &existing, nil
	}

	dest := DeliveryDestination{
		Name:         name,
		Arn:          "arn:aws:logs:" + b.region + ":" + b.accountID + ":delivery-destination:" + name,
		TargetArn:    targetArn,
		OutputFormat: outputFormat,
		Tags:         tags,
		CreatedAt:    time.Now().UTC(),
	}
	b.deliveryDestinations[name] = dest

	return &dest, nil
}

// GetDeliveryDestination returns a delivery destination by name.
func (b *InMemoryBackend) GetDeliveryDestination(name string) (*DeliveryDestination, error) {
	b.mu.RLock("GetDeliveryDestination")
	defer b.mu.RUnlock()

	dest, ok := b.deliveryDestinations[name]
	if !ok {
		return nil, fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	return &dest, nil
}

// DescribeDeliveryDestinations returns all delivery destinations sorted by name.
func (b *InMemoryBackend) DescribeDeliveryDestinations() []DeliveryDestination {
	b.mu.RLock("DescribeDeliveryDestinations")
	defer b.mu.RUnlock()

	out := make([]DeliveryDestination, 0, len(b.deliveryDestinations))
	for _, d := range b.deliveryDestinations {
		out = append(out, d)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteDeliveryDestination removes a delivery destination by name.
func (b *InMemoryBackend) DeleteDeliveryDestination(name string) error {
	b.mu.Lock("DeleteDeliveryDestination")
	defer b.mu.Unlock()

	if _, ok := b.deliveryDestinations[name]; !ok {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	delete(b.deliveryDestinations, name)

	return nil
}

// PutDeliveryDestinationPolicy stores a policy on a delivery destination.
func (b *InMemoryBackend) PutDeliveryDestinationPolicy(name, policy string) error {
	b.mu.Lock("PutDeliveryDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.deliveryDestinations[name]
	if !ok {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	dest.Policy = policy
	b.deliveryDestinations[name] = dest

	return nil
}

// GetDeliveryDestinationPolicy returns the policy for a delivery destination.
func (b *InMemoryBackend) GetDeliveryDestinationPolicy(name string) (string, error) {
	b.mu.RLock("GetDeliveryDestinationPolicy")
	defer b.mu.RUnlock()

	dest, ok := b.deliveryDestinations[name]
	if !ok {
		return "", fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	return dest.Policy, nil
}

// DeleteDeliveryDestinationPolicy removes the policy from a delivery destination.
func (b *InMemoryBackend) DeleteDeliveryDestinationPolicy(name string) error {
	b.mu.Lock("DeleteDeliveryDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.deliveryDestinations[name]
	if !ok {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	dest.Policy = ""
	b.deliveryDestinations[name] = dest

	return nil
}

// ---- DeliverySource ----

// DeliverySource represents a CloudWatch Logs delivery source.
type DeliverySource struct {
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	LogType      string            `json:"logType,omitempty"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	CreatedAt    time.Time         `json:"-"`
}

// PutDeliverySource creates or updates a delivery source.
func (b *InMemoryBackend) PutDeliverySource(
	name, logType string,
	resourceArns []string,
	tags map[string]string,
) (*DeliverySource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("PutDeliverySource")
	defer b.mu.Unlock()

	existing, exists := b.deliverySources[name]
	if exists {
		existing.LogType = logType
		existing.ResourceArns = resourceArns
		if tags != nil {
			existing.Tags = tags
		}
		b.deliverySources[name] = existing

		return &existing, nil
	}

	src := DeliverySource{
		Name:         name,
		Arn:          "arn:aws:logs:" + b.region + ":" + b.accountID + ":delivery-source:" + name,
		LogType:      logType,
		ResourceArns: resourceArns,
		Tags:         tags,
		CreatedAt:    time.Now().UTC(),
	}
	b.deliverySources[name] = src

	return &src, nil
}

// GetDeliverySource returns a delivery source by name.
func (b *InMemoryBackend) GetDeliverySource(name string) (*DeliverySource, error) {
	b.mu.RLock("GetDeliverySource")
	defer b.mu.RUnlock()

	src, ok := b.deliverySources[name]
	if !ok {
		return nil, fmt.Errorf("%w: delivery source %q not found", ErrDeliverySourceNotFound, name)
	}

	return &src, nil
}

// DescribeDeliverySources returns all delivery sources sorted by name.
func (b *InMemoryBackend) DescribeDeliverySources() []DeliverySource {
	b.mu.RLock("DescribeDeliverySources")
	defer b.mu.RUnlock()

	out := make([]DeliverySource, 0, len(b.deliverySources))
	for _, s := range b.deliverySources {
		out = append(out, s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteDeliverySource removes a delivery source by name.
func (b *InMemoryBackend) DeleteDeliverySource(name string) error {
	b.mu.Lock("DeleteDeliverySource")
	defer b.mu.Unlock()

	if _, ok := b.deliverySources[name]; !ok {
		return fmt.Errorf("%w: delivery source %q not found", ErrDeliverySourceNotFound, name)
	}

	delete(b.deliverySources, name)

	return nil
}

// ---- Destination (CWL log routing) ----

// CWLDestination represents a CloudWatch Logs log routing destination.
type CWLDestination struct {
	DestinationName string    `json:"destinationName"`
	TargetArn       string    `json:"targetArn"`
	RoleArn         string    `json:"roleArn"`
	AccessPolicy    string    `json:"accessPolicy,omitempty"`
	Arn             string    `json:"arn"`
	CreatedAt       time.Time `json:"-"`
}

// PutDestination creates or updates a log routing destination.
func (b *InMemoryBackend) PutDestination(name, targetArn, roleArn string) (*CWLDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: destinationName is required", ErrValidation)
	}

	b.mu.Lock("PutDestination")
	defer b.mu.Unlock()

	existing, exists := b.destinations[name]
	if exists {
		existing.TargetArn = targetArn
		existing.RoleArn = roleArn
		b.destinations[name] = existing

		return &existing, nil
	}

	dest := CWLDestination{
		DestinationName: name,
		TargetArn:       targetArn,
		RoleArn:         roleArn,
		Arn:             "arn:aws:logs:" + b.region + ":" + b.accountID + ":destination:" + name,
		CreatedAt:       time.Now().UTC(),
	}
	b.destinations[name] = dest

	return &dest, nil
}

// PutDestinationPolicy attaches an access policy to a destination.
func (b *InMemoryBackend) PutDestinationPolicy(name, policy string) error {
	b.mu.Lock("PutDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.destinations[name]
	if !ok {
		return fmt.Errorf("%w: destination %q not found", ErrDestinationNotFound, name)
	}

	dest.AccessPolicy = policy
	b.destinations[name] = dest

	return nil
}

// DescribeDestinations returns destinations optionally filtered by name prefix.
func (b *InMemoryBackend) DescribeDestinations(namePrefix string) []CWLDestination {
	b.mu.RLock("DescribeDestinations")
	defer b.mu.RUnlock()

	out := make([]CWLDestination, 0, len(b.destinations))

	for _, d := range b.destinations {
		if namePrefix == "" || strings.HasPrefix(d.DestinationName, namePrefix) {
			out = append(out, d)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].DestinationName < out[j].DestinationName })

	return out
}

// DeleteDestination removes a log routing destination.
func (b *InMemoryBackend) DeleteDestination(name string) error {
	b.mu.Lock("DeleteDestination")
	defer b.mu.Unlock()

	if _, ok := b.destinations[name]; !ok {
		return fmt.Errorf("%w: destination %q not found", ErrDestinationNotFound, name)
	}

	delete(b.destinations, name)

	return nil
}

// ---- IndexPolicy ----

// IndexPolicy represents a CloudWatch Logs field index policy.
type IndexPolicy struct {
	LogGroupIdentifier string    `json:"logGroupIdentifier"`
	PolicyDocument     string    `json:"policyDocument"`
	LastUpdated        time.Time `json:"lastUpdateTime"`
}

// PutIndexPolicy creates or updates an index policy for a log group.
func (b *InMemoryBackend) PutIndexPolicy(logGroupIdentifier, policyDocument string) (*IndexPolicy, error) {
	if logGroupIdentifier == "" {
		return nil, fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutIndexPolicy")
	defer b.mu.Unlock()

	p := IndexPolicy{
		LogGroupIdentifier: logGroupIdentifier,
		PolicyDocument:     policyDocument,
		LastUpdated:        time.Now().UTC(),
	}
	b.indexPolicies[logGroupIdentifier] = p

	return &p, nil
}

// DescribeIndexPolicies returns all index policies sorted by log group identifier.
func (b *InMemoryBackend) DescribeIndexPolicies() []IndexPolicy {
	b.mu.RLock("DescribeIndexPolicies")
	defer b.mu.RUnlock()

	out := make([]IndexPolicy, 0, len(b.indexPolicies))
	for _, p := range b.indexPolicies {
		out = append(out, p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].LogGroupIdentifier < out[j].LogGroupIdentifier })

	return out
}

// DeleteIndexPolicy removes the index policy for a log group.
func (b *InMemoryBackend) DeleteIndexPolicy(logGroupIdentifier string) error {
	b.mu.Lock("DeleteIndexPolicy")
	defer b.mu.Unlock()

	if _, ok := b.indexPolicies[logGroupIdentifier]; !ok {
		return fmt.Errorf("%w: index policy for %q not found", ErrIndexPolicyNotFound, logGroupIdentifier)
	}

	delete(b.indexPolicies, logGroupIdentifier)

	return nil
}

// ---- Transformer ----

// Transformer represents a CloudWatch Logs log transformer.
type Transformer struct {
	LogGroupIdentifier string           `json:"logGroupIdentifier"`
	Processors         []map[string]any `json:"transformerConfig"`
	CreatedAt          time.Time        `json:"-"`
}

// PutTransformer creates or updates a log transformer.
func (b *InMemoryBackend) PutTransformer(logGroupIdentifier string, processors []map[string]any) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutTransformer")
	defer b.mu.Unlock()

	b.transformers[logGroupIdentifier] = Transformer{
		LogGroupIdentifier: logGroupIdentifier,
		Processors:         processors,
		CreatedAt:          time.Now().UTC(),
	}

	return nil
}

// GetTransformer returns the transformer for a log group.
func (b *InMemoryBackend) GetTransformer(logGroupIdentifier string) (*Transformer, error) {
	b.mu.RLock("GetTransformer")
	defer b.mu.RUnlock()

	t, ok := b.transformers[logGroupIdentifier]
	if !ok {
		return nil, fmt.Errorf("%w: transformer for %q not found", ErrTransformerNotFound, logGroupIdentifier)
	}

	return &t, nil
}

// DeleteTransformer removes the transformer for a log group.
func (b *InMemoryBackend) DeleteTransformer(logGroupIdentifier string) error {
	b.mu.Lock("DeleteTransformer")
	defer b.mu.Unlock()

	if _, ok := b.transformers[logGroupIdentifier]; !ok {
		return fmt.Errorf("%w: transformer for %q not found", ErrTransformerNotFound, logGroupIdentifier)
	}

	delete(b.transformers, logGroupIdentifier)

	return nil
}

// ---- Integration ----

// CWLIntegration represents a CloudWatch Logs integration (e.g. OpenSearch).
type CWLIntegration struct {
	Name      string    `json:"integrationName"`
	Type      string    `json:"integrationType"`
	Status    string    `json:"integrationStatus"`
	CreatedAt time.Time `json:"-"`
}

// PutIntegration creates or updates an integration.
func (b *InMemoryBackend) PutIntegration(name, integrationType string) (*CWLIntegration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: integrationName is required", ErrValidation)
	}

	b.mu.Lock("PutIntegration")
	defer b.mu.Unlock()

	ig := CWLIntegration{
		Name:      name,
		Type:      integrationType,
		Status:    "ACTIVE",
		CreatedAt: time.Now().UTC(),
	}
	b.integrations[name] = ig

	return &ig, nil
}

// GetIntegration returns an integration by name.
func (b *InMemoryBackend) GetIntegration(name string) (*CWLIntegration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	ig, ok := b.integrations[name]
	if !ok {
		return nil, fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}

	return &ig, nil
}

// ListIntegrations returns all integrations sorted by name.
func (b *InMemoryBackend) ListIntegrations() []CWLIntegration {
	b.mu.RLock("ListIntegrations")
	defer b.mu.RUnlock()

	out := make([]CWLIntegration, 0, len(b.integrations))
	for _, ig := range b.integrations {
		out = append(out, ig)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteIntegration removes an integration by name.
func (b *InMemoryBackend) DeleteIntegration(name string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if _, ok := b.integrations[name]; !ok {
		return fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}

	delete(b.integrations, name)

	return nil
}

// ---- Log Group Deletion Protection ----

// SetLogGroupDeletionProtection enables or disables deletion protection for a log group.
func (b *InMemoryBackend) SetLogGroupDeletionProtection(logGroupIdentifier string, protected bool) error {
	if logGroupIdentifier == "" {
		return fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("SetLogGroupDeletionProtection")
	defer b.mu.Unlock()

	b.deletionProtected[logGroupIdentifier] = protected

	return nil
}

// IsLogGroupDeletionProtected returns whether deletion protection is enabled.
func (b *InMemoryBackend) IsLogGroupDeletionProtected(logGroupIdentifier string) bool {
	b.mu.RLock("IsLogGroupDeletionProtected")
	defer b.mu.RUnlock()

	return b.deletionProtected[logGroupIdentifier]
}

// ---- UpdateDeliveryConfiguration ----

// UpdateDeliveryConfiguration updates the field delimiter for a delivery.
func (b *InMemoryBackend) UpdateDeliveryConfiguration(id, fieldDelimiter string, recordFields []string) error {
	b.mu.Lock("UpdateDeliveryConfiguration")
	defer b.mu.Unlock()

	delivery, ok := b.deliveries[id]
	if !ok {
		return fmt.Errorf("%w: delivery %q not found", ErrDeliveryNotFound, id)
	}

	if fieldDelimiter != "" {
		delivery.FieldDelimiter = fieldDelimiter
	}

	if len(recordFields) > 0 {
		delivery.RecordFields = recordFields
	}

	b.deliveries[id] = delivery

	return nil
}
