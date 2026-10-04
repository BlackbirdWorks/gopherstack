package iot

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneRuleAction deep-copies a RuleAction, including unmodeled action keys.
func cloneRuleAction(a RuleAction) RuleAction {
	out := RuleAction{}
	if a.SQS != nil {
		cp := *a.SQS
		out.SQS = &cp
	}
	if a.Lambda != nil {
		cp := *a.Lambda
		out.Lambda = &cp
	}
	if a.SNS != nil {
		cp := *a.SNS
		out.SNS = &cp
	}
	if len(a.Other) > 0 {
		out.Other = make(map[string]json.RawMessage, len(a.Other))
		for k, v := range a.Other {
			out.Other[k] = append(json.RawMessage(nil), v...)
		}
	}

	return out
}

func cloneRuleActions(in []RuleAction) []RuleAction {
	out := make([]RuleAction, len(in))
	for i, a := range in {
		out[i] = cloneRuleAction(a)
	}

	return out
}

func cloneErrorAction(a *RuleAction) *RuleAction {
	if a == nil {
		return nil
	}
	cp := cloneRuleAction(*a)

	return &cp
}

// cloneTopicRule creates a deep copy of a TopicRule.
func cloneTopicRule(r *TopicRule) *TopicRule {
	return &TopicRule{
		RuleName:         r.RuleName,
		ARN:              r.ARN,
		SQL:              r.SQL,
		AWSIoTSQLVersion: r.AWSIoTSQLVersion,
		Description:      r.Description,
		Enabled:          r.Enabled,
		CreatedAt:        r.CreatedAt,
		Actions:          cloneRuleActions(r.Actions),
		ErrorAction:      cloneErrorAction(r.ErrorAction),
	}
}

// CreateTopicRule creates a new IoT Topic Rule.
func (b *InMemoryBackend) CreateTopicRule(input *CreateTopicRuleInput) error {
	if input.RuleName == "" {
		return fmt.Errorf("%w: RuleName is required", ErrValidation)
	}

	b.mu.Lock("CreateTopicRule")
	defer b.mu.Unlock()

	if b.rules.Has(input.RuleName) {
		return fmt.Errorf("%w: rule %q already exists", ErrAlreadyExists, input.RuleName)
	}

	payload := input.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	actions := cloneRuleActions(payload.Actions)

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("rule/%s", input.RuleName))

	sqlVersion := payload.AWSIoTSQLVersion
	if sqlVersion == "" {
		sqlVersion = sqlVersion2015
	}

	if err := validateRuleSQL(payload.SQL, sqlVersion); err != nil {
		return err
	}

	b.rules.Put(&TopicRule{
		RuleName:         input.RuleName,
		ARN:              arn,
		SQL:              payload.SQL,
		AWSIoTSQLVersion: sqlVersion,
		Description:      payload.Description,
		Actions:          actions,
		ErrorAction:      cloneErrorAction(payload.ErrorAction),
		Enabled:          !payload.RuleDisabled,
		CreatedAt:        time.Now(),
	})
	b.putResourceTagsLocked(arn, input.Tags)

	return nil
}

// GetTopicRule returns a deep copy of an existing Topic Rule.
func (b *InMemoryBackend) GetTopicRule(ruleName string) (*TopicRule, error) {
	b.mu.RLock("GetTopicRule")
	defer b.mu.RUnlock()

	r, ok := b.rules.Get(ruleName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	return cloneTopicRule(r), nil
}

// ListTopicRules returns all Topic Rules sorted by name.
func (b *InMemoryBackend) ListTopicRules() []*TopicRule {
	b.mu.RLock("ListTopicRules")
	defer b.mu.RUnlock()

	items := b.rules.Snapshot()
	out := make([]*TopicRule, 0, len(items))

	for _, v := range items {
		out = append(out, cloneTopicRule(v))
	}

	return out
}

// DeleteTopicRule deletes a Topic Rule by name.
func (b *InMemoryBackend) DeleteTopicRule(ruleName string) error {
	b.mu.Lock("DeleteTopicRule")
	defer b.mu.Unlock()

	r, ok := b.rules.Get(ruleName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	b.rules.Delete(ruleName)
	delete(b.resourceTags, r.ARN)

	return nil
}

// DisableTopicRule disables an existing topic rule.
func (b *InMemoryBackend) DisableTopicRule(ruleName string) error {
	b.mu.Lock("DisableTopicRule")
	defer b.mu.Unlock()

	r, ok := b.rules.Get(ruleName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	r.Enabled = false

	return nil
}

// EnableTopicRule enables an existing topic rule.
func (b *InMemoryBackend) EnableTopicRule(ruleName string) error {
	b.mu.Lock("EnableTopicRule")
	defer b.mu.Unlock()

	r, ok := b.rules.Get(ruleName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	r.Enabled = true

	return nil
}

// ReplaceTopicRule replaces the payload of an existing topic rule.
func (b *InMemoryBackend) ReplaceTopicRule(input *ReplaceTopicRuleInput) error {
	if input.RuleName == "" {
		return fmt.Errorf("%w: RuleName is required", ErrValidation)
	}

	b.mu.Lock("ReplaceTopicRule")
	defer b.mu.Unlock()

	r, ok := b.rules.Get(input.RuleName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, input.RuleName)
	}

	payload := input.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	actions := cloneRuleActions(payload.Actions)

	sqlVersion := payload.AWSIoTSQLVersion
	if sqlVersion == "" {
		sqlVersion = sqlVersion2015
	}

	if err := validateRuleSQL(payload.SQL, sqlVersion); err != nil {
		return err
	}

	r.SQL = payload.SQL
	r.Description = payload.Description
	r.Actions = actions
	r.ErrorAction = cloneErrorAction(payload.ErrorAction)
	r.AWSIoTSQLVersion = sqlVersion
	r.Enabled = !payload.RuleDisabled

	return nil
}

// AddRuleInternal seeds a TopicRule directly into the backend for testing.
func (b *InMemoryBackend) AddRuleInternal(r TopicRule) {
	b.mu.Lock("AddRuleInternal")
	defer b.mu.Unlock()

	if r.ARN == "" {
		r.ARN = arn.Build("iot", b.region, b.accountID, fmt.Sprintf("rule/%s", r.RuleName))
	}

	if r.Actions == nil {
		r.Actions = []RuleAction{}
	}

	b.rules.Put(&r)
}

// CreateTopicRuleDestination creates a new topic rule destination.
func (b *InMemoryBackend) CreateTopicRuleDestination(
	input *CreateTopicRuleDestinationInput,
) (*TopicRuleDestination, error) {
	b.mu.Lock("CreateTopicRuleDestination")
	defer b.mu.Unlock()

	if cfg := input.DestinationConfiguration; cfg != nil && cfg.InfluxDBConfiguration != nil {
		if err := validateInfluxDBConfiguration(cfg.InfluxDBConfiguration); err != nil {
			return nil, err
		}
	}

	destType := "http"
	if cfg := input.DestinationConfiguration; cfg != nil {
		switch {
		case cfg.VPCConfiguration != nil:
			destType = "vpc"
		case cfg.InfluxDBConfiguration != nil:
			destType = "influxdb"
		}
	}

	arn := arn.Build("iot", b.region, b.accountID,
		fmt.Sprintf("ruledestination/%s/%s", destType, uuid.NewString()))

	now := time.Now()
	dest := &TopicRuleDestination{
		ARN:           arn,
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	switch {
	case input.DestinationConfiguration != nil && input.DestinationConfiguration.HTTPURLConfiguration != nil:
		dest.HTTPURLProperties = &HTTPURLDestinationProperties{
			ConfirmationURL: input.DestinationConfiguration.HTTPURLConfiguration.ConfirmationURL,
		}
		// HTTP destinations require confirmation before they can be used,
		// matching AWS's real IN_PROGRESS -> ENABLED lifecycle.
		dest.Status = statusInProgress
		dest.ConfirmationToken = randomHex(certIDHexLen)
	case input.DestinationConfiguration != nil && input.DestinationConfiguration.VPCConfiguration != nil:
		vpcCfg := input.DestinationConfiguration.VPCConfiguration
		dest.VPCProperties = &VPCDestinationProperties{
			RoleARN:        vpcCfg.RoleARN,
			SecurityGroups: vpcCfg.SecurityGroups,
			SubnetIDs:      vpcCfg.SubnetIDs,
			VpcID:          vpcCfg.VpcID,
		}
		// VPC destinations need no out-of-band confirmation.
		dest.Status = statusEnabled
	case input.DestinationConfiguration != nil && input.DestinationConfiguration.InfluxDBConfiguration != nil:
		cp := *input.DestinationConfiguration.InfluxDBConfiguration
		dest.InfluxDBProperties = &cp
		dest.Status = statusEnabled
	default:
		dest.Status = statusEnabled
	}

	b.topicRuleDestinations.Put(dest)

	return cloneTopicRuleDestination(dest), nil
}

// SetTopicRuleDestinationTimestampsInternal backdates a destination's
// CreatedAt/LastUpdatedAt for testing (mirrors AddRuleInternal/
// AddCommandInternal), letting tests control Update's LastUpdatedAt delta
// without depending on real-clock second-resolution timing.
func (b *InMemoryBackend) SetTopicRuleDestinationTimestampsInternal(arn string, createdAt, lastUpdatedAt time.Time) {
	b.mu.Lock("SetTopicRuleDestinationTimestampsInternal")
	defer b.mu.Unlock()

	dest, ok := b.topicRuleDestinations.Get(arn)
	if !ok {
		return
	}

	dest.CreatedAt = createdAt
	dest.LastUpdatedAt = lastUpdatedAt
}

// GetTopicRuleDestination returns a topic rule destination by ARN.
func (b *InMemoryBackend) GetTopicRuleDestination(arn string) (*TopicRuleDestination, error) {
	b.mu.RLock("GetTopicRuleDestination")
	defer b.mu.RUnlock()

	dest, ok := b.topicRuleDestinations.Get(arn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, arn)
	}

	return cloneTopicRuleDestination(dest), nil
}

// ListTopicRuleDestinations returns all topic rule destinations.
func (b *InMemoryBackend) ListTopicRuleDestinations() []*TopicRuleDestination {
	b.mu.RLock("ListTopicRuleDestinations")
	defer b.mu.RUnlock()

	items := b.topicRuleDestinations.Snapshot()
	out := make([]*TopicRuleDestination, 0, len(items))

	for _, v := range items {
		out = append(out, cloneTopicRuleDestination(v))
	}

	return out
}

// UpdateTopicRuleDestination updates the status of a topic rule destination.
func (b *InMemoryBackend) UpdateTopicRuleDestination(input *UpdateTopicRuleDestinationInput) error {
	b.mu.Lock("UpdateTopicRuleDestination")
	defer b.mu.Unlock()

	dest, ok := b.topicRuleDestinations.Get(input.ARN)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, input.ARN)
	}

	dest.Status = input.Status
	dest.LastUpdatedAt = time.Now()

	return nil
}

// DeleteTopicRuleDestination deletes a topic rule destination by ARN.
func (b *InMemoryBackend) DeleteTopicRuleDestination(arn string) error {
	b.mu.Lock("DeleteTopicRuleDestination")
	defer b.mu.Unlock()

	if !b.topicRuleDestinations.Has(arn) {
		return fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, arn)
	}

	b.topicRuleDestinations.Delete(arn)

	return nil
}

// ConfirmTopicRuleDestination transitions a topic rule destination created
// with an HTTP URL configuration from IN_PROGRESS to ENABLED, given the
// confirmation token that was generated at creation time.
func (b *InMemoryBackend) ConfirmTopicRuleDestination(token string) error {
	if token == "" {
		return fmt.Errorf("%w: confirmationToken is required", ErrValidation)
	}

	b.mu.Lock("ConfirmTopicRuleDestination")
	defer b.mu.Unlock()

	for _, dest := range b.topicRuleDestinations.All() {
		if dest.ConfirmationToken != "" && dest.ConfirmationToken == token {
			dest.Status = statusEnabled
			dest.ConfirmationToken = ""
			dest.LastUpdatedAt = time.Now()

			return nil
		}
	}

	return fmt.Errorf("%w: invalid or expired confirmation token", ErrValidation)
}

func cloneTopicRuleDestination(d *TopicRuleDestination) *TopicRuleDestination {
	cp := *d
	if d.HTTPURLProperties != nil {
		p := *d.HTTPURLProperties
		cp.HTTPURLProperties = &p
	}
	if d.VPCProperties != nil {
		p := *d.VPCProperties
		p.SecurityGroups = append([]string(nil), d.VPCProperties.SecurityGroups...)
		p.SubnetIDs = append([]string(nil), d.VPCProperties.SubnetIDs...)
		cp.VPCProperties = &p
	}
	if d.InfluxDBProperties != nil {
		p := *d.InfluxDBProperties
		cp.InfluxDBProperties = &p
	}

	return &cp
}

// validateInfluxDBConfiguration enforces the required members and the V2/V3 enums
// (types.InfluxDBVersion, types.InfluxDBSecretType, iot@v1.83.0 enums.go).
func validateInfluxDBConfiguration(c *InfluxDBDestinationProperties) error {
	if c.Endpoint == "" || c.SecretID == "" {
		return fmt.Errorf("%w: influxDBConfiguration requires endpoint and secretId", ErrValidation)
	}
	if c.InfluxDBVersion != "V2" && c.InfluxDBVersion != "V3" {
		return fmt.Errorf("%w: invalid influxDBVersion %q", ErrValidation, c.InfluxDBVersion)
	}
	if c.SecretType != "" && c.SecretType != "SecretString" && c.SecretType != "SecretBinary" {
		return fmt.Errorf("%w: invalid secretType %q", ErrValidation, c.SecretType)
	}

	return nil
}

func validateRuleSQL(sql, version string) error {
	if !validSQLVersion(version) {
		return fmt.Errorf("%w: unsupported awsIotSqlVersion %q", ErrValidation, version)
	}

	if _, err := ParseRuleSQLVersion(sql, version); err != nil {
		return err
	}

	return nil
}
