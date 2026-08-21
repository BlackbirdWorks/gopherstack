package securityhub

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// knownStandards defines the built-in standards available in SecurityHub.
var knownStandards = []Standard{ //nolint:gochecknoglobals // read-only lookup data
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
		Name:         "AWS Foundational Security Best Practices v1.0.0",
		Description: "The AWS Foundational Security Best Practices standard is a set of controls that detect " +
			"when your deployed accounts and resources deviate from security best practices.",
		EnabledByDefault: true,
	},
	{
		StandardsArn: "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0",
		Name:         "CIS AWS Foundations Benchmark v1.2.0",
		Description: "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.2.0 is a set of " +
			"security configuration best practices for AWS.",
		EnabledByDefault: true,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/cis-aws-foundations-benchmark/v/1.4.0",
		Name:         "CIS AWS Foundations Benchmark v1.4.0",
		Description: "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.4.0 is a set of " +
			"security configuration best practices for AWS.",
		EnabledByDefault: false,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/pci-dss/v/3.2.1",
		Name:         "PCI DSS v3.2.1",
		Description: "The Payment Card Industry Data Security Standard (PCI DSS) is a proprietary " +
			"information security standard.",
		EnabledByDefault: false,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/nist-800-53/v/5.0.0",
		Name:         "NIST Special Publication 800-53 Revision 5",
		Description: "The NIST Special Publication 800-53 Revision 5 standard provides a catalog of " +
			"security and privacy controls.",
		EnabledByDefault: false,
	},
}

func (b *InMemoryBackend) subscriptionARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("subscription/%d", seq))
}

// unused: keep compiler happy
var _ = (*InMemoryBackend).subscriptionARN

func (b *InMemoryBackend) BatchEnableStandards(requests []map[string]any) ([]*StandardsSubscription, []map[string]any) {
	b.mu.Lock("BatchEnableStandards")
	defer b.mu.Unlock()

	var subscriptions []*StandardsSubscription
	var failures []map[string]any

	for _, req := range requests {
		standardsArn, _ := req[keyStandardsArn].(string)

		if standardsArn == "" {
			failures = append(failures, map[string]any{
				keyStandardsArn: standardsArn,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: "StandardsArn is required",
			})

			continue
		}

		b.standardsSeq++
		subArn := arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("subscription/%d", b.standardsSeq))

		sub := &StandardsSubscription{
			StandardsSubscriptionArn: subArn,
			StandardsArn:             standardsArn,
			StandardsInput:           map[string]string{},
			StandardsStatus:          "PENDING",
		}

		if input, ok := req["StandardsInput"].(map[string]any); ok {
			for k, v := range input {
				if sv, isStr := v.(string); isStr {
					sub.StandardsInput[k] = sv
				}
			}
		}

		b.standardsSubscriptions.Put(sub)
		subscriptions = append(subscriptions, sub)
	}

	if subscriptions == nil {
		subscriptions = []*StandardsSubscription{}
	}

	if failures == nil {
		failures = []map[string]any{}
	}

	return subscriptions, failures
}

func (b *InMemoryBackend) BatchDisableStandards(
	subscriptionArns []string,
) ([]*StandardsSubscription, []map[string]any) {
	b.mu.Lock("BatchDisableStandards")
	defer b.mu.Unlock()

	var subscriptions []*StandardsSubscription
	var failures []map[string]any

	for _, arn := range subscriptionArns {
		sub, ok := b.standardsSubscriptions.Get(arn)
		if !ok {
			failures = append(failures, map[string]any{
				"StandardsSubscriptionArn": arn,
				keyErrorCode:               errCodeInvalidInput,
				keyErrorMessage:            "StandardsSubscription not found",
			})

			continue
		}

		sub.StandardsStatus = "DELETING"
		subscriptions = append(subscriptions, sub)
		b.standardsSubscriptions.Delete(arn)
	}

	if subscriptions == nil {
		subscriptions = []*StandardsSubscription{}
	}

	if failures == nil {
		failures = []map[string]any{}
	}

	return subscriptions, failures
}

// GetEnabledStandards returns enabled standards subscriptions, advancing any
// PENDING subscription to READY on first poll. BatchEnableStandards stamped
// StandardsStatus PENDING and nothing else in this backend ever advanced it
// -- EnableHub's own default-standards subscriptions are stamped the
// terminal READY directly (no async work modeled for those either), so the
// contrast is exactly the sibling-resource pattern this bug class hides
// behind. A client polling GetEnabledStandards for readiness never saw a
// terminal status.
func (b *InMemoryBackend) GetEnabledStandards(
	subscriptionArns []string,
	nextToken string,
	maxResults int,
) ([]*StandardsSubscription, string) {
	b.mu.Lock("GetEnabledStandards")
	defer b.mu.Unlock()
	results := filterOrAll(subscriptionArns, b.standardsSubscriptions)

	for _, sub := range results {
		if sub.StandardsStatus == "PENDING" {
			sub.pollCount++
			if sub.pollCount >= 1 {
				sub.StandardsStatus = statusReady
			}
		}
	}

	return paginateSlice(results, nextToken, maxResults, maxStandardsResults)
}

func (b *InMemoryBackend) DescribeStandards(nextToken string, maxResults int) ([]*Standard, string) {
	b.mu.RLock("DescribeStandards")
	defer b.mu.RUnlock()

	results := make([]*Standard, len(knownStandards))
	for i := range knownStandards {
		cp := knownStandards[i]
		results[i] = &cp
	}

	if maxResults <= 0 || maxResults > 25 {
		maxResults = 25
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*Standard{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) DescribeStandardsControls(
	subscriptionArn, nextToken string,
	maxResults int,
) ([]*StandardsControl, string) {
	b.mu.RLock("DescribeStandardsControls")
	defer b.mu.RUnlock()

	if !b.standardsSubscriptions.Has(subscriptionArn) {
		return []*StandardsControl{}, ""
	}

	// Return a small set of mock controls
	controls := defaultControls(subscriptionArn)

	// Apply overrides
	for i, c := range controls {
		if override, ok := b.controlOverrides.Get(c.StandardsControlArn); ok {
			controls[i] = override
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(controls) {
		return []*StandardsControl{}, ""
	}

	end := start + maxResults
	end = min(end, len(controls))

	page := controls[start:end]
	nextOut := ""

	if end < len(controls) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

// defaultControls returns a minimal set of controls for a subscription.
func defaultControls(subscriptionArn string) []*StandardsControl {
	prefix := subscriptionArn + "/control"

	return []*StandardsControl{
		{
			StandardsControlArn:    prefix + "/1",
			ControlStatus:          statusEnabled,
			ControlID:              "1",
			Title:                  "Avoid the use of the root user",
			Description:            "The root user has unrestricted access to all resources in the AWS account.",
			RemediationURL:         "https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html",
			SeverityRating:         "CRITICAL",
			RelatedRequirements:    []string{"CIS AWS Foundations 1.1"},
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		},
		{
			StandardsControlArn:    prefix + "/2",
			ControlStatus:          statusEnabled,
			ControlID:              "2",
			Title:                  "Ensure MFA is enabled for all IAM users with console password",
			Description:            "Multi-Factor Authentication (MFA) adds an extra layer of protection.",
			RemediationURL:         "https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html",
			SeverityRating:         severityLabelMedium,
			RelatedRequirements:    []string{"CIS AWS Foundations 1.2"},
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}
}

func (b *InMemoryBackend) UpdateStandardsControl(controlArn, controlStatus, disabledReason string) error {
	b.mu.Lock("UpdateStandardsControl")
	defer b.mu.Unlock()

	ctrl, exists := b.controlOverrides.Get(controlArn)
	if !exists {
		// Create from defaults
		ctrl = &StandardsControl{
			StandardsControlArn:    controlArn,
			ControlStatus:          statusEnabled,
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
	}

	ctrl.ControlStatus = controlStatus
	ctrl.DisabledReason = disabledReason
	ctrl.ControlStatusUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.controlOverrides.Put(ctrl)

	return nil
}

func (b *InMemoryBackend) ListStandardsControlAssociations(
	securityControlID, nextToken string,
	maxResults int,
) ([]*StandardsControlAssociation, string) {
	b.mu.RLock("ListStandardsControlAssociations")
	defer b.mu.RUnlock()

	results := make([]*StandardsControlAssociation, 0, len(knownStandards))

	for _, std := range knownStandards {
		assoc := &StandardsControlAssociation{
			SecurityControlID: securityControlID,
			StandardsArn:      std.StandardsArn,
			AssociationStatus: statusEnabled,
			UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		}
		results = append(results, assoc)
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*StandardsControlAssociation{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) BatchGetStandardsControlAssociations(
	requests []map[string]any,
) ([]*StandardsControlAssociation, []map[string]any) {
	b.mu.RLock("BatchGetStandardsControlAssociations")
	defer b.mu.RUnlock()

	var associations []*StandardsControlAssociation
	var unprocessed []map[string]any

	for _, req := range requests {
		secCtlID, _ := req[keySecurityControlID].(string)
		stdArn, _ := req[keyStandardsArn].(string)

		if secCtlID == "" || stdArn == "" {
			unprocessed = append(unprocessed, req)

			continue
		}

		assoc := &StandardsControlAssociation{
			SecurityControlID: secCtlID,
			StandardsArn:      stdArn,
			AssociationStatus: statusEnabled,
			UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		}

		overrideKey := controlAssocOverrideKey(stdArn, secCtlID)
		if override, hasOverride := b.controlAssocOverrides.Get(overrideKey); hasOverride {
			assoc = override
		}

		associations = append(associations, assoc)
	}

	if associations == nil {
		associations = []*StandardsControlAssociation{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return associations, unprocessed
}

func (b *InMemoryBackend) BatchUpdateStandardsControlAssociations(updates []map[string]any) ([]map[string]any, error) {
	b.mu.Lock("BatchUpdateStandardsControlAssociations")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, u := range updates {
		if _, hasCtl := u[keySecurityControlID]; !hasCtl {
			unprocessed = append(unprocessed, u)

			continue
		}

		status, _ := u["AssociationStatus"].(string)
		reason, _ := u["UpdatedReason"].(string)
		secCtlID, _ := u[keySecurityControlID].(string)
		stdArn, _ := u[keyStandardsArn].(string)

		b.controlAssocOverrides.Put(&StandardsControlAssociation{
			SecurityControlID: secCtlID,
			StandardsArn:      stdArn,
			AssociationStatus: status,
			UpdatedReason:     reason,
			UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		})
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return unprocessed, nil
}
