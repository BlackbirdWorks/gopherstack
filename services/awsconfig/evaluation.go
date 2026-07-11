package awsconfig

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Compliance type constants used by the evaluation engine.
const (
	complianceCompliant        = "COMPLIANT"
	complianceNonCompliant     = "NON_COMPLIANT"
	complianceNotApplicable    = "NOT_APPLICABLE"
	complianceInsufficientData = "INSUFFICIENT_DATA"
)

// Config rule owner constants.
const (
	ruleOwnerAWS          = "AWS"
	ruleOwnerCustomLambda = "CUSTOM_LAMBDA"
	ruleOwnerCustomPolicy = "CUSTOM_POLICY"
)

// resourceTypeS3Bucket is the AWS Config resource type for S3 buckets.
const resourceTypeS3Bucket = "AWS::S3::Bucket"

// StoredEvaluation is a single per-(rule, resource) evaluation outcome. It is
// purely internal bookkeeping -- never serialized directly to an AWS API
// response (DetailedEvaluationResult is the wire type built from it) -- so
// RuleName was added here (Phase 3.3 store.Table conversion) purely to give
// the flattened ruleResourceEvals table a composite primary key
// ("<ruleName>|<resourceType>\x1f<resourceID>", see store_setup.go's
// storedEvaluationKeyFn) without affecting any wire shape.
type StoredEvaluation struct {
	ComplianceType     string
	ResourceType       string
	ResourceID         string
	Annotation         string
	RuleName           string
	OrderingTimestamp  float64
	ResultRecordedTime float64
}

// ResourceEvaluation records a StartResourceEvaluation run so the Get/List
// operations can read it back.
type ResourceEvaluation struct {
	ResourceEvaluationID string  `json:"ResourceEvaluationId"`
	ResourceType         string  `json:"ResourceType"`
	ResourceID           string  `json:"ResourceId"`
	EvaluationMode       string  `json:"EvaluationMode"`
	Status               string  `json:"Status"`
	Compliance           string  `json:"Compliance,omitempty"`
	Configuration        string  `json:"-"`
	StartTime            float64 `json:"EvaluationStartTimestamp"`
}

// managedRuleEvaluator models a single AWS managed config rule's logic.
type managedRuleEvaluator struct {
	// evaluate returns the compliance type and an optional annotation for a
	// single resource configuration item. An empty compliance type means the
	// resource is not in scope and should be skipped.
	evaluate func(item *ResourceConfigItem, params map[string]string) (compliance string, annotation string)
	// resourceTypes are the resource types this rule applies to. When empty the
	// rule is evaluated against every discovered resource type (e.g. REQUIRED_TAGS).
	resourceTypes []string
}

// lookupManagedEvaluator returns the evaluator for an AWS managed rule source
// identifier. Rules without an entry are treated honestly as INSUFFICIENT_DATA
// rather than being blanket-marked COMPLIANT. Implemented as a function (not a
// package-level map) to avoid mutable global state.
func lookupManagedEvaluator(sourceID string) (managedRuleEvaluator, bool) {
	switch sourceID {
	case "REQUIRED_TAGS":
		return managedRuleEvaluator{evaluate: evalRequiredTags}, true
	case "S3_BUCKET_VERSIONING_ENABLED":
		return managedRuleEvaluator{resourceTypes: []string{resourceTypeS3Bucket}, evaluate: evalS3Versioning}, true
	case "S3_BUCKET_PUBLIC_READ_PROHIBITED":
		return managedRuleEvaluator{resourceTypes: []string{resourceTypeS3Bucket}, evaluate: evalS3PublicRead}, true
	case "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED":
		return managedRuleEvaluator{resourceTypes: []string{resourceTypeS3Bucket}, evaluate: evalS3Encryption}, true
	case "ENCRYPTED_VOLUMES":
		return managedRuleEvaluator{resourceTypes: []string{"AWS::EC2::Volume"}, evaluate: evalEncryptedVolumes}, true
	case "EC2_INSTANCE_NO_PUBLIC_IP":
		return managedRuleEvaluator{resourceTypes: []string{"AWS::EC2::Instance"}, evaluate: evalEC2NoPublicIP}, true
	default:
		return managedRuleEvaluator{}, false
	}
}

// resourceEvalKey builds the composite key used to store per-resource evaluations.
func resourceEvalKey(resourceType, resourceID string) string {
	return resourceType + "\x1f" + resourceID
}

// parseConfiguration parses a resource configuration JSON string into a generic map.
func parseConfiguration(configuration string) map[string]any {
	if configuration == "" {
		return map[string]any{}
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(configuration), &m); err != nil {
		return map[string]any{}
	}

	return m
}

// parseInputParameters parses a config rule's InputParameters JSON into a
// string→string map. Non-string values are stringified.
func parseInputParameters(input string) map[string]string {
	out := map[string]string{}
	if input == "" {
		return out
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(input), &raw); err != nil {
		return out
	}

	for k, v := range raw {
		switch tv := v.(type) {
		case string:
			out[k] = tv
		default:
			b, _ := json.Marshal(tv)
			out[k] = string(b)
		}
	}

	return out
}

// parseResourceTags extracts tags from a resource configuration, supporting both
// the array-of-{Key,Value} form and the flat map form under "Tags"/"tags".
func parseResourceTags(configuration string) map[string]string {
	cfg := parseConfiguration(configuration)
	tags := map[string]string{}

	raw, ok := cfg["Tags"]
	if !ok {
		raw, ok = cfg["tags"]
	}

	if !ok {
		return tags
	}

	switch tv := raw.(type) {
	case []any:
		for _, e := range tv {
			m, isMap := e.(map[string]any)
			if !isMap {
				continue
			}

			key, _ := stringField(m, "Key", "key")
			val, _ := stringField(m, "Value", "value")

			if key != "" {
				tags[key] = val
			}
		}
	case map[string]any:
		for k, v := range tv {
			if s, isStr := v.(string); isStr {
				tags[k] = s
			}
		}
	}

	return tags
}

// stringField returns the first present string field from cfg among the given keys.
func stringField(cfg map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		if v, ok := cfg[k]; ok {
			if s, isStr := v.(string); isStr {
				return s, true
			}
		}
	}

	return "", false
}

// boolField returns the bool value of the first present field among keys.
func boolField(cfg map[string]any, keys ...string) (bool, bool) {
	for _, k := range keys {
		if v, ok := cfg[k]; ok {
			if b, isBool := v.(bool); isBool {
				return b, true
			}
		}
	}

	return false, false
}

// valueAllowed reports whether val is contained in a comma-separated allow list.
func valueAllowed(val, allowList string) bool {
	for allowed := range strings.SplitSeq(allowList, ",") {
		if strings.TrimSpace(allowed) == val {
			return true
		}
	}

	return false
}

// --- Managed rule evaluators ---

func evalRequiredTags(item *ResourceConfigItem, params map[string]string) (string, string) {
	tags := parseResourceTags(item.Configuration)

	var missing []string

	for i := 1; i <= 6; i++ {
		keyParam := params[fmt.Sprintf("tag%dKey", i)]
		if keyParam == "" {
			continue
		}

		val, ok := tags[keyParam]
		if !ok {
			missing = append(missing, keyParam)

			continue
		}

		if valParam := params[fmt.Sprintf("tag%dValue", i)]; valParam != "" && !valueAllowed(val, valParam) {
			missing = append(missing, keyParam)
		}
	}

	if len(missing) > 0 {
		return complianceNonCompliant, "Missing or invalid required tags: " + strings.Join(missing, ", ")
	}

	return complianceCompliant, "All required tags present"
}

func evalS3Versioning(item *ResourceConfigItem, _ map[string]string) (string, string) {
	cfg := parseConfiguration(item.Configuration)

	if vc, ok := cfg["VersioningConfiguration"].(map[string]any); ok {
		if status, _ := stringField(vc, "Status"); status == "Enabled" {
			return complianceCompliant, "Versioning enabled"
		}

		return complianceNonCompliant, "Versioning not enabled"
	}

	if enabled, ok := boolField(cfg, "VersioningEnabled", "versioning"); ok {
		if enabled {
			return complianceCompliant, "Versioning enabled"
		}

		return complianceNonCompliant, "Versioning not enabled"
	}

	return complianceNonCompliant, "Versioning configuration missing"
}

func evalS3PublicRead(item *ResourceConfigItem, _ map[string]string) (string, string) {
	cfg := parseConfiguration(item.Configuration)

	if acl, ok := stringField(cfg, "Acl", "acl"); ok {
		if acl == "public-read" || acl == "public-read-write" {
			return complianceNonCompliant, "Bucket ACL grants public read"
		}
	}

	if public, ok := boolField(cfg, "PublicRead", "Public", "publicRead"); ok && public {
		return complianceNonCompliant, "Bucket allows public read"
	}

	if pab, ok := cfg["PublicAccessBlockConfiguration"].(map[string]any); ok {
		if blocked, _ := boolField(pab, "BlockPublicAcls"); !blocked {
			return complianceNonCompliant, "Public ACLs are not blocked"
		}
	}

	return complianceCompliant, "Bucket does not allow public read"
}

func evalS3Encryption(item *ResourceConfigItem, _ map[string]string) (string, string) {
	cfg := parseConfiguration(item.Configuration)

	for _, key := range []string{"ServerSideEncryptionConfiguration", "BucketEncryption"} {
		if v, ok := cfg[key]; ok && v != nil {
			if m, isMap := v.(map[string]any); !isMap || len(m) > 0 {
				return complianceCompliant, "Server-side encryption enabled"
			}
		}
	}

	if enabled, ok := boolField(cfg, "Encryption", "encrypted"); ok && enabled {
		return complianceCompliant, "Server-side encryption enabled"
	}

	return complianceNonCompliant, "Server-side encryption not configured"
}

func evalEncryptedVolumes(item *ResourceConfigItem, _ map[string]string) (string, string) {
	cfg := parseConfiguration(item.Configuration)

	if encrypted, ok := boolField(cfg, "Encrypted", "encrypted"); ok && encrypted {
		return complianceCompliant, "Volume is encrypted"
	}

	return complianceNonCompliant, "Volume is not encrypted"
}

func evalEC2NoPublicIP(item *ResourceConfigItem, _ map[string]string) (string, string) {
	cfg := parseConfiguration(item.Configuration)

	if ip, ok := stringField(cfg, "PublicIpAddress", "publicIpAddress"); ok && ip != "" {
		return complianceNonCompliant, "Instance has a public IP: " + ip
	}

	return complianceCompliant, "Instance has no public IP"
}

// scopeMatches reports whether a resource item falls within a config rule scope.
func scopeMatches(scope *ConfigRuleScope, item *ResourceConfigItem) bool {
	if scope == nil {
		return true
	}

	if scope.ComplianceResourceID != "" && scope.ComplianceResourceID != item.ResourceID {
		return false
	}

	if scope.TagKey != "" {
		tags := parseResourceTags(item.Configuration)

		val, ok := tags[scope.TagKey]
		if !ok {
			return false
		}

		if scope.TagValue != "" && scope.TagValue != val {
			return false
		}
	}

	return true
}

// --- Evaluation engine (all helpers below assume the write lock is held) ---

// evaluateRuleLocked runs the appropriate evaluation strategy for a single rule.
func (b *InMemoryBackend) evaluateRuleLocked(rule *ConfigRule, now float64) {
	owner := ""
	sourceID := ""

	if rule.Source != nil {
		owner = rule.Source.Owner
		sourceID = rule.Source.SourceIdentifier
	}

	switch owner {
	case ruleOwnerAWS:
		evaluator, ok := lookupManagedEvaluator(sourceID)
		if !ok {
			// Managed rule we cannot model: report INSUFFICIENT_DATA honestly.
			b.ruleEvaluations[rule.ConfigRuleName] = complianceInsufficientData

			return
		}

		b.evaluateManagedRuleLocked(rule, evaluator, now)
	case ruleOwnerCustomLambda, ruleOwnerCustomPolicy:
		// Custom/Lambda rules are evaluated out-of-band; their results arrive via
		// PutEvaluations. We do not fabricate compliance — just roll up what exists.
		b.recomputeRollupLocked(rule.ConfigRuleName)
	default:
		b.ruleEvaluations[rule.ConfigRuleName] = complianceInsufficientData
	}
}

// distinctResourceTypesLocked returns every distinct ResourceType currently
// present in b.resourceConfigs. store.Index has no key-enumeration method (by
// design -- see pkgs/store/index.go), so unlike the old
// map[string]map[string]*ResourceConfigItem this walks the flat table once
// and dedupes via ResourceConfigItem's own ResourceType field instead of
// ranging over outer map keys.
func (b *InMemoryBackend) distinctResourceTypesLocked() []string {
	seen := make(map[string]struct{})

	for _, item := range b.resourceConfigs.All() {
		seen[item.ResourceType] = struct{}{}
	}

	types := make([]string, 0, len(seen))
	for rt := range seen {
		types = append(types, rt)
	}

	return types
}

// evaluateManagedRuleLocked evaluates a modelable managed rule against stored
// resource configuration items and records per-resource results.
func (b *InMemoryBackend) evaluateManagedRuleLocked(rule *ConfigRule, evaluator managedRuleEvaluator, now float64) {
	params := parseInputParameters(rule.InputParameters)

	types := evaluator.resourceTypes
	if rule.Scope != nil && len(rule.Scope.ComplianceResourceTypes) > 0 {
		types = rule.Scope.ComplianceResourceTypes
	}

	if len(types) == 0 {
		// Rule applies to all resource types (e.g. REQUIRED_TAGS without scope).
		types = b.distinctResourceTypesLocked()
	}

	// The prior map assignment (b.ruleResourceEvals[rule.ConfigRuleName] =
	// evals) atomically replaced the rule's whole evaluation set. The
	// flattened table has no equivalent bulk-replace, so drop this rule's
	// existing entries first (snapshot via slices.Clone since Delete mutates
	// the very index slice AddIndex/Get returns) then insert the fresh set.
	for _, old := range slices.Clone(b.ruleResourceEvalsByRule.Get(rule.ConfigRuleName)) {
		b.ruleResourceEvals.Delete(storedEvaluationKeyFn(old))
	}

	for _, rt := range types {
		for _, item := range b.resourceConfigsByType.Get(rt) {
			if !scopeMatches(rule.Scope, item) {
				continue
			}

			compliance, annotation := evaluator.evaluate(item, params)
			if compliance == "" {
				continue
			}

			b.ruleResourceEvals.Put(&StoredEvaluation{
				ComplianceType:     compliance,
				ResourceType:       rt,
				ResourceID:         item.ResourceID,
				Annotation:         annotation,
				RuleName:           rule.ConfigRuleName,
				OrderingTimestamp:  now,
				ResultRecordedTime: now,
			})
		}
	}

	b.recomputeRollupLocked(rule.ConfigRuleName)
}

// recomputeRollupLocked recomputes the rule-level rollup compliance from the
// stored per-resource evaluations.
func (b *InMemoryBackend) recomputeRollupLocked(ruleName string) {
	b.ruleEvaluations[ruleName] = rollupCompliance(b.ruleResourceEvalsByRule.Get(ruleName))
}

// rollupCompliance derives a single rule-level compliance type from per-resource
// evaluations: any NON_COMPLIANT wins, then COMPLIANT, else INSUFFICIENT_DATA.
func rollupCompliance(evals []*StoredEvaluation) string {
	if len(evals) == 0 {
		return complianceInsufficientData
	}

	hasCompliant := false

	for _, e := range evals {
		switch e.ComplianceType {
		case complianceNonCompliant:
			return complianceNonCompliant
		case complianceCompliant:
			hasCompliant = true
		}
	}

	if hasCompliant {
		return complianceCompliant
	}

	return complianceInsufficientData
}

// recordEvaluationLocked stores a single per-resource evaluation and refreshes
// the rule rollup.
func (b *InMemoryBackend) recordEvaluationLocked(
	ruleName, resourceType, resourceID, compliance, annotation string,
	now float64,
) {
	b.ruleResourceEvals.Put(&StoredEvaluation{
		ComplianceType:     compliance,
		ResourceType:       resourceType,
		ResourceID:         resourceID,
		Annotation:         annotation,
		RuleName:           ruleName,
		OrderingTimestamp:  now,
		ResultRecordedTime: now,
	})

	b.recomputeRollupLocked(ruleName)
}

// StartConfigRulesEvaluation evaluates every config rule against current state.
func (b *InMemoryBackend) StartConfigRulesEvaluation() error {
	return b.StartConfigRulesEvaluationFor(nil)
}

// StartConfigRulesEvaluationFor evaluates the named config rules (all when empty).
// Managed rules with modelable logic are evaluated against stored resource config
// items; custom rules keep whatever PutEvaluations reported; unmodelable managed
// rules are reported as INSUFFICIENT_DATA rather than blanket COMPLIANT.
func (b *InMemoryBackend) StartConfigRulesEvaluationFor(names []string) error {
	b.mu.Lock("StartConfigRulesEvaluation")
	defer b.mu.Unlock()

	targets := names
	if len(targets) == 0 {
		all := b.configRules.All()
		targets = make([]string, 0, len(all))

		for _, rule := range all {
			targets = append(targets, rule.ConfigRuleName)
		}
	}

	now := float64(time.Now().Unix())

	for _, name := range targets {
		rule, ok := b.configRules.Get(name)
		if !ok {
			continue
		}

		b.evaluateRuleLocked(rule, now)
	}

	return nil
}

// buildDetailedResults converts a per-resource evaluation slice into sorted,
// compliance-filtered detailed evaluation results.
func buildDetailedResults(
	ruleName string,
	evals []*StoredEvaluation,
	complianceTypes []string,
) []DetailedEvaluationResult {
	out := make([]DetailedEvaluationResult, 0, len(evals))

	for _, e := range evals {
		if len(complianceTypes) > 0 && !slices.Contains(complianceTypes, e.ComplianceType) {
			continue
		}

		out = append(out, DetailedEvaluationResult{
			EvaluationResultIdentifier: EvaluationResultIdentifier{
				EvaluationResultQualifier: EvaluationResultQualifier{
					ConfigRuleName: ruleName,
					ResourceType:   e.ResourceType,
					ResourceID:     e.ResourceID,
				},
				OrderingTimestamp: e.OrderingTimestamp,
			},
			ComplianceType:        e.ComplianceType,
			ResultRecordedTime:    e.ResultRecordedTime,
			ConfigRuleInvokedTime: e.OrderingTimestamp,
			Annotation:            e.Annotation,
		})
	}

	slices.SortFunc(out, func(a, c DetailedEvaluationResult) int {
		aq := a.EvaluationResultIdentifier.EvaluationResultQualifier
		cq := c.EvaluationResultIdentifier.EvaluationResultQualifier

		if aq.ResourceType != cq.ResourceType {
			return strings.Compare(aq.ResourceType, cq.ResourceType)
		}

		return strings.Compare(aq.ResourceID, cq.ResourceID)
	})

	return out
}

// GetComplianceDetailsByConfigRule returns per-resource evaluation results for a
// config rule, optionally filtered to the given compliance types.
func (b *InMemoryBackend) GetComplianceDetailsByConfigRule(
	ruleName string,
	complianceTypes []string,
) []DetailedEvaluationResult {
	b.mu.RLock("GetComplianceDetailsByConfigRule")
	defer b.mu.RUnlock()

	return buildDetailedResults(ruleName, b.ruleResourceEvalsByRule.Get(ruleName), complianceTypes)
}

// GetComplianceDetailsByResource returns per-rule evaluation results recorded for
// a single resource, optionally filtered to the given compliance types.
func (b *InMemoryBackend) GetComplianceDetailsByResource(
	resourceType, resourceID string,
	complianceTypes []string,
) []DetailedEvaluationResult {
	b.mu.RLock("GetComplianceDetailsByResource")
	defer b.mu.RUnlock()

	key := resourceEvalKey(resourceType, resourceID)
	out := make([]DetailedEvaluationResult, 0)

	for _, e := range b.ruleResourceEvalsByResource.Get(key) {
		if len(complianceTypes) > 0 && !slices.Contains(complianceTypes, e.ComplianceType) {
			continue
		}

		out = append(out, DetailedEvaluationResult{
			EvaluationResultIdentifier: EvaluationResultIdentifier{
				EvaluationResultQualifier: EvaluationResultQualifier{
					ConfigRuleName: e.RuleName,
					ResourceType:   e.ResourceType,
					ResourceID:     e.ResourceID,
				},
				OrderingTimestamp: e.OrderingTimestamp,
			},
			ComplianceType:        e.ComplianceType,
			ResultRecordedTime:    e.ResultRecordedTime,
			ConfigRuleInvokedTime: e.OrderingTimestamp,
			Annotation:            e.Annotation,
		})
	}

	slices.SortFunc(out, func(a, c DetailedEvaluationResult) int {
		return strings.Compare(
			a.EvaluationResultIdentifier.EvaluationResultQualifier.ConfigRuleName,
			c.EvaluationResultIdentifier.EvaluationResultQualifier.ConfigRuleName,
		)
	})

	return out
}

// StartResourceEvaluation records a resource evaluation run and returns its
// unique identifier.
func (b *InMemoryBackend) StartResourceEvaluation(
	resourceType, resourceID, evaluationMode, configuration string,
) string {
	b.mu.Lock("StartResourceEvaluation")
	defer b.mu.Unlock()

	b.resourceEvalCounter++

	id := fmt.Sprintf(
		"%s-%08d-%d",
		b.accountID, b.resourceEvalCounter, time.Now().UnixNano(),
	)

	if evaluationMode == "" {
		evaluationMode = "DETAILED"
	}

	compliance := ""
	if configuration != "" {
		compliance = complianceCompliant
	}

	b.resourceEvaluations.Put(&ResourceEvaluation{
		ResourceEvaluationID: id,
		ResourceType:         resourceType,
		ResourceID:           resourceID,
		EvaluationMode:       evaluationMode,
		Status:               "SUCCEEDED",
		Compliance:           compliance,
		Configuration:        configuration,
		StartTime:            float64(time.Now().Unix()),
	})

	return id
}

// GetResourceEvaluationSummaryByID returns the recorded resource evaluation, or
// nil when the id is unknown.
func (b *InMemoryBackend) GetResourceEvaluationSummaryByID(id string) *ResourceEvaluation {
	b.mu.RLock("GetResourceEvaluationSummaryByID")
	defer b.mu.RUnlock()

	re, ok := b.resourceEvaluations.Get(id)
	if !ok {
		return nil
	}

	cp := *re

	return &cp
}

// ListResourceEvaluationSummaries returns all recorded resource evaluations,
// most-recent first.
func (b *InMemoryBackend) ListResourceEvaluationSummaries() []ResourceEvaluation {
	b.mu.RLock("ListResourceEvaluationSummaries")
	defer b.mu.RUnlock()

	all := b.resourceEvaluations.All()
	out := make([]ResourceEvaluation, 0, len(all))

	for _, re := range all {
		out = append(out, *re)
	}

	slices.SortFunc(out, func(a, c ResourceEvaluation) int {
		if a.StartTime != c.StartTime {
			if a.StartTime > c.StartTime {
				return -1
			}

			return 1
		}

		return strings.Compare(c.ResourceEvaluationID, a.ResourceEvaluationID)
	})

	return out
}
