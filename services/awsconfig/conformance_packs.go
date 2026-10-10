package awsconfig

import (
	"fmt"
	"regexp"
	"slices"
	"time"
)

const (
	conformancePackStateComplete       = "CREATE_COMPLETE"
	conformancePackStateCreating       = "CREATE_IN_PROGRESS"
	conformancePackStateUpdating       = "UPDATE_IN_PROGRESS"
	conformancePackStateUpdateComplete = "UPDATE_COMPLETE"

	maxConformancePackNameLen = 256
)

var conformancePackNameRe = regexp.MustCompile(`^[a-zA-Z][-a-zA-Z0-9]*$`)

// packTransition is a pack's in-flight deployment: until when it reports state.
type packTransition struct {
	state string
	until float64
}

// SetLifecycleDelay sets how long a conformance pack stays in CREATE_IN_PROGRESS or UPDATE_IN_PROGRESS after
// PutConformancePack; the default 0 settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// packStateLocked derives the pack's deployment state from its pending transition. Caller holds b.mu.
func (b *InMemoryBackend) packStateLocked(name string) string {
	tr, ok := b.packTransitions[name]
	if !ok {
		return conformancePackStateComplete
	}

	if epochSeconds(b.now()) < tr.until {
		return tr.state
	}

	if tr.state == conformancePackStateUpdating {
		return conformancePackStateUpdateComplete
	}

	return conformancePackStateComplete
}

func (b *InMemoryBackend) packStatusLocked(p *ConformancePack) ConformancePackStatus {
	st := ConformancePackStatus{
		ConformancePackName:     p.ConformancePackName,
		ConformancePackArn:      p.ConformancePackArn,
		ConformancePackID:       p.ConformancePackID,
		ConformancePackState:    b.packStateLocked(p.ConformancePackName),
		LastUpdateRequestedTime: p.LastUpdateRequestedTime,
	}

	if tr, ok := b.packTransitions[p.ConformancePackName]; ok && st.ConformancePackState != tr.state {
		st.LastUpdateCompletedTime = tr.until
	}

	return st
}

func validateConformancePackName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConformancePackName is required", ErrInvalidParameterValue)
	}

	if len(name) > maxConformancePackNameLen || !conformancePackNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: ConformancePackName must match [a-zA-Z][-a-zA-Z0-9]* and be at most %d characters",
			ErrValidation, maxConformancePackNameLen,
		)
	}

	return nil
}

// PutConformancePack creates or updates a conformance pack from one of TemplateBody, TemplateS3Uri or
// TemplateSSMDocumentDetails (the latter two are resolved to a body by the handler via
// ResolveConformancePackTemplate). Specifying more than one is rejected; none deploys zero rules.
// AWS::Config::ConfigRule resources in the template become config rules linked to the pack, and updating a pack
// replaces its rule set, cascading deletes of rules no longer present.
func (b *InMemoryBackend) PutConformancePack(
	name, deliveryS3Bucket, deliveryS3KeyPrefix, templateBody, templateS3URI, templateSSMDocumentName string,
	tags []Tag,
) error {
	_, err := b.PutConformancePackWithParams(
		name, deliveryS3Bucket, deliveryS3KeyPrefix, templateBody, templateS3URI, templateSSMDocumentName, tags, nil,
	)

	return err
}

// PutConformancePackWithParams is PutConformancePack plus input parameters; it returns the pack ARN.
func (b *InMemoryBackend) PutConformancePackWithParams(
	name, deliveryS3Bucket, deliveryS3KeyPrefix, templateBody, templateS3URI, templateSSMDocumentName string,
	tags []Tag, params []ConformancePackInputParameter,
) (string, error) {
	if err := validateConformancePackName(name); err != nil {
		return "", err
	}

	if err := validateSingleTemplateSource(templateBody, templateS3URI, templateSSMDocumentName); err != nil {
		return "", err
	}

	rules := parseConformancePackConfigRules(templateBody, name, params)

	b.mu.Lock("PutConformancePack")
	defer b.mu.Unlock()

	b.replacePackRulesLocked(name, rules)

	_, exists := b.conformancePacks.Get(name)
	if !exists {
		b.conformancePackCounter++
	}

	packID := fmt.Sprintf("conformance-pack-%08d", b.conformancePackCounter)
	arn := fmt.Sprintf(
		"arn:aws:config:%s:%s:conformance-pack/%s/%s",
		b.region, b.accountID, name, packID,
	)

	b.conformancePacks.Put(&ConformancePack{
		ConformancePackName: name,
		ConformancePackArn:  arn,
		ConformancePackID:   packID,
		DeliveryS3Bucket:    deliveryS3Bucket,
		DeliveryS3KeyPrefix: deliveryS3KeyPrefix,

		ConformancePackInputParameters: slices.Clone(params),
		LastUpdateRequestedTime:        epochSeconds(b.now()),
	})
	b.setResourceTagsLocked(arn, tags)
	b.beginPackTransitionLocked(name, exists)

	return arn, nil
}

func (b *InMemoryBackend) beginPackTransitionLocked(name string, updating bool) {
	delete(b.packTransitions, name)

	if b.lifecycleDelay <= 0 {
		return
	}

	state := conformancePackStateCreating
	if updating {
		state = conformancePackStateUpdating
	}

	b.packTransitions[name] = packTransition{
		state: state,
		until: epochSeconds(b.now().Add(b.lifecycleDelay)),
	}
}

func validateSingleTemplateSource(templateBody, templateS3URI, templateSSMDocumentName string) error {
	sourceCount := 0
	for _, set := range []bool{templateBody != "", templateS3URI != "", templateSSMDocumentName != ""} {
		if set {
			sourceCount++
		}
	}

	if sourceCount > 1 {
		return fmt.Errorf(
			"%w: specify only one of TemplateBody, TemplateS3Uri, or TemplateSSMDocumentDetails",
			ErrInvalidParameterValue,
		)
	}

	return nil
}

// replacePackRulesLocked registers newRules as packName's deployed config
// rules, deleting (cascade: config rule + its evaluations + its link entry)
// any previously-linked rule that is absent from newRules. Caller must already
// hold the write lock.
func (b *InMemoryBackend) replacePackRulesLocked(packName string, newRules []*ConfigRule) {
	newNames := make(map[string]struct{}, len(newRules))
	for _, r := range newRules {
		newNames[r.ConfigRuleName] = struct{}{}
	}

	for _, link := range slices.Clone(b.conformancePackRulesByPack.Get(packName)) {
		if _, keep := newNames[link.ConfigRuleName]; keep {
			continue
		}

		b.configRules.Delete(link.ConfigRuleName)
		b.clearRuleEvaluationsLocked(link.ConfigRuleName)
		b.conformancePackRules.Delete(conformancePackRuleLinkKeyFn(link))
	}

	for _, r := range newRules {
		b.putConfigRuleLocked(r)
		b.conformancePackRules.Put(&ConformancePackRuleLink{
			ConformancePackName: packName,
			ConfigRuleName:      r.ConfigRuleName,
		})
	}
}

// DeleteConformancePack deletes a conformance pack by name, cascade-deleting
// every config rule it deployed (and their evaluations) along with it --
// matching real AWS Config, where deleting a conformance pack removes the
// managed rules it created.
func (b *InMemoryBackend) DeleteConformancePack(name string) error {
	if name == "" {
		// Declared set is NoSuchConformancePackException/ResourceInUseException only --
		// no validation-shaped code fits an empty name (configservice@v1.68.4 deserializers.go).
		return fmt.Errorf("%w: ConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConformancePack")
	defer b.mu.Unlock()

	if !b.conformancePacks.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchConformancePack, name)
	}

	b.replacePackRulesLocked(name, nil)
	b.conformancePacks.Delete(name)
	delete(b.packTransitions, name)

	return nil
}

// DescribeConformancePacks returns all conformance packs.
func (b *InMemoryBackend) DescribeConformancePacks() []ConformancePack {
	b.mu.RLock("DescribeConformancePacks")
	defer b.mu.RUnlock()

	all := b.conformancePacks.All()
	out := make([]ConformancePack, 0, len(all))

	for _, p := range all {
		out = append(out, *p)
	}

	return out
}

// DescribeConformancePackStatus returns conformance pack statuses.
// If names is empty, all packs are returned.
func (b *InMemoryBackend) DescribeConformancePackStatus(names []string) []ConformancePackStatus {
	b.mu.RLock("DescribeConformancePackStatus")
	defer b.mu.RUnlock()

	var packs []*ConformancePack

	if len(names) == 0 {
		packs = b.conformancePacks.All()
	} else {
		for _, name := range names {
			if p, ok := b.conformancePacks.Get(name); ok {
				packs = append(packs, p)
			}
		}
	}

	out := make([]ConformancePackStatus, 0, len(packs))
	for _, p := range packs {
		out = append(out, b.packStatusLocked(p))
	}

	return out
}

func epochSeconds(t time.Time) float64 { return float64(t.UnixNano()) / float64(time.Second) }
