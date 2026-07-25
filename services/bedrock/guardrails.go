package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newGuardrailID generates a unique guardrail ID.
func (b *InMemoryBackend) newGuardrailID() string {
	b.guardrailCounter++

	return fmt.Sprintf("bedrock-guardrail-%07d", b.guardrailCounter)
}

// CreateGuardrail creates a new guardrail. The optional policies argument configures
// content, topic, word, sensitive-information, and contextual-grounding policies.
func (b *InMemoryBackend) CreateGuardrail(
	name, description, blockedInput, blockedOutput string,
	tags []Tag,
	policies ...*GuardrailPolicies,
) (*Guardrail, error) {
	b.mu.Lock("CreateGuardrail")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, exists := b.guardrailsByName[name]; exists {
		return nil, fmt.Errorf("%w: guardrail %s already exists", ErrAlreadyExists, name)
	}

	id := b.newGuardrailID()
	guardrailARN := arn.Build("bedrock", b.region, b.accountID, "guardrail/"+id)
	now := time.Now().UTC()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	var pol *GuardrailPolicies
	if len(policies) > 0 {
		pol = copyGuardrailPolicies(policies[0])
	}

	g := &Guardrail{
		GuardrailID:             id,
		GuardrailArn:            guardrailARN,
		Name:                    name,
		Description:             description,
		Status:                  "READY",
		Version:                 agentStatusDraft,
		BlockedInputMessaging:   blockedInput,
		BlockedOutputsMessaging: blockedOutput,
		Tags:                    tagsCopy,
		Policies:                pol,
		CreatedAt:               now,
		UpdatedAt:               now,
	}
	b.guardrails.Put(g)
	b.guardrailsByName[name] = id
	b.guardrailsByARN[guardrailARN] = id
	cp := *g
	cp.Tags = copyTags(g.Tags)
	cp.Policies = copyGuardrailPolicies(g.Policies)

	return &cp, nil
}

func (b *InMemoryBackend) GetGuardrail(idOrARN string) (*Guardrail, error) {
	b.mu.RLock("GetGuardrail")
	defer b.mu.RUnlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	cp := *g
	cp.Tags = copyTags(g.Tags)
	cp.Policies = copyGuardrailPolicies(g.Policies)

	return &cp, nil
}

// ListGuardrails returns guardrails with optional pagination.
// If guardrailIdentifier is non-empty, results are filtered to guardrails whose
// ID, ARN, or name equals the identifier (case-sensitive).
func (b *InMemoryBackend) ListGuardrails(
	nextToken, guardrailIdentifier string,
) ([]*GuardrailSummary, string) {
	b.mu.RLock("ListGuardrails")
	defer b.mu.RUnlock()

	list := make([]*GuardrailSummary, 0, b.guardrails.Len())

	for _, g := range b.guardrails.All() {
		if guardrailIdentifier != "" &&
			g.GuardrailID != guardrailIdentifier &&
			g.GuardrailArn != guardrailIdentifier &&
			g.Name != guardrailIdentifier {
			continue
		}

		list = append(list, &GuardrailSummary{
			GuardrailID: g.GuardrailID,
			Arn:         g.GuardrailArn,
			Name:        g.Name,
			Description: g.Description,
			Status:      g.Status,
			Version:     g.Version,
			CreatedAt:   g.CreatedAt,
			UpdatedAt:   g.UpdatedAt,
		})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].GuardrailID < list[j].GuardrailID })

	return paginateBedrockSlice(list, nextToken)
}

// UpdateGuardrail updates a guardrail's name, description, messaging, and policies.
// UpdateGuardrail always mutates the DRAFT version; numbered versions created via
// CreateGuardrailVersion are immutable snapshots and are unaffected (AWS imposes no
// restriction on editing DRAFT after versions have been published).
// The optional policies argument replaces all existing policy configs when provided.
func (b *InMemoryBackend) UpdateGuardrail(
	idOrARN, name, description, blockedInput, blockedOutput string,
	policies ...*GuardrailPolicies,
) (*Guardrail, error) {
	b.mu.Lock("UpdateGuardrail")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	// Update name with index maintenance.
	if name != "" && name != g.Name {
		if _, exists := b.guardrailsByName[name]; exists {
			return nil, fmt.Errorf("%w: guardrail name %s already in use", ErrAlreadyExists, name)
		}

		delete(b.guardrailsByName, g.Name)
		b.guardrailsByName[name] = g.GuardrailID
		g.Name = name
	}

	if description != "" {
		g.Description = description
	}

	if blockedInput != "" {
		g.BlockedInputMessaging = blockedInput
	}

	if blockedOutput != "" {
		g.BlockedOutputsMessaging = blockedOutput
	}

	if len(policies) > 0 {
		g.Policies = copyGuardrailPolicies(policies[0])
	}

	g.UpdatedAt = time.Now().UTC()
	cp := *g
	cp.Tags = copyTags(g.Tags)

	return &cp, nil
}

// DeleteGuardrail removes a guardrail by ID or ARN. If version is empty, the DRAFT and
// every numbered version are deleted. If version is a specific numbered version, only
// that version's snapshot is deleted and the DRAFT (and other versions) are untouched.
func (b *InMemoryBackend) DeleteGuardrail(idOrARN, version string) error {
	b.mu.Lock("DeleteGuardrail")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	if version != "" && version != agentStatusDraft {
		if !b.guardrailVersions.Delete(g.GuardrailID + ":" + version) {
			return fmt.Errorf("%w: guardrail %s version %s not found", ErrNotFound, idOrARN, version)
		}

		return nil
	}

	b.guardrails.Delete(g.GuardrailID)
	delete(b.guardrailsByName, g.Name)
	delete(b.guardrailsByARN, g.GuardrailArn)

	b.guardrailVersions.Range(func(v *GuardrailVersion) bool {
		if v.GuardrailID == g.GuardrailID {
			b.guardrailVersions.Delete(v.GuardrailID + ":" + v.Version)
		}

		return true
	})

	return nil
}

// findGuardrailByIDOrARN finds a guardrail by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findGuardrailByIDOrARN(idOrARN string) (*Guardrail, bool) {
	if g, ok := b.guardrails.Get(idOrARN); ok {
		return g, true
	}

	if id, ok := b.guardrailsByARN[idOrARN]; ok {
		return b.guardrails.Get(id)
	}

	return nil, false
}

// copyGuardrailPolicies returns a deep copy of a GuardrailPolicies struct, or nil if src is nil.
func copyGuardrailPolicies(src *GuardrailPolicies) *GuardrailPolicies {
	if src == nil {
		return nil
	}

	dst := &GuardrailPolicies{}

	if src.ContentPolicy != nil {
		filters := make([]GuardrailContentFilter, len(src.ContentPolicy.FiltersConfig))
		copy(filters, src.ContentPolicy.FiltersConfig)
		dst.ContentPolicy = &GuardrailContentPolicyConfig{FiltersConfig: filters}
	}

	if src.TopicPolicy != nil {
		topics := make([]GuardrailTopic, len(src.TopicPolicy.TopicsConfig))
		for i, t := range src.TopicPolicy.TopicsConfig {
			tc := t
			if len(t.Examples) > 0 {
				tc.Examples = append([]string(nil), t.Examples...)
			}
			topics[i] = tc
		}
		dst.TopicPolicy = &GuardrailTopicPolicyConfig{TopicsConfig: topics}
	}

	if src.WordPolicy != nil {
		wp := &GuardrailWordPolicyConfig{}
		wp.WordsConfig = append([]GuardrailWordConfig(nil), src.WordPolicy.WordsConfig...)
		wp.ManagedWordListsConfig = append(
			[]GuardrailManagedWordList(nil),
			src.WordPolicy.ManagedWordListsConfig...,
		)
		dst.WordPolicy = wp
	}

	if src.SensitiveInformationPolicy != nil {
		sip := &GuardrailSensitiveInformationPolicyConfig{}
		sip.PiiEntitiesConfig = append(
			[]GuardrailPIIEntity(nil),
			src.SensitiveInformationPolicy.PiiEntitiesConfig...,
		)
		sip.RegexesConfig = append(
			[]GuardrailRegexConfig(nil),
			src.SensitiveInformationPolicy.RegexesConfig...,
		)
		dst.SensitiveInformationPolicy = sip
	}

	if src.ContextualGroundingPolicy != nil {
		filters := make(
			[]GuardrailContextualGroundingFilter,
			len(src.ContextualGroundingPolicy.FiltersConfig),
		)
		copy(filters, src.ContextualGroundingPolicy.FiltersConfig)
		dst.ContextualGroundingPolicy = &GuardrailContextualGroundingPolicyConfig{
			FiltersConfig: filters,
		}
	}

	return dst
}

// CreateGuardrailVersion creates a new numbered version snapshot of a guardrail.
// Each guardrail maintains its own monotonically increasing version counter.
func (b *InMemoryBackend) CreateGuardrailVersion(
	idOrARN, description string,
) (*GuardrailVersion, error) {
	b.mu.Lock("CreateGuardrailVersion")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	// Use per-guardrail version counter for isolated, predictable numbering.
	g.versionCounter++
	versionNum := strconv.Itoa(g.versionCounter)

	// AWS freezes the DRAFT's current configuration into the numbered version at
	// creation time; the version stays immutable even as DRAFT continues to change.
	gv := &GuardrailVersion{
		GuardrailID:             g.GuardrailID,
		GuardrailArn:            g.GuardrailArn,
		Version:                 versionNum,
		Name:                    g.Name,
		Description:             description,
		BlockedInputMessaging:   g.BlockedInputMessaging,
		BlockedOutputsMessaging: g.BlockedOutputsMessaging,
		Policies:                copyGuardrailPolicies(g.Policies),
		Tags:                    copyTags(g.Tags),
		CreatedAt:               time.Now().UTC(),
	}

	b.guardrailVersions.Put(gv)

	return gv, nil
}

// GetGuardrailVersion returns guardrail details for a specific version. An empty or
// "DRAFT" version returns the current (mutable) draft. A numbered version returns the
// immutable snapshot captured when that version was published via CreateGuardrailVersion.
func (b *InMemoryBackend) GetGuardrailVersion(idOrARN, version string) (*Guardrail, error) {
	if version == "" || version == agentStatusDraft {
		return b.GetGuardrail(idOrARN)
	}

	b.mu.RLock("GetGuardrailVersion")
	defer b.mu.RUnlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	gv, ok := b.guardrailVersions.Get(g.GuardrailID + ":" + version)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s version %s not found", ErrNotFound, idOrARN, version)
	}

	return &Guardrail{
		CreatedAt:               gv.CreatedAt,
		UpdatedAt:               gv.CreatedAt,
		Policies:                copyGuardrailPolicies(gv.Policies),
		GuardrailID:             gv.GuardrailID,
		GuardrailArn:            gv.GuardrailArn,
		Name:                    gv.Name,
		Description:             gv.Description,
		Status:                  "READY",
		Version:                 gv.Version,
		BlockedInputMessaging:   gv.BlockedInputMessaging,
		BlockedOutputsMessaging: gv.BlockedOutputsMessaging,
		Tags:                    copyTags(gv.Tags),
	}, nil
}
