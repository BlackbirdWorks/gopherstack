package gcpartifactregistry

import (
	"context"
	"slices"
	"sort"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var ruleMutable = []string{ //nolint:gochecknoglobals // immutable catalog
	"action", "operation", "condition", "package_id",
}

func cloneRule(r *artifactregistrypb.Rule) *artifactregistrypb.Rule {
	out, _ := proto.Clone(r).(*artifactregistrypb.Rule)

	return out
}

func validateRule(r *artifactregistrypb.Rule) error {
	if r.GetAction() == artifactregistrypb.Rule_ACTION_UNSPECIFIED {
		return errInvalid("rule.action is required.")
	}

	if _, ok := artifactregistrypb.Rule_Action_name[int32(r.GetAction())]; !ok {
		return errInvalid("Unknown rule action.")
	}

	if r.GetOperation() == artifactregistrypb.Rule_OPERATION_UNSPECIFIED {
		return errInvalid("rule.operation is required.")
	}

	if _, ok := artifactregistrypb.Rule_Operation_name[int32(r.GetOperation())]; !ok {
		return errInvalid("Unknown rule operation.")
	}

	return nil
}

// ruleInRepo lists the stored rules of a repository. Must hold b.mu.
func (b *InMemoryBackend) rulesOfLocked(ref repoRef) []*artifactregistrypb.Rule {
	prefix := ref.full() + "/rules/"

	var out []*artifactregistrypb.Rule

	for name, r := range b.rules {
		if len(name) > len(prefix) && name[:len(prefix)] == prefix {
			out = append(out, r)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })

	return out
}

func (b *InMemoryBackend) scopeTakenLocked(ref repoRef, pkg, except string) bool {
	return slices.ContainsFunc(b.rulesOfLocked(ref), func(r *artifactregistrypb.Rule) bool {
		return r.GetName() != except && r.GetPackageId() == pkg
	})
}

func (b *InMemoryBackend) requireRepoLocked(ref repoRef) error {
	if _, ok := b.repos[ref.full()]; !ok {
		return errNotFound("Repository %q not found.", ref.full())
	}

	return nil
}

// CreateRule stores a rule; a repository holds one rule for itself and one per package.
func (b *InMemoryBackend) CreateRule(in *artifactregistrypb.CreateRuleRequest) (*artifactregistrypb.Rule, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateID(in.GetRuleId()); err != nil {
		return nil, errInvalid("rule_id %q must be 1-63 lowercase letters, digits or hyphens.", in.GetRuleId())
	}

	if in.GetRule() == nil {
		return nil, errInvalid("rule is required.")
	}

	if err = validateRule(in.GetRule()); err != nil {
		return nil, err
	}

	rule := cloneRule(in.GetRule())
	rule.Name = ref.ruleName(in.GetRuleId())

	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if err = b.requireRepoLocked(ref); err != nil {
		return nil, err
	}

	if _, exists := b.rules[rule.GetName()]; exists {
		return nil, errExists("Rule %q already exists.", rule.GetName())
	}

	if b.scopeTakenLocked(ref, rule.GetPackageId(), "") {
		return nil, errExists("A rule for %s already exists.", scopeLabel(rule.GetPackageId()))
	}

	b.rules[rule.GetName()] = rule

	return cloneRule(rule), nil
}

func scopeLabel(pkg string) string {
	if pkg == "" {
		return "the repository"
	}

	return "package " + pkg
}

// GetRule returns one rule.
func (b *InMemoryBackend) GetRule(name string) (*artifactregistrypb.Rule, error) {
	ref, id, err := parseRule(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	r, ok := b.rules[ref.ruleName(id)]
	if !ok {
		return nil, errNotFound("Rule %q not found.", name)
	}

	return cloneRule(r), nil
}

// ListRules lists a repository's rules by name.
func (b *InMemoryBackend) ListRules(
	in *artifactregistrypb.ListRulesRequest,
) ([]*artifactregistrypb.Rule, string, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	b.mu.RLock("ListRules")

	if err = b.requireRepoLocked(ref); err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	all := b.rulesOfLocked(ref)
	for i, r := range all {
		all[i] = cloneRule(r)
	}
	b.mu.RUnlock()

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

// UpdateRule applies the masked fields; an empty mask copies every field the request sets.
func (b *InMemoryBackend) UpdateRule(in *artifactregistrypb.UpdateRuleRequest) (*artifactregistrypb.Rule, error) {
	ref, id, err := parseRule(in.GetRule().GetName())
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, ruleMutable); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	cur, ok := b.rules[ref.ruleName(id)]
	if !ok {
		return nil, errNotFound("Rule %q not found.", in.GetRule().GetName())
	}

	next := cloneRule(cur)
	src, dst := in.GetRule().ProtoReflect(), next.ProtoReflect()

	if len(paths) == 0 {
		for _, f := range ruleMutable {
			copyField(dst, src, f, false)
		}
	} else {
		for _, f := range paths {
			copyField(dst, src, f, true)
		}
	}

	if err = validateRule(next); err != nil {
		return nil, err
	}

	if b.scopeTakenLocked(ref, next.GetPackageId(), cur.GetName()) {
		return nil, errExists("A rule for %s already exists.", scopeLabel(next.GetPackageId()))
	}

	b.rules[cur.GetName()] = next

	return cloneRule(next), nil
}

// DeleteRule removes a rule.
func (b *InMemoryBackend) DeleteRule(name string) (*emptypb.Empty, error) {
	ref, id, err := parseRule(name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	if _, ok := b.rules[ref.ruleName(id)]; !ok {
		return nil, errNotFound("Rule %q not found.", name)
	}

	delete(b.rules, ref.ruleName(id))

	return &emptypb.Empty{}, nil
}

// downloadDenied reports whether a DENY rule without a condition forbids downloading the package. The rule of
// the package wins over the repository rule. Rules with a condition are stored but never evaluated.
func (b *InMemoryBackend) downloadDenied(_ context.Context, project, repoID, pkg string) bool {
	b.mu.RLock("downloadDenied")
	defer b.mu.RUnlock()

	name, _, ok := b.repoByKey(project, repoID)
	if !ok {
		return false
	}

	ref, err := parseRepo(name)
	if err != nil {
		return false
	}

	var repoRule, pkgRule *artifactregistrypb.Rule

	for _, r := range b.rulesOfLocked(ref) {
		if r.GetOperation() != artifactregistrypb.Rule_DOWNLOAD || r.GetCondition() != nil {
			continue
		}

		switch {
		case r.GetPackageId() == "":
			repoRule = r
		case ruleMatchesPackage(r.GetPackageId(), pkg):
			pkgRule = r
		}
	}

	if pkgRule != nil {
		return pkgRule.GetAction() == artifactregistrypb.Rule_DENY
	}

	return repoRule != nil && repoRule.GetAction() == artifactregistrypb.Rule_DENY
}

func ruleMatchesPackage(ruleID, pkg string) bool {
	if ruleID == pkg {
		return true
	}

	id, ok := unescapeImage(ruleID)

	return ok && id == pkg
}
