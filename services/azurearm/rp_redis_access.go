package azurearm

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/redisacl"
)

const (
	redisPolicyType        = "accessPolicies"
	redisAssignmentType    = "accessPolicyAssignments"
	redisPolicyTypeCustom  = "Custom"
	redisPolicyTypeBuiltIn = "BuiltIn"
	redisAssignMinName     = 3
	redisAssignMaxName     = 63
)

var (
	redisPolicyNameRE = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9- ]*[a-zA-Z0-9]|[a-zA-Z0-9])$`)
	redisAssignNameRE = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]|[a-zA-Z0-9])$`)
)

type redisPolicy struct {
	Name        string `json:"name"`
	Permissions string `json:"permissions"`
}

type redisAssignment struct {
	Name       string `json:"name"`
	PolicyName string `json:"policyName"`
	ObjectID   string `json:"objectId"`
	Alias      string `json:"alias"`
}

// redisBuiltInPolicies are the three policies every cache offers; Learn names them but publishes no permission
// strings, so these follow the role names (owner: everything, contributor: everything but @dangerous, reader: @read).
func redisBuiltInPolicies() []redisPolicy {
	return []redisPolicy{
		{Name: "Data Contributor", Permissions: "+@all -@dangerous allkeys"},
		{Name: "Data Owner", Permissions: "+@all allkeys"},
		{Name: "Data Reader", Permissions: "+@read +@connection allkeys"},
	}
}

func redisBuiltIn(name string) (redisPolicy, bool) {
	for _, p := range redisBuiltInPolicies() {
		if strings.EqualFold(p.Name, name) {
			return p, true
		}
	}

	return redisPolicy{}, false
}

func (rec *redisRecord) policyNamed(name string) (redisPolicy, bool) {
	if p, ok := redisBuiltIn(name); ok {
		return p, true
	}

	p, ok := rec.Policies[strings.ToLower(name)]

	return p, ok
}

func redisAccessAssignments(rec *redisRecord) []RedisAccessAssignment {
	if len(rec.Assignments) == 0 {
		return nil
	}

	out := make([]RedisAccessAssignment, 0, len(rec.Assignments))

	for _, a := range sortedAssignments(rec) {
		if p, ok := rec.policyNamed(a.PolicyName); ok {
			out = append(out, RedisAccessAssignment{ObjectID: a.ObjectID, Alias: a.Alias, Permissions: p.Permissions})
		}
	}

	return out
}

func (p *RedisProvider) putPolicy(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	name := id.LeafName()
	props, _ := body[fieldProperties].(map[string]any)
	perms, _ := props[authFieldPermissions].(string)

	if _, builtIn := redisBuiltIn(name); builtIn {
		return nil, badRedisRequest(
			fmt.Errorf("%w: the built-in access policy %q cannot be changed", ErrRedisInvalid, name),
		)
	}

	if !redisPolicyNameRE.MatchString(name) {
		return nil, badRedisRequest(fmt.Errorf("%w: invalid access policy name %q", ErrRedisInvalid, name))
	}

	if _, err = redisacl.ParsePermissions(perms); err != nil || strings.TrimSpace(perms) == "" {
		return nil, badRedisRequest(fmt.Errorf("%w: invalid permissions: %w", ErrRedisInvalid, err))
	}

	key := strings.ToLower(name)
	if _, exists := rec.Policies[key]; !exists && len(rec.Policies) >= redisMaxChildren {
		return nil, badRedisRequest(fmt.Errorf("%w: at most %d access policies", ErrRedisInvalid, redisMaxChildren))
	}

	if rec.Policies == nil {
		rec.Policies = map[string]redisPolicy{}
	}

	prev, hadPrev := rec.Policies[key]
	rec.Policies[key] = redisPolicy{Name: name, Permissions: perms}

	if err = p.syncFirewall(id, rec); err != nil {
		if hadPrev {
			rec.Policies[key] = prev
		} else {
			delete(rec.Policies, key)
		}

		return nil, err
	}

	return policyBody(id, rec.Policies[key], redisPolicyTypeCustom), nil
}

func policyBody(id ResourceID, pol redisPolicy, kind string) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: pol.Name, fieldType: namespaceMicrosoftCache + "/Redis/" + redisPolicyType,
		fieldProperties: map[string]any{
			fieldType: kind, authFieldPermissions: pol.Permissions, fieldProvisioningState: provisioningStateSucceeded,
		},
	}
}

func (p *RedisProvider) putAssignment(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	a := redisAssignment{Name: id.LeafName()}
	a.PolicyName, _ = props["accessPolicyName"].(string)
	a.ObjectID, _ = props["objectId"].(string)
	a.Alias, _ = props["objectIdAlias"].(string)

	if err = validAssignment(rec, a); err != nil {
		return nil, badRedisRequest(err)
	}

	key := strings.ToLower(a.Name)
	if _, exists := rec.Assignments[key]; !exists && len(rec.Assignments) >= redisMaxChildren {
		return nil, badRedisRequest(
			fmt.Errorf("%w: at most %d access policy assignments", ErrRedisInvalid, redisMaxChildren))
	}

	if rec.Assignments == nil {
		rec.Assignments = map[string]redisAssignment{}
	}

	prev, hadPrev := rec.Assignments[key]
	rec.Assignments[key] = a

	if err = p.syncFirewall(id, rec); err != nil {
		if hadPrev {
			rec.Assignments[key] = prev
		} else {
			delete(rec.Assignments, key)
		}

		return nil, err
	}

	return assignmentBody(id, a), nil
}

func validAssignment(rec *redisRecord, a redisAssignment) error {
	if len(a.Name) < redisAssignMinName || len(a.Name) > redisAssignMaxName || !redisAssignNameRE.MatchString(a.Name) {
		return fmt.Errorf("%w: invalid access policy assignment name %q", ErrRedisInvalid, a.Name)
	}

	if a.PolicyName == "" || a.ObjectID == "" || a.Alias == "" {
		return fmt.Errorf("%w: accessPolicyName, objectId and objectIdAlias are required", ErrRedisInvalid)
	}

	if !redisPolicyNameRE.MatchString(a.PolicyName) {
		return fmt.Errorf("%w: invalid accessPolicyName %q", ErrRedisInvalid, a.PolicyName)
	}

	if _, ok := rec.policyNamed(a.PolicyName); !ok {
		return fmt.Errorf("%w: the access policy %q does not exist", ErrRedisInvalid, a.PolicyName)
	}

	return nil
}

func assignmentBody(id ResourceID, a redisAssignment) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: a.Name, fieldType: namespaceMicrosoftCache + "/Redis/" + redisAssignmentType,
		fieldProperties: map[string]any{
			"accessPolicyName": a.PolicyName, "objectId": a.ObjectID, "objectIdAlias": a.Alias,
			fieldProvisioningState: provisioningStateSucceeded,
		},
	}
}

func (p *RedisProvider) deletePolicy(id ResourceID, rec *redisRecord) error {
	key := strings.ToLower(id.LeafName())

	if _, builtIn := redisBuiltIn(id.LeafName()); builtIn {
		return badRedisRequest(
			fmt.Errorf("%w: the built-in access policy %q cannot be deleted", ErrRedisInvalid, id.LeafName()),
		)
	}

	if _, ok := rec.Policies[key]; !ok {
		return ErrRedisAccessPolicyNotFound
	}

	for _, a := range rec.Assignments {
		if strings.EqualFold(a.PolicyName, id.LeafName()) {
			return badRedisRequest(fmt.Errorf("%w: the access policy %q is assigned to %q",
				ErrRedisInvalid, id.LeafName(), a.Name))
		}
	}

	delete(rec.Policies, key)

	return nil
}

func (p *RedisProvider) deleteAssignment(id ResourceID, rec *redisRecord) error {
	key := strings.ToLower(id.LeafName())
	if _, ok := rec.Assignments[key]; !ok {
		return ErrRedisAccessAssignmentNotFound
	}

	delete(rec.Assignments, key)

	return p.syncFirewall(id, rec)
}

func (rec *redisRecord) policyBodyFor(id ResourceID) (map[string]any, error) {
	if b, ok := redisBuiltIn(id.LeafName()); ok {
		return policyBody(id, b, redisPolicyTypeBuiltIn), nil
	}

	pol, ok := rec.Policies[strings.ToLower(id.LeafName())]
	if !ok {
		return nil, ErrRedisAccessPolicyNotFound
	}

	return policyBody(id, pol, redisPolicyTypeCustom), nil
}

func (rec *redisRecord) assignmentBodyFor(id ResourceID) (map[string]any, error) {
	a, ok := rec.Assignments[strings.ToLower(id.LeafName())]
	if !ok {
		return nil, ErrRedisAccessAssignmentNotFound
	}

	return assignmentBody(id, a), nil
}

func listPolicies(parent ResourceID, rec *redisRecord) []map[string]any {
	out := make([]map[string]any, 0, len(rec.Policies)+len(redisBuiltInPolicies()))

	for _, b := range redisBuiltInPolicies() {
		out = append(out, policyBody(childID(parent, redisPolicyType, b.Name), b, redisPolicyTypeBuiltIn))
	}

	names := make([]string, 0, len(rec.Policies))
	for k := range rec.Policies {
		names = append(names, k)
	}

	sort.Strings(names)

	for _, k := range names {
		pol := rec.Policies[k]
		out = append(out, policyBody(childID(parent, redisPolicyType, pol.Name), pol, redisPolicyTypeCustom))
	}

	return out
}

func sortedAssignments(rec *redisRecord) []redisAssignment {
	out := make([]redisAssignment, 0, len(rec.Assignments))
	for _, a := range rec.Assignments {
		out = append(out, a)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func listAssignments(parent ResourceID, rec *redisRecord) []map[string]any {
	out := make([]map[string]any, 0, len(rec.Assignments))

	for _, a := range sortedAssignments(rec) {
		out = append(out, assignmentBody(childID(parent, redisAssignmentType, a.Name), a))
	}

	return out
}
