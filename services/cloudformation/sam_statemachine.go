package cloudformation

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	samTypeStateMachine = "AWS::StepFunctions::StateMachine"
	samSMCreatedBy      = "stateMachine:createdBy"
	samSMSubstitution   = "definition_substitution_"
	samXRayPolicyName   = "AWSXrayWriteOnlyAccess"
	samKeyRolePath      = "RolePath"
)

func samSMAllowed() map[string]bool {
	return keySet(
		"Definition",
		"DefinitionUri",
		"Logging",
		"Role",
		samKeyRolePath,
		"DefinitionSubstitutions",
		samKeyEvents,
		"Name",
		"Type",
		samKeyTags,
		"PropagateTags",
		"Policies",
		"Tracing",
		"PermissionsBoundary",
		samKeyAutoPublish,
		samKeyDeployPref,
		"UseAliasAsEventTarget",
	)
}

var samDynamicRefRe = regexp.MustCompile(`\{\{resolve:[^}]+\}\}`)

func (t *samTranslator) translateStateMachine(id string, r map[string]any) error {
	props := t.resProps("StateMachine", r)
	if err := rejectUnknown(id, props, samSMAllowed()); err != nil {
		return err
	}
	t.pass = passAttrs(r)
	defer func() { t.pass = nil }()
	sm := map[string]any{}
	if v := asMap(props["DefinitionSubstitutions"]); v != nil {
		sm["DefinitionSubstitutions"] = maps.Clone(v)
	}
	if err := smDefinition(id, props, sm); err != nil {
		return err
	}
	if props["Role"] != nil && props["Policies"] != nil {
		return samErr(id, "Specify either 'Role' or 'Policies' (but not both at the same time) or neither of them")
	}
	if props["Role"] != nil {
		sm["RoleArn"] = props["Role"]
	} else {
		sm["RoleArn"] = samGetAtt(id+"Role", attrNameArn)
		if err := t.putSMRole(id, props); err != nil {
			return err
		}
	}
	for src, dst := range map[string]string{attrNameName: "StateMachineName", "Type": "StateMachineType",
		"Logging": "LoggingConfiguration", "Tracing": "TracingConfiguration"} {
		if props[src] != nil {
			sm[dst] = props[src]
		}
	}
	sm[samKeyTags] = smTagList(props)
	base := map[string]any{samKeyType: samTypeStateMachine, samKeyProps: sm}
	carryAttrs(r, base)
	if err := t.put(id, base); err != nil {
		return err
	}
	target, err := t.putSMAlias(id, props)
	if err != nil {
		return err
	}

	return t.translateStateMachineEvents(id, target, asMap(props[samKeyEvents]), props["PermissionsBoundary"])
}

// smTagList is stateMachine:createdBy plus the user's Tags.
func smTagList(props map[string]any) []any {
	user := samTagList(asMap(props[samKeyTags]))
	tags := make([]any, 0, len(user)+1)
	tags = append(tags, map[string]any{samKeyKey: samSMCreatedBy, samKValue: samCreatedBy})

	return append(tags, user...)
}

// smDefinition fills DefinitionString (a Fn::Join of the indented JSON, with intrinsics turned into
// DefinitionSubstitutions) or DefinitionS3Location.
func smDefinition(id string, props, sm map[string]any) error {
	def, uri := props["Definition"], props["DefinitionUri"]
	switch {
	case def != nil && uri != nil:
		return samErr(id, "Specify either 'Definition' or 'DefinitionUri' property and not both.")
	case def != nil:
		doc, err := deepCopyAny(def)
		if err != nil {
			return samErr(id, "Definition is not valid JSON: %v", err)
		}
		subs := map[string]any{}
		counter := 1
		replaceDynamicValues(doc, subs, &counter)
		if len(subs) > 0 {
			merged := asMap(sm["DefinitionSubstitutions"])
			if merged == nil {
				merged = map[string]any{}
			}
			maps.Copy(merged, subs)
			sm["DefinitionSubstitutions"] = merged
		}
		lines := strings.Split(pyJSONIndented(doc), "\n")
		list := make([]any, len(lines))
		for i, l := range lines {
			list[i] = l
		}
		sm["DefinitionString"] = map[string]any{samFnJoin: []any{"\n", list}}
	case uri != nil:
		loc, err := s3Location(id, "DefinitionUri", uri, "Bucket", "Key", "Version")
		if err != nil {
			return err
		}
		sm["DefinitionS3Location"] = loc
	default:
		return samErr(id, "Either 'Definition' or 'DefinitionUri' property must be specified.")
	}

	return nil
}

func deepCopyAny(v any) (any, error) {
	m, err := deepCopyMap(map[string]any{"v": v})
	if err != nil {
		return nil, err
	}

	return m["v"], nil
}

// replaceDynamicValues swaps every intrinsic or dynamic reference in a definition for ${definition_substitution_N},
// walking keys in sorted order like SAM so the numbering matches.
func replaceDynamicValues(node any, subs map[string]any, counter *int) {
	visit := func(get func() any, set func(any)) {
		v := get()
		if isDynamicValue(v) {
			name := fmt.Sprintf("%s%d", samSMSubstitution, *counter)
			*counter++
			subs[name] = v
			set("${" + name + "}")

			return
		}
		replaceDynamicValues(v, subs, counter)
	}
	switch n := node.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(n)) {
			visit(func() any { return n[k] }, func(x any) { n[k] = x })
		}
	case []any:
		for i := range n {
			visit(func() any { return n[i] }, func(x any) { n[i] = x })
		}
	}
}

func isDynamicValue(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		return isSAMIntrinsic(t)
	case string:
		return samDynamicRefRe.MatchString(t)
	}

	return false
}

func (t *samTranslator) putSMRole(id string, props map[string]any) error {
	pol := &samRolePolicies{}
	policies := slices.Clone(asList(props["Policies"]))
	if asMap(props["Tracing"])["Enabled"] == true {
		policies = append(policies, samXRayPolicyName)
	}
	if err := t.applyPolicies(id, id+"Role", policies, pol); err != nil {
		return err
	}
	role := map[string]any{
		samKeyAssumeRole:  servicePrincipalTrust("states.amazonaws.com"),
		samKeyManagedARNs: toSubs(append([]any{}, pol.managed...)),
		samKeyTags:        smTagList(props),
	}
	if len(pol.inline) > 0 {
		role["Policies"] = pol.inline
	}
	if props["PermissionsBoundary"] != nil {
		role["PermissionsBoundary"] = props["PermissionsBoundary"]
	}
	if props[samKeyRolePath] != nil {
		role["Path"] = props[samKeyRolePath]
	}

	return t.put(id+"Role", map[string]any{samKeyType: resTypeIAMRole, samKeyProps: role})
}

// putSMAlias emits the StateMachine version and alias of AutoPublishAlias and returns the logical id events target.
func (t *samTranslator) putSMAlias(id string, props map[string]any) (string, error) {
	alias, hasAlias := props[samKeyAutoPublish]
	dp := props[samKeyDeployPref]
	switch {
	case !hasAlias && props["UseAliasAsEventTarget"] != nil:
		return "", samErr(id, "'UseAliasAsEventTarget' requires 'AutoPublishAlias' property to be specified.")
	case !hasAlias && dp != nil:
		return "", samErr(id, "'DeploymentPreference' requires 'AutoPublishAlias' property to be specified.")
	case !hasAlias:
		return id, nil
	}
	name, ok := alias.(string)
	if !ok || name == "" {
		return "", samErr(id, "'AutoPublishAlias' must be a string.")
	}
	version := map[string]any{samKeyType: samTypeSFNVersion, samKeyProps: map[string]any{
		"StateMachineArn":        samRef(id),
		"StateMachineRevisionId": samGetAtt(id, "StateMachineRevisionId"),
	}}
	for _, k := range []string{samKeyDelPolicy, samKeyUpdPolicy} {
		if _, has := t.pass[k]; !has {
			version[k] = deletionPolicyRetain
		}
	}
	if err := t.put(id+samKeyVersion, version); err != nil {
		return "", err
	}
	pref := map[string]any{}
	if m := asMap(dp); m != nil {
		pref = maps.Clone(m)
	} else {
		pref[samKeyType] = "ALL_AT_ONCE"
	}
	pref["StateMachineVersionArn"] = samRef(id + samKeyVersion)
	aliasID := id + "Alias" + name
	if err := t.put(
		aliasID,
		map[string]any{samKeyType: samTypeSFNAlias, samKeyProps: map[string]any{
			"Name": name, "DeploymentPreference": pref,
		}},
	); err != nil {
		return "", err
	}
	if props["UseAliasAsEventTarget"] == true {
		return aliasID, nil
	}

	return id, nil
}
