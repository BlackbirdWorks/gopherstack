package cloudformation

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

const (
	samTypeConnector     = samResPrefix + "Connector"
	samKeyConnectors     = "Connectors"
	samKeyID             = "Id"
	samKeyPermissions    = "Permissions"
	samKeySource         = "Source"
	samKeyMetadata       = "Metadata"
	samKeyAccessCats     = "AccessCategories"
	samKeySourcePolicy   = "SourcePolicy"
	samProfileIAMPolicy  = "AWS_IAM_ROLE_MANAGED_POLICY"
	samProfileSQSPolicy  = "AWS_SQS_QUEUE_POLICY"
	samProfileSNSPolicy  = "AWS_SNS_TOPIC_POLICY"
	samProfileLambdaPerm = "AWS_LAMBDA_PERMISSION"
	samGetAttParts       = 2
	samEventSourceMap    = "DESTINATION_EVENT_SOURCE_MAPPING"
)

// samConnRef is a resolved Source or Destination of a connector; nil fields are unknown.
type samConnRef struct {
	arn        any
	roleName   any
	queueURL   any
	resourceID any
	name       any
	qualifier  any
	logicalID  string
	resType    string
}

func samConnCFNType(t string) string {
	switch t {
	case samResPrefix + samKeyFunction:
		return samTypeFunction
	case samResPrefix + "StateMachine":
		return "AWS::StepFunctions::StateMachine"
	case samResPrefix + "Api":
		return samTypeRestAPI
	case samResPrefix + "HttpApi":
		return samTypeHTTPAPI
	case samResPrefix + "SimpleTable":
		return resTypeDynamoDBTable
	}

	return t
}

// extractEmbeddedConnectors turns each resource's Connectors attribute into AWS::Serverless::Connector resources.
func extractEmbeddedConnectors(res map[string]any) error {
	for _, srcID := range slices.Sorted(maps.Keys(res)) {
		r := asMap(res[srcID])
		raw, has := r[samKeyConnectors]
		if !has {
			continue
		}
		delete(r, samKeyConnectors)
		conns, ok := raw.(map[string]any)
		if !ok {
			return samErr(srcID, "Property '%s.Connectors' should be a map.", srcID)
		}
		for _, cid := range slices.Sorted(maps.Keys(conns)) {
			full := srcID + cid
			conn, err := embeddedConnector(srcID, full, cid, conns[cid])
			if err != nil {
				return err
			}
			if _, dup := res[full]; dup {
				return samErr(
					srcID,
					"Resource with id [%s] is invalid. Resource with this Logical Id already exists.",
					full,
				)
			}
			res[full] = conn
		}
	}

	return nil
}

func embeddedConnector(srcID, full, cid string, v any) (map[string]any, error) {
	d, ok := v.(map[string]any)
	if !ok {
		return nil, samErr(full, "%s.%s should be a map.", srcID, full)
	}
	conn, err := deepCopyMap(d)
	if err != nil {
		return nil, err
	}
	conn[samKeyType] = samTypeConnector
	props, ok := conn[samKeyProps].(map[string]any)
	if !ok {
		return nil, samErr(srcID, "Property 'Connectors.%s.Properties' should be a map.", cid)
	}
	source := map[string]any{samKeyID: srcID}
	if ref, has := props["SourceReference"]; has {
		refMap, isMap := ref.(map[string]any)
		if !isMap {
			return nil, samErr(srcID, "Property 'Connectors.%s.Properties.SourceReference' should be a map.", cid)
		}
		if _, hasID := refMap[samKeyID]; hasID {
			return nil, samErr(cid, "'Id' shouldn't be defined in 'SourceReference'.")
		}
		maps.Copy(source, refMap)
		delete(props, "SourceReference")
	}
	props[samKeySource] = source

	return conn, nil
}

// translateConnectors expands every AWS::Serverless::Connector against the already-translated resources.
func (t *samTranslator) translateConnectors(res map[string]any) error {
	for _, id := range slices.Sorted(maps.Keys(res)) {
		r := asMap(res[id])
		if r[samKeyType] != samTypeConnector {
			continue
		}
		generated, err := t.expandConnector(id, r)
		if err != nil {
			return err
		}
		if len(generated) == 0 {
			return samErr(id, "'Destination' is an empty list")
		}
		t.replaceDependsOn(id, generated)
	}

	return nil
}

func (t *samTranslator) expandConnector(id string, r map[string]any) ([]string, error) {
	props := asMap(r[samKeyProps])
	if err := rejectUnknown(id, props, keySet(samKeySource, samKeyDestination, samKeyPermissions)); err != nil {
		return nil, err
	}
	source, ok := props[samKeySource].(map[string]any)
	if !ok {
		return nil, samErr(id, "Property 'Source' should be a map.")
	}
	var dests []any
	multi := false
	switch d := props[samKeyDestination].(type) {
	case map[string]any:
		dests = []any{d}
	case []any:
		dests, multi = d, true
	default:
		return nil, samErr(id, "Property 'Destination' should be a map or a list of maps.")
	}
	perms, err := connectorPermissions(id, props[samKeyPermissions])
	if err != nil {
		return nil, err
	}

	var generated []string
	for i, d := range dests {
		dest := asMap(d)
		if dest == nil {
			return nil, samErr(id, "Property 'Destination' should be a map or a list of maps.")
		}
		ids, connErr := t.connectOne(id, r, source, dest, perms, connIndex{i: i, multi: multi})
		if connErr != nil {
			return nil, connErr
		}
		generated = append(generated, ids...)
	}

	return generated, nil
}

func connectorPermissions(id string, v any) ([]string, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, samErr(id, "Property 'Permissions' should be a list.")
	}
	var out []string
	for _, p := range list {
		s, isStr := p.(string)
		if !isStr {
			return nil, samErr(id, "Property 'Permissions' should be a list of strings.")
		}
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}

	return out, nil
}

type connIndex struct {
	i     int
	multi bool
}

// name builds a generated logical id: "<connector><suffix>Destination<i>" for lists, "<connector><suffix>" otherwise.
func (c connIndex) name(connector, suffix, mid string) string {
	if c.multi {
		return connector + suffix + mid + "Destination" + strconv.Itoa(c.i)
	}

	return connector + suffix + mid
}

func (t *samTranslator) connectOne(
	id string, r, srcObj, dstObj map[string]any, perms []string, idx connIndex,
) ([]string, error) {
	dest, err := t.connectorRef(id, dstObj, srcObj)
	if err != nil {
		return nil, err
	}
	source, err := t.connectorRef(id, srcObj, dstObj)
	if err != nil {
		return nil, err
	}
	entry, ok := samConnectorProfiles().Permissions[source.resType][dest.resType]
	if !ok {
		return nil, samErr(
			id,
			"Unable to create connector from %s to %s; it's not supported or the template is invalid.",
			source.resType,
			dest.resType,
		)
	}
	props, err := connectorProfileProps(id, entry, source, dest, perms)
	if err != nil {
		return nil, err
	}

	resources, err := t.connectorResources(id, entry.Type, source, dest, props, perms, idx)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{"aws:sam:connectors": map[string]any{id: map[string]any{
		samKeySource:      map[string]any{samKeyType: t.originalType(source)},
		samKeyDestination: map[string]any{samKeyType: t.originalType(dest)},
	}}}

	var ids []string
	for _, g := range resources {
		gid, _ := g["id"].(string)
		res := asMap(g["res"])
		carryConnectorAttrs(r, res, meta)
		if _, exists := t.out[gid]; exists {
			return nil, samErr(id, "Resource with id [%s] already exists in the template.", gid)
		}
		t.out[gid] = res
		ids = append(ids, gid)
	}

	return ids, nil
}

func carryConnectorAttrs(from, to, meta map[string]any) {
	for _, k := range []string{samKeyCondition, samKeyDelPolicy, samKeyUpdPolicy, "UpdatePolicy"} {
		if v, ok := from[k]; ok {
			to[k] = v
		}
	}
	if v, ok := from[samKeyDependsOn]; ok {
		to[samKeyDependsOn] = v
	}
	merged := map[string]any{}
	if orig, ok := from[samKeyMetadata].(map[string]any); ok {
		maps.Copy(merged, orig)
	}
	maps.Copy(merged, meta)
	to[samKeyMetadata] = merged
}

func (t *samTranslator) originalType(ref samConnRef) string {
	if orig, ok := t.input[ref.logicalID].(map[string]any); ok {
		if typ, _ := orig[samKeyType].(string); typ != "" {
			return typ
		}
	}
	if _, ok := t.apis[ref.logicalID]; ok {
		return samResPrefix + "Api"
	}
	if _, ok := t.httpAPIs[ref.logicalID]; ok {
		return samResPrefix + "HttpApi"
	}

	return ref.resType
}

func connectorProfileProps(
	id string, entry samConnProfileEntry, source, dest samConnRef, perms []string,
) (map[string]any, error) {
	cats := asMap(entry.Properties[samKeyAccessCats])
	valid := slices.Sorted(maps.Keys(cats))
	if len(perms) == 0 {
		return nil, samErr(id, "'Permissions' cannot be empty; valid values are: %s.", strings.Join(valid, ", "))
	}
	for _, p := range perms {
		if _, ok := cats[p]; !ok {
			return nil, samErr(
				id,
				"Unsupported 'Permissions' provided for connector from %s to %s; valid values are: %s.",
				source.resType,
				dest.resType,
				strings.Join(valid, ", "),
			)
		}
	}
	if err := checkPermissionCombos(id, entry, source, dest, perms); err != nil {
		return nil, err
	}
	repl := []samReplacement{
		rp("Source.Arn", source.arn), rp("Destination.Arn", dest.arn),
		rp("Source.ResourceId", source.resourceID), rp("Destination.ResourceId", dest.resourceID),
		rp("Source.Name", source.name), rp("Destination.Name", dest.name),
		rp("Source.Qualifier", source.qualifier), rp("Destination.Qualifier", dest.qualifier),
	}
	cp, err := deepCopyMap(entry.Properties)
	if err != nil {
		return nil, err
	}
	replaced, err := profileReplace(cp, repl)
	if err != nil {
		return nil, samErr(id, "%s.", err.Error())
	}
	if err = verifyProfileVarsReplaced(replaced); err != nil {
		return nil, samErr(id, "%s.", err.Error())
	}

	return asMap(replaced), nil
}

func checkPermissionCombos(id string, entry samConnProfileEntry, source, dest samConnRef, perms []string) error {
	combos, _ := entry.Properties["ValidAccessCategories"].([]any)
	if len(combos) == 0 {
		return nil
	}
	want := slices.Sorted(slices.Values(perms))
	var shown []string
	for _, c := range combos {
		var names []string
		for _, n := range asList(c) {
			s, _ := n.(string)
			names = append(names, s)
		}
		slices.Sort(names)
		if slices.Equal(names, want) {
			return nil
		}
		shown = append(shown, strings.Join(names, " + "))
	}

	return samErr(id, "Unsupported 'Permissions' provided for connector from %s to %s; valid combinations are: %s.",
		source.resType, dest.resType, strings.Join(shown, ", "))
}

// connectorRef resolves a Source/Destination object; connecting is the opposite side (used for Events rule roles).
func (t *samTranslator) connectorRef(id string, obj, connecting map[string]any) (samConnRef, error) {
	_, hasID := obj[samKeyID]
	other := 0
	for k := range obj {
		if k != samKeyID && k != samKeyQualifier {
			other++
		}
	}
	if hasID == (other > 0) {
		return samConnRef{}, samErr(id,
			"Must provide 'Id' (with optional 'Qualifier') or a supported combination of other properties.")
	}
	if !hasID {
		typ, _ := obj[samKeyType].(string)
		if typ == "" {
			return samConnRef{}, samErr(id, "'Type' is missing or not a string.")
		}

		return samConnRef{
			resType: samConnCFNType(typ), arn: obj["Arn"], roleName: obj["RoleName"], queueURL: obj["QueueUrl"],
			resourceID: obj["ResourceId"], name: obj["Name"], qualifier: obj[samKeyQualifier],
		}, nil
	}

	lid, _ := obj[samKeyID].(string)
	if lid == "" {
		return samConnRef{}, samErr(id, "'Id' is missing or not a string.")
	}
	res, ok := t.out[lid].(map[string]any)
	if !ok {
		return samConnRef{}, samErr(id, "Unable to find resource with logical ID '%s'.", lid)
	}
	typ, _ := res[samKeyType].(string)
	if typ == "" {
		return samConnRef{}, samErr(id, "'Type' is missing or not a string.")
	}
	cfn, err := profileReplace(samConnectorProfiles().CfnResourceProperties[typ].Outputs,
		[]samReplacement{rp("logicalId", lid)})
	if err != nil {
		return samConnRef{}, samErr(id, "%s.", err.Error())
	}
	outs := asMap(cfn)
	ref := samConnRef{
		logicalID: lid, resType: typ, arn: outs["Arn"], queueURL: outs["Url"],
		resourceID: outs["Id"], name: outs["Name"], qualifier: outs[samKeyQualifier],
		roleName: connectorRoleName(typ, connecting, asMap(res[samKeyProps])),
	}
	if q, has := obj[samKeyQualifier]; has {
		ref.qualifier = q
	}

	return ref, nil
}

// connectorRoleName is Ref of the role resource a function, state machine, rule target or data source uses.
func connectorRoleName(typ string, connecting, props map[string]any) any {
	in := samConnectorProfiles().CfnResourceProperties[typ].Inputs["Role"]
	var role any
	switch v := in.(type) {
	case string:
		role = props[v]
	case map[string]any:
		if v["Function"] == "GetEventsRuleRole" {
			role = eventsRuleRole(connecting, props)
		}
	}
	if lid := logicalIDFromIntrinsic(role); lid != "" {
		return samRef(lid)
	}

	return nil
}

func eventsRuleRole(connecting, props map[string]any) any {
	cid, _ := connecting[samKeyID].(string)
	for _, tg := range asList(props["Targets"]) {
		target := asMap(tg)
		tid := logicalIDFromIntrinsic(target["Arn"])
		if (tid != "" && tid == cid) ||
			(connecting["Arn"] != nil && fmt.Sprint(target["Arn"]) == fmt.Sprint(connecting["Arn"])) {
			return target["RoleArn"]
		}
	}

	return nil
}

func logicalIDFromIntrinsic(v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return ""
	}
	if s, isStr := m[samKeyRef].(string); isStr {
		return s
	}
	switch ga := m[fnGetAtt].(type) {
	case []any:
		if s, isStr := ga[0].(string); isStr && len(ga) == 2 {
			return s
		}
	case string:
		if parts := strings.Split(ga, "."); len(parts) == samGetAttParts {
			return parts[0]
		}
	}

	return ""
}

func policyStatements(props map[string]any, perms []string) map[string]any {
	cats := asMap(props[samKeyAccessCats])
	stmts := []any{}
	for _, name := range slices.Sorted(maps.Keys(cats)) {
		if slices.Contains(perms, name) {
			stmts = append(stmts, asList(asMap(cats[name])[samKeyStatement])...)
		}
	}

	return map[string]any{samKeyVersion: samPolicyVersion, samKeyStatement: stmts}
}

// connectorResources builds the generated resources as {id, res} pairs.
func (t *samTranslator) connectorResources(
	id, typ string, source, dest samConnRef, props map[string]any, perms []string, idx connIndex,
) ([]map[string]any, error) {
	srcPolicy, _ := props[samKeySourcePolicy].(bool)
	target, which := dest, samKeyDestination
	if srcPolicy {
		target, which = source, samKeySource
	}
	mk := func(gid, rt string, p map[string]any) map[string]any {
		return map[string]any{"id": gid, "res": map[string]any{samKeyType: rt, samKeyProps: p}}
	}
	missing := func(what string) error { return samErr(id, "Unable to get %s from '%s' resource.", what, which) }

	switch typ {
	case samProfileIAMPolicy:
		if target.roleName == nil {
			return nil, samErr(id, "Unable to get IAM role name from '%s' resource.", which)
		}
		gid := idx.name(id, "Policy", "")
		t.addConnectorDependsOn(props, gid, source, dest)

		return []map[string]any{mk(gid, "AWS::IAM::ManagedPolicy", map[string]any{
			samKeyPolicyDoc: policyStatements(props, perms), "Roles": []any{target.roleName},
		})}, nil
	case samProfileSQSPolicy:
		if target.queueURL == nil {
			return nil, missing("SQS queue URL")
		}

		return []map[string]any{mk(idx.name(id, "QueuePolicy", ""), "AWS::SQS::QueuePolicy", map[string]any{
			samKeyPolicyDoc: policyStatements(props, perms), "Queues": []any{target.queueURL},
		})}, nil
	case samProfileSNSPolicy:
		if target.arn == nil {
			return nil, missing("SNS topic ARN")
		}

		return []map[string]any{mk(idx.name(id, "TopicPolicy", ""), "AWS::SNS::TopicPolicy", map[string]any{
			samKeyPolicyDoc: policyStatements(props, perms), "Topics": []any{target.arn},
		})}, nil
	case samProfileLambdaPerm:
		if target.arn == nil {
			return nil, missing("Lambda function ARN")
		}

		return lambdaPermissions(id, target, props, perms, idx), nil
	}

	return nil, samErr(id, "Profile type %s is not supported", typ)
}

func lambdaPermissions(id string, fn samConnRef, props map[string]any, perms []string, idx connIndex) []map[string]any {
	cats := asMap(props[samKeyAccessCats])
	var out []map[string]any
	for _, name := range slices.Sorted(maps.Keys(cats)) {
		if !slices.Contains(perms, name) {
			continue
		}
		c := asMap(cats[name])
		p := map[string]any{
			samKeyAction: c[samKeyAction], samKeyFnName: fn.arn, samKeyPrincipal: c[samKeyPrincipal],
			"SourceArn": c["SourceArn"],
		}
		if sa, ok := c["SourceAccount"]; ok && sa != nil {
			p["SourceAccount"] = sa
		}
		gid := idx.name(id, name, "LambdaPermission")
		out = append(out, map[string]any{"id": gid, "res": map[string]any{samKeyType: samTypePerm, samKeyProps: p}})
	}

	return out
}

// addConnectorDependsOn makes the event source mapping (or source) wait for the generated policy.
func (t *samTranslator) addConnectorDependsOn(props map[string]any, policyID string, source, dest samConnRef) {
	switch props["DependedBy"] {
	case samEventSourceMap:
		if source.logicalID == "" || dest.logicalID == "" {
			return
		}
		var esms []string
		for lid, v := range t.out {
			r := asMap(v)
			if r[samKeyType] != samTypeESM {
				continue
			}
			p := asMap(r[samKeyProps])
			if logicalIDFromIntrinsic(p[samKeyFnName]) == dest.logicalID &&
				logicalIDFromIntrinsic(p["EventSourceArn"]) == source.logicalID {
				esms = append(esms, lid)
			}
		}
		if len(esms) == 1 {
			addDependsOn(asMap(t.out[esms[0]]), policyID)
		}
	case "SOURCE":
		if source.logicalID != "" {
			if r, ok := t.out[source.logicalID].(map[string]any); ok {
				addDependsOn(r, policyID)
			}
		}
	}
}

func addDependsOn(r map[string]any, dep string) {
	deps := slices.Clone(asList(r[samKeyDependsOn]))
	if !slices.Contains(deps, any(dep)) {
		deps = append(deps, dep)
	}
	r[samKeyDependsOn] = deps
}

// replaceDependsOn swaps a connector's id in every DependsOn for the resources it generated.
func (t *samTranslator) replaceDependsOn(id string, generated []string) {
	for _, v := range t.out {
		r := asMap(v)
		raw, has := r[samKeyDependsOn]
		if !has {
			continue
		}
		deps := slices.Clone(asList(raw))
		i := slices.Index(deps, any(id))
		if i < 0 {
			continue
		}
		deps = slices.Delete(deps, i, i+1)
		for _, g := range generated {
			if !slices.Contains(deps, any(g)) {
				deps = append(deps, g)
			}
		}
		r[samKeyDependsOn] = deps
	}
}
