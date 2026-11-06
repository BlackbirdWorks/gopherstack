package cloudformation

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	samTransformName   = "AWS::Serverless-2016-10-31"
	samHashLen         = 10
	samResPrefix       = "AWS::Serverless::"
	samKeyProps        = "Properties"
	samKeyType         = "Type"
	samKeyRef          = "Ref"
	samKeyFnSub        = "Fn::Sub"
	samKeyDependsOn    = "DependsOn"
	samKeyDesc         = "Description"
	samKeyFnName       = "FunctionName"
	samKeyCondition    = "Condition"
	samKeyTableName    = "TableName"
	samKeyTopicArn     = "TopicArn"
	samTypeFunction    = "AWS::Lambda::Function"
	samTypePerm        = "AWS::Lambda::Permission"
	samTypeESM         = "AWS::Lambda::EventSourceMapping"
	samTypeVersion     = "AWS::Lambda::Version"
	samTypeAlias       = "AWS::Lambda::Alias"
	samTypeLayer       = "AWS::Lambda::LayerVersion"
	samTypeRule        = "AWS::Events::Rule"
	samTypeSubscr      = "AWS::SNS::Subscription"
	samTypeRestAPI     = "AWS::ApiGateway::RestApi"
	samTypeDeploy      = "AWS::ApiGateway::Deployment"
	samTypeStage       = "AWS::ApiGateway::Stage"
	samStateEnabled    = "ENABLED"
	samStateDisabled   = "DISABLED"
	samTypeString      = "String"
	samKeyStageName    = "StageName"
	samKeyResponses    = "responses"
	samKeyTypeLower    = "type"
	samTokenType       = "TOKEN"
	samKeyAction       = "Action"
	samKeyPrincipal    = "Principal"
	samKeyState        = "State"
	samKeyAssumeRole   = "AssumeRolePolicyDocument"
	samPolicyVersion   = "2012-10-17"
	samKeyStatement    = "Statement"
	samKeyPolicyName   = "PolicyName"
	samKeyFunction     = "Function"
	samJSONMime        = "application/json"
	samAuthHeader      = "Authorization"
	samKeyVersion      = "Version"
	samKeyEffect       = "Effect"
	samTypeAPIKey      = "apiKey"
	samKeyHTTPMethod   = "httpMethod"
	samKeyURI          = "uri"
	samKeyStageVars    = "StageVariables"
	samCreatedBy       = "SAM"
	samKeyDomainName   = "DomainName"
	samKeyCorsProp     = "Cors"
	samKeyDelPolicy    = "DeletionPolicy"
	samKeyUpdPolicy    = "UpdateReplacePolicy"
	samLocPath         = "path"
	samKeyOrigins      = "AllowOrigins"
	samFnJoin          = "Fn::Join"
	samFnNot           = "Fn::Not"
	samFnAnd           = "Fn::And"
	samFnOr            = "Fn::Or"
	samKeyHeaders      = "Headers"
	samKeyEnabled      = "Enabled"
	samKeyKeyID        = "KeyId"
	samKeyManagedARNs  = "ManagedPolicyArns"
	samKeyEvents       = "Events"
	samDestLambda      = "Lambda"
	samKeyAllowCreds   = "AllowCredentials"
	samKeyImage        = "Image"
	samKeyBatchSize    = "BatchSize"
	samKeyDatabaseName = "DatabaseName"
	samKeyKey          = "Key"
	samKeyLogGroup     = "LogGroupName"
	samTypeUserPool    = "AWS::Cognito::UserPool"
)

// ErrSAMTransform is returned when a SAM template cannot be expanded.
var ErrSAMTransform = errors.New("transform " + samTransformName + " failed with: " +
	"Invalid Serverless Application Specification document")

// samTranslator expands AWS::Serverless::* into plain CloudFormation; logical IDs follow
// the SAM developer guide page "sam-specification-generated-resources".
type samTranslator struct {
	refs       map[string]string
	params     map[string]any
	apis       map[string]*samAPI
	httpAPIs   map[string]*samHTTPAPI
	s3Configs  map[string][]any
	s3Perms    map[string][]string
	cognito    map[string][]samCognitoTrigger
	input      map[string]any
	globals    map[string]map[string]any
	out        map[string]any
	pass       map[string]any
	conditions map[string]any
	mappings   map[string]any
	bucketTags map[string]map[string]any
	owner      map[string]string
	curTags    map[string]any
	cur        string
	deploys    []*samDeployPref
	sharedPlan samSharedPlan
}

func samErr(id, format string, args ...any) error {
	return fmt.Errorf("%w. Resource with id [%s] is invalid. %s", ErrSAMTransform, id, fmt.Sprintf(format, args...))
}

func samRef(id string) map[string]any { return map[string]any{samKeyRef: id} }

func samGetAtt(id, attr string) map[string]any {
	return map[string]any{fnGetAtt: []any{id, attr}}
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func asList(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case nil:
		return nil
	default:
		return []any{t}
	}
}

func isSAMTransform(doc map[string]any) bool {
	return slices.Contains(parseTransform(doc["Transform"]), samTransformName)
}

// expandSAMDocument expands a decoded SAM template in place of Resources and
// drops Globals; Transform is left for the caller to keep or strip.
func expandSAMDocument(doc map[string]any, region string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w. Unexpected template shape: %v", ErrSAMTransform, r)
		}
	}()

	if err = validateSAMDocument(doc); err != nil {
		return err
	}

	t := &samTranslator{
		out:        map[string]any{},
		globals:    map[string]map[string]any{},
		apis:       map[string]*samAPI{},
		httpAPIs:   map[string]*samHTTPAPI{},
		s3Configs:  map[string][]any{},
		s3Perms:    map[string][]string{},
		cognito:    map[string][]samCognitoTrigger{},
		input:      asMap(doc["Resources"]),
		refs:       map[string]string{},
		params:     map[string]any{},
		conditions: map[string]any{},
		mappings:   asMap(doc["Mappings"]),
		bucketTags: map[string]map[string]any{},
		owner:      map[string]string{},
	}
	if region != "" {
		t.params["AWS::Region"] = region
	}
	for name, p := range asMap(doc["Parameters"]) {
		if def, ok := asMap(p)["Default"]; ok {
			t.params[name] = def
		}
	}
	if err = t.readGlobals(asMap(doc["Globals"])); err != nil {
		return err
	}
	if err = t.validateSubVariables(doc["Resources"]); err != nil {
		return err
	}
	delete(doc, "Globals")

	res := asMap(doc["Resources"])
	if err = t.translate(res); err != nil {
		return err
	}
	doc["Resources"] = rewriteSAMRefs(t.out, t.refs)
	if len(t.conditions) > 0 {
		conds := asMap(doc["Conditions"])
		if conds == nil {
			conds = map[string]any{}
		}
		maps.Copy(conds, t.conditions)
		doc["Conditions"] = conds
	}

	return nil
}

func (t *samTranslator) readGlobals(g map[string]any) error {
	for section, v := range g {
		switch section {
		case samKeyFunction, "Api", "HttpApi", "SimpleTable", "StateMachine", "LayerVersion":
			if err := validateGlobalsSection(section, asMap(v)); err != nil {
				return err
			}
			t.globals[section] = asMap(v)
		default:
			return fmt.Errorf("%w. Globals section [%s] is not supported", ErrSAMTransform, section)
		}
	}

	return nil
}

func (t *samTranslator) translate(res map[string]any) error {
	if err := extractEmbeddedConnectors(res); err != nil {
		return err
	}
	if err := t.validateIgnoreGlobals(res); err != nil {
		return err
	}
	ids := slices.Sorted(maps.Keys(res))
	for _, id := range ids {
		r := asMap(res[id])
		if err := t.registerAPIs(id, r); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if err := t.translateOne(id, asMap(res[id])); err != nil {
			return err
		}
	}

	t.cur = ""
	if err := t.finalizeAPIs(); err != nil {
		return err
	}

	if err := t.finalizeHTTPAPIs(); err != nil {
		return err
	}

	if err := t.finalizeDeployments(); err != nil {
		return err
	}
	if err := t.finalizeS3Events(); err != nil {
		return err
	}

	if err := t.finalizeCognito(); err != nil {
		return err
	}

	return t.translateConnectors(res)
}

func (t *samTranslator) registerAPIs(id string, r map[string]any) error {
	switch r[samKeyType] {
	case samResPrefix + "Api":
		return t.registerAPI(id, r)
	case samResPrefix + "HttpApi":
		return t.registerHTTPAPI(id, r)
	}

	return nil
}

func (t *samTranslator) translateOne(id string, r map[string]any) error {
	typ, _ := r[samKeyType].(string)
	t.cur = id
	switch typ {
	case samResPrefix + samKeyFunction:
		return t.translateWithTags(samKeyFunction, r, func() error { return t.translateFunction(id, r) })
	case samResPrefix + "SimpleTable":
		return t.translateSimpleTable(id, r)
	case samResPrefix + "LayerVersion":
		return t.translateLayer(id, r)
	case samResPrefix + "StateMachine":
		return t.translateWithTags("StateMachine", r, func() error { return t.translateStateMachine(id, r) })
	case samResPrefix + "Application":
		return t.translateApplication(id, r)
	case samResPrefix + "Api", samResPrefix + "HttpApi", samTypeConnector:
		return nil
	}
	if strings.HasPrefix(typ, samResPrefix) {
		return samErr(id, "Resource type '%s' is not supported by this emulator's SAM transform", typ)
	}
	if _, exists := t.out[id]; !exists {
		t.out[id] = r
	}

	return nil
}

// put adds a generated resource, rejecting logical-ID collisions.
func (t *samTranslator) put(id string, r map[string]any) error {
	if err := t.checkUniqueID(id, r); err != nil {
		return err
	}
	for k, v := range t.pass {
		if _, has := r[k]; !has {
			r[k] = v
		}
	}
	t.out[id] = r

	return nil
}

// samReplacedTypes lists generated resource types that may reuse the id of the template resource they replace.
//
//nolint:gochecknoglobals // read-only table
var samReplacedTypes = map[string][]string{
	samTypeFunction: {samResPrefix + samKeyFunction}, samTypeLayer: {samResPrefix + "LayerVersion"},
	samTypeRestAPI: {samResPrefix + "Api"}, samTypeHTTPAPI: {samResPrefix + "HttpApi"},
	samTypeBucket: {samTypeBucket}, resTypeSNSTopic: {resTypeSNSTopic},
	resTypeDynamoDBTable: {samResPrefix + "SimpleTable"}, samTypeStack: {samResPrefix + "Application"},
	samTypeUserPool: {samTypeUserPool}, samTypeV1Domain: {samTypeV1Domain},
	samTypeV1Mapping:                   {samTypeV1Mapping},
	"AWS::StepFunctions::StateMachine": {samResPrefix + "StateMachine"},
}

// checkUniqueID rejects a generated id that collides with a different template resource, or an IAM role that two
// SAM resources both generate; any other generated resource silently replaces an earlier one, as in SAM.
func (t *samTranslator) checkUniqueID(id string, r map[string]any) error {
	typ, _ := r[samKeyType].(string)
	if orig, inInput := t.input[id].(map[string]any); inInput {
		origType, _ := orig[samKeyType].(string)
		if !slices.Contains(samReplacedTypes[typ], origType) {
			return samErr(id, "Resource with this logical ID already exists in the template")
		}
	}
	if prior, exists := t.out[id].(map[string]any); exists && typ == resTypeIAMRole && prior[samKeyType] == typ &&
		t.owner[id] != t.cur {
		return samErr(id, "Resource with this logical ID already exists in the template")
	}
	if typ == resTypeIAMRole {
		t.owner[id] = t.cur
	}

	return nil
}

// passAttrs are the resource attributes SAM copies onto every resource it generates from r.
func passAttrs(r map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range []string{samKeyCondition, samKeyDelPolicy, samKeyUpdPolicy} {
		if v, ok := r[k]; ok {
			out[k] = v
		}
	}

	return out
}

// carryAttrs copies resource-level attributes (DependsOn, Condition, ...) from
// a SAM resource onto its generated base resource.
func carryAttrs(from, to map[string]any) {
	for _, k := range []string{samKeyDependsOn, samKeyCondition, samKeyDelPolicy, samKeyUpdPolicy, "Metadata"} {
		if v, ok := from[k]; ok {
			to[k] = v
		}
	}
}

// paramDefault resolves a Ref to a template Parameter to its Default, for the few SAM
// properties that pick a structure rather than carry a value (the stack's own parameter
// values are not visible to the transform); anything else is returned unchanged.
func (t *samTranslator) paramDefault(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	name, _ := m[samKeyRef].(string)
	if def, has := t.params[name]; has {
		return def
	}

	return v
}

// resolveParamRefs replaces every Ref to a template parameter that has a Default, at any depth.
func (t *samTranslator) resolveParamRefs(v any) any {
	switch val := v.(type) {
	case map[string]any:
		if r := t.paramDefault(val); len(val) == 1 && val[samKeyRef] != nil {
			return r
		}
		if _, isSub := val[samKeyFnSub].(string); isSub && len(val) == 1 {
			return t.resolveStringParam(val)
		}
		if args, isFind := val["Fn::FindInMap"].([]any); isFind && len(val) == 1 {
			return t.resolveFindInMap(args)
		}
		out := make(map[string]any, len(val))
		for k, e := range val {
			out[k] = t.resolveParamRefs(e)
		}

		return out
	case []any:
		out := make([]any, len(val))
		for i, e := range val {
			out[i] = t.resolveParamRefs(e)
		}

		return out
	}

	return v
}

var samSubVarPattern = regexp.MustCompile(`\$\{([^}!]+)\}`)

// resolveStringParam substitutes template parameter defaults into a Ref or Fn::Sub string property.
func (t *samTranslator) resolveStringParam(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	if m[samKeyRef] != nil {
		return t.paramDefault(v)
	}
	s, isStr := m[samKeyFnSub].(string)
	if !isStr {
		return v
	}
	s = samSubVarPattern.ReplaceAllStringFunc(s, func(ref string) string {
		name := ref[2 : len(ref)-1]
		if def, isDefault := t.params[name].(string); isDefault {
			return def
		}

		return ref
	})

	return map[string]any{samKeyFnSub: s}
}

func (t *samTranslator) resProps(section string, r map[string]any) map[string]any {
	return mergeSAMGlobals(t.globalsFor(section, r), asMap(r[samKeyProps]))
}

// globalsFor is the section's Globals minus what the resource's IgnoreGlobals attribute drops.
func (t *samTranslator) globalsFor(section string, r map[string]any) map[string]any {
	g := t.globals[section]
	switch ig := r["IgnoreGlobals"].(type) {
	case string:
		return nil
	case []any:
		g = maps.Clone(g)
		for _, k := range ig {
			if name, ok := k.(string); ok {
				delete(g, name)
			}
		}
	}

	return g
}

// validateIgnoreGlobals rejects an IgnoreGlobals that is neither "*" nor a list of keys present in Globals.
func (t *samTranslator) validateIgnoreGlobals(res map[string]any) error {
	for _, id := range slices.Sorted(maps.Keys(res)) {
		r := asMap(res[id])
		ig, has := r["IgnoreGlobals"]
		if !has {
			continue
		}
		section := strings.TrimPrefix(asString(r[samKeyType]), samResPrefix)
		switch v := ig.(type) {
		case string:
			if v != "*" {
				return samErr(id, "Resource %s has invalid resource attribute 'IgnoreGlobals'.", id)
			}
		case []any:
			for _, k := range v {
				name, _ := k.(string)
				if _, ok := t.globals[section][name]; !ok {
					return samErr(
						id,
						"Resource %s has invalid resource attribute 'IgnoreGlobals' on item '%s'.",
						id,
						name,
					)
				}
			}
		default:
			return samErr(id, "Resource %s has invalid resource attribute 'IgnoreGlobals'.", id)
		}
	}

	return nil
}

// mergeSAMGlobals merges Globals under local props: maps recurse, lists
// concatenate (globals first), scalars take the local value.
func mergeSAMGlobals(global, local map[string]any) map[string]any {
	out := map[string]any{}
	maps.Copy(out, global)
	for k, lv := range local {
		gv, ok := out[k]
		if !ok {
			out[k] = lv

			continue
		}
		if isSAMIntrinsic(asMap(lv)) || isSAMIntrinsic(asMap(gv)) {
			out[k] = lv

			continue
		}
		switch l := lv.(type) {
		case map[string]any:
			if g, isMap := gv.(map[string]any); isMap {
				out[k] = mergeSAMGlobals(g, l)

				continue
			}
		case []any:
			if g, isList := gv.([]any); isList {
				out[k] = append(slices.Clone(g), l...)

				continue
			}
		}
		out[k] = lv
	}

	return out
}

// rejectUnknown fails on any property outside allowed so nothing is dropped silently.
func rejectUnknown(id string, props map[string]any, allowed ...map[string]bool) error {
	for _, k := range slices.Sorted(maps.Keys(props)) {
		ok := false
		for _, a := range allowed {
			ok = ok || a[k]
		}
		if !ok {
			return samErr(id, "Property '%s' is not supported by this emulator's SAM transform", k)
		}
	}

	return nil
}

func keySet(keys ...string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}

	return m
}

// samTags converts SAM's Tags map into CloudFormation's Key/Value list and
// adds the lambda:createdBy tag SAM always applies.
func samTags(tags map[string]any) []any {
	keys := slices.Sorted(maps.Keys(tags))
	rest := make([]any, 0, len(keys))
	for _, k := range keys {
		rest = append(rest, map[string]any{samKeyKey: k, samKValue: tags[k]})
	}

	return slices.Concat([]any{map[string]any{samKeyKey: "lambda:createdBy", samKValue: samCreatedBy}}, rest)
}

// rewriteSAMRefs applies referenceable-property rewrites (Ref Fn.Alias etc.).
func rewriteSAMRefs(v any, refs map[string]string) map[string]any {
	if len(refs) == 0 {
		return asMap(v)
	}

	return asMap(rewriteRefValue(v, refs))
}

// rewriteRefIntrinsic redirects a Ref or Fn::GetAtt on a renamed resource.
func rewriteRefIntrinsic(m map[string]any, refs map[string]string) (any, bool) {
	if len(m) != 1 {
		return nil, false
	}
	if ref, ok := m[samKeyRef].(string); ok {
		to, hit := refs[ref]

		return samRef(to), hit
	}
	if ga, ok := m[fnGetAtt].([]any); ok && len(ga) == samGetAttParts {
		if name, isStr := ga[0].(string); isStr {
			to, hit := refs[name]

			return map[string]any{fnGetAtt: []any{to, ga[1]}}, hit
		}
	}

	return nil, false
}

func rewriteRefValue(v any, refs map[string]string) any {
	switch t := v.(type) {
	case map[string]any:
		if replaced, ok := rewriteRefIntrinsic(t, refs); ok {
			return replaced
		}
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = rewriteRefValue(e, refs)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = rewriteRefValue(e, refs)
		}

		return out
	case string:
		for from, to := range refs {
			t = strings.ReplaceAll(t, "${"+from+"}", "${"+to+"}")
		}

		return t
	default:
		return v
	}
}

// ProcessedTemplateBody returns the template as CloudFormation reports it for
// GetTemplate TemplateStage=Processed: SAM expanded and its Transform removed. A non-empty region is
// substituted for AWS::Region where the transform resolves parameters.
func ProcessedTemplateBody(body, region string) (string, error) {
	if !strings.Contains(body, samTransformName) {
		return body, nil
	}
	doc, err := decodeTemplateDoc(body)
	if err != nil || !isSAMTransform(doc) {
		return body, err
	}
	if err = expandSAMDocument(doc, region); err != nil {
		return "", err
	}
	if rest := slices.DeleteFunc(parseTransform(doc["Transform"]), func(s string) bool {
		return s == samTransformName
	}); len(rest) > 0 {
		doc["Transform"] = rest
	} else {
		delete(doc, "Transform")
	}
	out, err := json.MarshalIndent(doc, "", "  ")

	return string(out), err
}

// expandSAMBody returns body with SAM expanded (Transform retained so the
// AUTO_EXPAND capability check still sees it); non-SAM bodies pass through.
func expandSAMBody(body string) (string, error) {
	if !strings.Contains(body, samTransformName) {
		return body, nil
	}
	doc, err := decodeTemplateDoc(body)
	if err != nil || !isSAMTransform(doc) {
		return body, err
	}
	if err = expandSAMDocument(doc, ""); err != nil {
		return "", err
	}
	out, err := json.Marshal(doc)

	return string(out), err
}

func decodeTemplateDoc(body string) (map[string]any, error) {
	jsonBody := strings.TrimSpace(body)
	if !strings.HasPrefix(jsonBody, "{") {
		// A trailing block scalar keeps its final newline only if the body is not trimmed.
		converted, err := yamlToJSON(strings.TrimLeft(body, " \t\r\n"))
		if err != nil {
			return nil, fmt.Errorf("failed to parse YAML template: %w", err)
		}
		jsonBody = converted
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(jsonBody), &doc); err != nil {
		return nil, fmt.Errorf("failed to parse JSON template: %w", err)
	}

	return doc, nil
}
