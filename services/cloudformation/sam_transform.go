package cloudformation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	samTransformName = "AWS::Serverless-2016-10-31"
	samHashLen       = 10
	samResPrefix     = "AWS::Serverless::"
	samKeyProps      = "Properties"
	samKeyType       = "Type"
	samKeyRef        = "Ref"
	samKeyFnSub      = "Fn::Sub"
	samKeyDependsOn  = "DependsOn"
	samKeyDesc       = "Description"
	samKeyFnName     = "FunctionName"
	samKeyCondition  = "Condition"
	samKeyTableName  = "TableName"
	samKeyTopicArn   = "TopicArn"
	samTypeFunction  = "AWS::Lambda::Function"
	samTypePerm      = "AWS::Lambda::Permission"
	samTypeESM       = "AWS::Lambda::EventSourceMapping"
	samTypeVersion   = "AWS::Lambda::Version"
	samTypeAlias     = "AWS::Lambda::Alias"
	samTypeLayer     = "AWS::Lambda::LayerVersion"
	samTypeRule      = "AWS::Events::Rule"
	samTypeSubscr    = "AWS::SNS::Subscription"
	samTypeRestAPI   = "AWS::ApiGateway::RestApi"
	samTypeDeploy    = "AWS::ApiGateway::Deployment"
	samTypeStage     = "AWS::ApiGateway::Stage"
	samStateEnabled  = "ENABLED"
	samStateDisabled = "DISABLED"
	samTypeString    = "String"
	samKeyStageName  = "StageName"
	samKeyResponses  = "responses"
	samKeyTypeLower  = "type"
	samTokenType     = "TOKEN"
	samKeyAction     = "Action"
	samKeyPrincipal  = "Principal"
	samKeyState      = "State"
	samKeyAssumeRole = "AssumeRolePolicyDocument"
	samPolicyVersion = "2012-10-17"
	samKeyStatement  = "Statement"
	samKeyPolicyName = "PolicyName"
	samKeyFunction   = "Function"
	samJSONMime      = "application/json"
	samAuthHeader    = "Authorization"
	samKeyVersion    = "Version"
	samKeyEffect     = "Effect"
)

// ErrSAMTransform is returned when a SAM template cannot be expanded.
var ErrSAMTransform = errors.New("transform " + samTransformName + " failed with: " +
	"Invalid Serverless Application Specification document")

// samTranslator expands AWS::Serverless::* into plain CloudFormation; logical IDs follow
// the SAM developer guide page "sam-specification-generated-resources".
type samTranslator struct {
	out       map[string]any
	globals   map[string]map[string]any
	apis      map[string]*samAPI
	httpAPIs  map[string]*samHTTPAPI
	s3Configs map[string][]any
	refs      map[string]string
}

func samErr(id, format string, args ...any) error {
	return fmt.Errorf("%w. Resource with id [%s] is invalid. %s", ErrSAMTransform, id, fmt.Sprintf(format, args...))
}

func samRef(id string) map[string]any { return map[string]any{samKeyRef: id} }

func samGetAtt(id, attr string) map[string]any {
	return map[string]any{fnGetAtt: []any{id, attr}}
}

func samHash(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])[:samHashLen]
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
func expandSAMDocument(doc map[string]any) error {
	t := &samTranslator{
		out:       map[string]any{},
		globals:   map[string]map[string]any{},
		apis:      map[string]*samAPI{},
		httpAPIs:  map[string]*samHTTPAPI{},
		s3Configs: map[string][]any{},
		refs:      map[string]string{},
	}
	if err := t.readGlobals(asMap(doc["Globals"])); err != nil {
		return err
	}
	delete(doc, "Globals")

	res := asMap(doc["Resources"])
	if err := t.translate(res); err != nil {
		return err
	}
	doc["Resources"] = rewriteSAMRefs(t.out, t.refs)

	return nil
}

func (t *samTranslator) readGlobals(g map[string]any) error {
	for section, v := range g {
		switch section {
		case samKeyFunction, "Api", "HttpApi", "SimpleTable":
			t.globals[section] = asMap(v)
		default:
			return fmt.Errorf("%w. Globals section [%s] is not supported", ErrSAMTransform, section)
		}
	}

	return nil
}

func (t *samTranslator) translate(res map[string]any) error {
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

	if err := t.finalizeAPIs(); err != nil {
		return err
	}

	if err := t.finalizeHTTPAPIs(); err != nil {
		return err
	}

	return t.finalizeS3Events()
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
	switch typ {
	case samResPrefix + samKeyFunction:
		return t.translateFunction(id, r)
	case samResPrefix + "SimpleTable":
		return t.translateSimpleTable(id, r)
	case samResPrefix + "LayerVersion":
		return t.translateLayer(id, r)
	case samResPrefix + "StateMachine":
		return t.translateStateMachine(id, r)
	case samResPrefix + "Api", samResPrefix + "HttpApi":
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
	if _, exists := t.out[id]; exists {
		return samErr(id, "Resource with this logical ID already exists in the template")
	}
	t.out[id] = r

	return nil
}

// carryAttrs copies resource-level attributes (DependsOn, Condition, ...) from
// a SAM resource onto its generated base resource.
func carryAttrs(from, to map[string]any) {
	for _, k := range []string{samKeyDependsOn, samKeyCondition, "DeletionPolicy", "UpdateReplacePolicy", "Metadata"} {
		if v, ok := from[k]; ok {
			to[k] = v
		}
	}
}

func (t *samTranslator) resProps(section string, r map[string]any) map[string]any {
	return mergeSAMGlobals(t.globals[section], asMap(r[samKeyProps]))
}

// samMaxMergeLen bounds template-supplied list lengths before concatenation.
const samMaxMergeLen = 1 << 16

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
		switch l := lv.(type) {
		case map[string]any:
			if g, isMap := gv.(map[string]any); isMap {
				out[k] = mergeSAMGlobals(g, l)

				continue
			}
		case []any:
			if g, isList := gv.([]any); isList && len(g) <= samMaxMergeLen && len(l) <= samMaxMergeLen {
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
	out := make([]any, 1, len(tags)+1)
	out[0] = map[string]any{"Key": "lambda:createdBy", "Value": "SAM"}
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		out = append(out, map[string]any{"Key": k, "Value": tags[k]})
	}

	return out
}

// rewriteSAMRefs applies referenceable-property rewrites (Ref Fn.Alias etc.).
func rewriteSAMRefs(v any, refs map[string]string) map[string]any {
	if len(refs) == 0 {
		return asMap(v)
	}

	return asMap(rewriteRefValue(v, refs))
}

func rewriteRefValue(v any, refs map[string]string) any {
	switch t := v.(type) {
	case map[string]any:
		if ref, ok := t[samKeyRef].(string); ok && len(t) == 1 {
			if to, hit := refs[ref]; hit {
				return samRef(to)
			}
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

// processedTemplateBody returns the template as CloudFormation reports it for
// GetTemplate TemplateStage=Processed: SAM expanded and its Transform removed.
func processedTemplateBody(body string) (string, error) {
	if !strings.Contains(body, samTransformName) {
		return body, nil
	}
	doc, err := decodeTemplateDoc(body)
	if err != nil || !isSAMTransform(doc) {
		return body, err
	}
	if err = expandSAMDocument(doc); err != nil {
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
	if err = expandSAMDocument(doc); err != nil {
		return "", err
	}
	out, err := json.Marshal(doc)

	return string(out), err
}

func decodeTemplateDoc(body string) (map[string]any, error) {
	jsonBody := strings.TrimSpace(body)
	if !strings.HasPrefix(jsonBody, "{") {
		converted, err := yamlToJSON(jsonBody)
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
