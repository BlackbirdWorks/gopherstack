package cloudformation

import (
	"maps"
	"slices"
)

const samKeyPropagateTags = "PropagateTags"

// samTagListTypes take Tags as a Key/Value list, samTagMapTypes as a map: the generated resources
// SAM's PropagateTags reaches.
//
//nolint:gochecknoglobals // read-only type tables
var (
	samTagListTypes = keySet(samTypeStack, resTypeDynamoDBTable, samTypeCDApp, samTypeCDGroup, resTypeIAMRole,
		"AWS::IoT::TopicRule", samTypeRestAPI, samTypeStage, samTypeV1Domain,
		"AWS::ApiGateway::DomainNameV2", "AWS::ApiGateway::UsagePlan", "AWS::ApiGateway::ApiKey", resTypeSNSTopic,
		"AWS::SQS::Queue", samTypeFunction, samTypeBucket, "AWS::StepFunctions::StateMachine")
	samTagMapTypes = keySet("AWS::ApiGatewayV2::Stage", "AWS::ApiGatewayV2::DomainName")
)

// plainTagList is CloudFormation's Key/Value list for a SAM tag map, without SAM's own tag.
func plainTagList(tags map[string]any) []any {
	out := make([]any, 0, len(tags))
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		v := tags[k]
		if v == nil {
			v = ""
		}
		out = append(out, map[string]any{samKeyKey: k, samKValue: v})
	}

	return out
}

// propagatedTags are the resource's Tags when PropagateTags is on and Tags is not empty.
func propagatedTags(props map[string]any) map[string]any {
	if props[samKeyPropagateTags] != true {
		return nil
	}
	tags := asMap(props[samKeyTags])
	if len(tags) == 0 {
		return nil
	}

	return tags
}

func (t *samTranslator) idSet() map[string]bool {
	out := make(map[string]bool, len(t.out))
	for id := range t.out {
		out[id] = true
	}

	return out
}

// propagateTagsTo replaces the Tags of the Tags-capable resources among ids.
func (t *samTranslator) propagateTagsTo(ids []string, tags map[string]any) {
	for _, id := range ids {
		r := asMap(t.out[id])
		typ, _ := r[samKeyType].(string)
		props := asMap(r[samKeyProps])
		switch {
		case samTagListTypes[typ]:
			if props == nil {
				props = map[string]any{}
				r[samKeyProps] = props
			}
			props[samKeyTags] = plainTagList(tags)
		case samTagMapTypes[typ]:
			props[samKeyTags] = maps.Clone(tags)
		}
	}
}

// propagateNewTags applies tags to every resource added to t.out since before.
func (t *samTranslator) propagateNewTags(before map[string]bool, tags map[string]any) {
	var ids []string
	for id := range t.out {
		if !before[id] {
			ids = append(ids, id)
		}
	}
	t.propagateTagsTo(ids, tags)
}

// addImplicitTags adds the propagating function's tags to an implicit API.
func (t *samTranslator) addImplicitTags(props map[string]any) {
	if len(t.curTags) == 0 {
		return
	}
	tags := maps.Clone(asMap(props[samKeyTags]))
	if tags == nil {
		tags = map[string]any{}
	}
	maps.Copy(tags, t.curTags)
	props[samKeyTags] = tags
	props[samKeyPropagateTags] = true
}

// translateWithTags runs fn and propagates the resource's Tags to everything it generated.
func (t *samTranslator) translateWithTags(section string, r map[string]any, fn func() error) error {
	tags := propagatedTags(t.resProps(section, r))
	if tags == nil {
		return fn()
	}
	before := t.idSet()
	t.curTags = tags
	defer func() { t.curTags = nil }()
	if err := fn(); err != nil {
		return err
	}
	t.propagateNewTags(before, tags)

	return nil
}
