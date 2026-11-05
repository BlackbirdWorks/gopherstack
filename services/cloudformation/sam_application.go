package cloudformation

const samTypeStack = "AWS::CloudFormation::Stack"

var samApplicationProps = keySet("Location", "Parameters", "NotificationARNs", samKeyTags, "TimeoutInMinutes")

// translateApplication expands AWS::Serverless::Application into a nested stack. Only a template URL Location
// is supported; Serverless Application Repository locations (ApplicationId + SemanticVersion) are rejected
// because the transform has no repository to resolve them against.
func (t *samTranslator) translateApplication(id string, r map[string]any) error {
	props := asMap(r[samKeyProps])
	if err := rejectUnknown(id, props, samApplicationProps); err != nil {
		return err
	}
	switch loc := props["Location"].(type) {
	case string:
		if loc == "" {
			return samErr(id, "Property 'Location' cannot be empty.")
		}
	case map[string]any:
		return samErr(id, "Location with ApplicationId and SemanticVersion needs the Serverless Application Repository; "+
			"call CreateCloudFormationTemplate and use the returned TemplateUrl as Location.")
	case nil:
		return samErr(id, "Missing required property 'Location'.")
	default:
		return samErr(id, "Property 'Location' should be a string or a map.")
	}

	tags, _ := props[samKeyTags].(map[string]any)
	stack := map[string]any{"TemplateURL": props["Location"], samKeyTags: samTags(tags)}
	for _, k := range []string{"Parameters", "NotificationARNs", "TimeoutInMinutes"} {
		if v, ok := props[k]; ok {
			stack[k] = v
		}
	}
	res := map[string]any{samKeyType: samTypeStack, samKeyProps: stack}
	carryAttrs(r, res)

	return t.put(id, res)
}
