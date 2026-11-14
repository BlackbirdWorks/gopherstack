package cloudformation

import (
	"maps"
	"strings"
)

func samTableAllowed() map[string]bool {
	return keySet("PrimaryKey", "ProvisionedThroughput", samKeyTableName, scopeTags, "SSESpecification",
		"BillingMode", "PointInTimeRecoverySpecification")
}

func samLayerAllowed() map[string]bool {
	return keySet("LayerName", samKeyDesc, "ContentUri", "CompatibleRuntimes",
		"CompatibleArchitectures", "LicenseInfo", "RetentionPolicy", "PublishLambdaVersion")
}

func (t *samTranslator) translateSimpleTable(id string, r map[string]any) error {
	props := t.resProps("SimpleTable", r)
	if err := rejectUnknown(id, props, samTableAllowed()); err != nil {
		return err
	}
	key, typ := "id", samTypeString
	if pk := asMap(props["PrimaryKey"]); pk != nil {
		n, hasName := pk[attrNameName]
		ty, hasType := pk[samKeyType]
		switch {
		case !hasName || n == nil:
			return samErr(id, "Property 'PrimaryKey.Name' is required.")
		case !hasType || ty == nil:
			return samErr(id, "Property 'PrimaryKey.Type' is required.")
		}
		key, _ = n.(string)
		typ, _ = ty.(string)
	}
	attrType := map[string]string{samTypeString: "S", "Number": "N", "Binary": "B"}[typ]
	if attrType == "" {
		return samErr(id, "Type '%s' of PrimaryKey is invalid; must be String, Number or Binary.", typ)
	}
	tbl := map[string]any{
		"AttributeDefinitions": []any{map[string]any{"AttributeName": key, "AttributeType": attrType}},
		"KeySchema":            []any{map[string]any{"AttributeName": key, "KeyType": "HASH"}},
	}
	if tp := props["ProvisionedThroughput"]; tp != nil {
		tbl["ProvisionedThroughput"] = tp
	} else {
		tbl["BillingMode"] = "PAY_PER_REQUEST"
	}
	for _, k := range []string{samKeyTableName, "SSESpecification", "PointInTimeRecoverySpecification"} {
		if v, ok := props[k]; ok {
			tbl[k] = v
		}
	}
	if tags := asMap(props[scopeTags]); tags != nil {
		tbl[scopeTags] = samTagList(tags)
	}
	base := map[string]any{samKeyType: resTypeDynamoDBTable, samKeyProps: tbl}
	carryAttrs(r, base)

	return t.put(id, base)
}

func samTagList(tags map[string]any) []any {
	out := samTags(tags)

	return out[1:]
}

func (t *samTranslator) translateLayer(id string, r map[string]any) error {
	props := t.resProps("LayerVersion", r)
	if err := rejectUnknown(id, props, samLayerAllowed()); err != nil {
		return err
	}
	layer := map[string]any{"LayerName": id}
	for _, k := range []string{"LayerName", samKeyDesc, "CompatibleRuntimes", "LicenseInfo"} {
		if v, ok := props[k]; ok {
			layer[k] = t.resolveStringParam(v)
		}
	}
	if v, ok := props["CompatibleArchitectures"]; ok {
		layer["CompatibleArchitectures"] = v
	}
	content, err := s3Location(id, "ContentUri", props["ContentUri"], "S3Bucket", "S3Key", "S3ObjectVersion")
	if err != nil {
		return err
	}
	layer["Content"] = content
	retention, hasRetention := props["RetentionPolicy"]
	body := layerHashBody(r)
	if hasRetention {
		body = maps.Clone(body)
		resolved := maps.Clone(asMap(body[samKeyProps]))
		resolved["RetentionPolicy"] = t.paramDefault(retention)
		body[samKeyProps] = resolved
	}
	layerID := samLogicalID(id, map[string]any{id: body})
	t.refs[id] = layerID
	base := map[string]any{samKeyType: samTypeLayer, samKeyProps: layer}
	carryAttrs(r, base)
	t.mapLayerDependsOn(base)
	policy, err := layerDeletionPolicy(id, t.paramDefault(retention), base[samKeyDelPolicy])
	if err != nil {
		return err
	}
	if policy != nil {
		base[samKeyDelPolicy] = policy
	}

	return t.put(layerID, base)
}

// mapLayerDependsOn points DependsOn at the hashed ids of layers already translated.
func (t *samTranslator) mapLayerDependsOn(res map[string]any) {
	remap := func(v any) any {
		if s, ok := v.(string); ok {
			if id, has := t.refs[s]; has {
				return id
			}
		}

		return v
	}
	switch d := res["DependsOn"].(type) {
	case string:
		res["DependsOn"] = remap(d)
	case []any:
		out := make([]any, len(d))
		for i, e := range d {
			out[i] = remap(e)
		}
		res["DependsOn"] = out
	}
}

// layerDeletionPolicy maps RetentionPolicy (case-insensitive) onto DeletionPolicy; without it
// a user-set DeletionPolicy stands and the default is Retain.
func layerDeletionPolicy(id string, retention, existing any) (any, error) {
	switch v := retention.(type) {
	case nil:
		if existing != nil {
			return existing, nil
		}

		return deletionPolicyRetain, nil
	case string:
		switch strings.ToLower(v) {
		case "retain":
			return deletionPolicyRetain, nil
		case "delete":
			return "Delete", nil
		}
	default:
		return nil, nil //nolint:nilnil // unresolved intrinsic: leave as is
	}

	return nil, samErr(id, "'RetentionPolicy' must be one of the following options: ['Retain', 'Delete'].")
}

// layerHashBody is the SAM resource as written, minus passthrough attributes
// that SAM excludes from the layer's hash.
func layerHashBody(r map[string]any) map[string]any {
	body := maps.Clone(r)
	for _, k := range []string{samKeyDelPolicy, samKeyUpdPolicy, "Metadata"} {
		delete(body, k)
	}

	return body
}
