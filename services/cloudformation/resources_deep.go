package cloudformation

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// resolveDeep resolves intrinsics anywhere inside a property value, leaving
// plain maps and lists structurally intact.
func resolveDeep(v any, params, physicalIDs map[string]string) any {
	switch t := v.(type) {
	case map[string]any:
		if isIntrinsicMap(t) {
			return resolve(t, params, physicalIDs)
		}
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = resolveDeep(e, params, physicalIDs)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = resolveDeep(e, params, physicalIDs)
		}

		return out
	default:
		return v
	}
}

func isIntrinsicMap(m map[string]any) bool {
	if len(m) != 1 {
		return false
	}
	for k := range m {
		return k == samKeyRef || k == samKeyCondition || strings.HasPrefix(k, "Fn::")
	}

	return false
}

// resolvedJSONProp renders a policy-document-style property (object or JSON string)
// as a JSON string with intrinsics resolved.
func resolvedJSONProp(props map[string]any, key string, params, physicalIDs map[string]string) string {
	switch t := props[key].(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		raw, err := json.Marshal(resolveDeep(t, params, physicalIDs))
		if err != nil {
			return ""
		}

		return string(raw)
	}
}

// inlineCodeZip packages CloudFormation's Code.ZipFile source the way the real
// service does, as a single index.<ext> file for the function's runtime.
func inlineCodeZip(runtime, source string) []byte {
	name := "index"
	switch {
	case strings.HasPrefix(runtime, "python"):
		name += ".py"
	case strings.HasPrefix(runtime, "nodejs"):
		name += ".js"
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		return nil
	}
	if _, err = w.Write([]byte(source)); err != nil {
		return nil
	}
	if err = zw.Close(); err != nil {
		return nil
	}

	return buf.Bytes()
}

func stringList(v any, params, physicalIDs map[string]string) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, e := range list {
		out = append(out, resolve(e, params, physicalIDs))
	}

	return out
}

// applyLambdaTuning copies the AWS::Lambda::Function properties beyond the
// name/runtime/handler/role basics (limits, environment, layers, code).
func applyLambdaTuning(fn *lambdabackend.FunctionConfiguration, props map[string]any, params, ids map[string]string) {
	fn.MemorySize = intProp(props, "MemorySize")
	fn.Timeout = intProp(props, "Timeout")
	fn.Architectures = stringList(props["Architectures"], params, ids)
	for _, arn := range stringList(props["Layers"], params, ids) {
		fn.Layers = append(fn.Layers, &lambdabackend.FunctionLayer{Arn: arn})
	}
	if env := asMap(props["Environment"]); env != nil {
		vars := map[string]string{}
		for k, v := range asMap(env["Variables"]) {
			vars[k] = resolve(v, params, ids)
		}
		fn.Environment = &lambdabackend.EnvironmentConfig{Variables: vars}
	}
	if tc := asMap(props["TracingConfig"]); tc != nil {
		fn.TracingConfig = &lambdabackend.TracingConfig{Mode: resolve(tc["Mode"], params, ids)}
	}
	if dl := asMap(props["DeadLetterConfig"]); dl != nil {
		fn.DeadLetterConfig = &lambdabackend.DeadLetterConfig{TargetArn: resolve(dl["TargetArn"], params, ids)}
	}
	applyLambdaCode(fn, asMap(props["Code"]), params, ids)
}

func applyLambdaCode(fn *lambdabackend.FunctionConfiguration, code map[string]any, params, ids map[string]string) {
	if code == nil {
		return
	}
	fn.S3BucketCode = resolve(code["S3Bucket"], params, ids)
	fn.S3KeyCode = resolve(code["S3Key"], params, ids)
	if src, ok := code["ZipFile"]; ok {
		fn.ZipData = inlineCodeZip(fn.Runtime, resolve(src, params, ids))
	}
	if img, ok := code["ImageUri"]; ok {
		fn.ImageURI = resolve(img, params, ids)
		fn.PackageType = "Image"
	}
}

// bodyTitle returns the info.title of an OpenAPI/Swagger Body property.
func bodyTitle(props map[string]any, params, ids map[string]string) string {
	info := asMap(asMap(props["Body"])["info"])

	return resolve(info["title"], params, ids)
}

func (rc *ResourceCreator) putRuleTargets(
	ctx context.Context,
	rule, bus string,
	props map[string]any,
	params, ids map[string]string,
) error {
	raw, _ := props["Targets"].([]any)
	targets := make([]ebbackend.Target, 0, len(raw))
	for _, item := range raw {
		m := asMap(item)
		targets = append(targets, ebbackend.Target{
			ID:        resolve(m["Id"], params, ids),
			Arn:       resolve(m["Arn"], params, ids),
			RoleArn:   resolve(m["RoleArn"], params, ids),
			Input:     resolve(m["Input"], params, ids),
			InputPath: resolve(m["InputPath"], params, ids),
		})
	}
	if len(targets) == 0 {
		return nil
	}
	if _, err := rc.backends.EventBridge.Backend.PutTargets(ctx, rule, bus, targets); err != nil {
		return fmt.Errorf("put EventBridge targets for rule %s: %w", rule, err)
	}

	return nil
}
