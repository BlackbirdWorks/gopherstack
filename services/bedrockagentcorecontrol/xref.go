package bedrockagentcorecontrol

import (
	"context"
	"strings"
)

// RoleChecker is the optional References extension that verifies IAM roles exist.
type RoleChecker interface {
	RoleExists(roleARN string) bool
}

func collectRoleARNs(v any, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, m := range t {
			if s, ok := m.(string); ok && (k == "roleArn" || strings.HasSuffix(k, "RoleArn")) && s != "" {
				*out = append(*out, s)

				continue
			}

			collectRoleARNs(m, out)
		}
	case []any:
		for _, m := range t {
			collectRoleARNs(m, out)
		}
	}
}

// ObjectChecker is the optional References extension that tests an S3 object exists without reading it.
type ObjectChecker interface {
	ObjectExists(bucket, key string) bool
}

// checkCodeObject rejects a direct-code runtime whose S3 package does not exist.
func (b *InMemoryBackend) checkCodeObject(in args) error {
	s3 := objOf(objOf(objOf(in.obj("agentRuntimeArtifact"), "codeConfiguration"), "code"), "s3")
	if s3 == nil {
		return nil
	}

	b.mu.RLock("checkCodeObject")
	oc, ok := b.refs.(ObjectChecker)
	b.mu.RUnlock()

	if ok && !oc.ObjectExists(strOf(s3, "bucket"), strOf(s3, "prefix")) {
		return invalid("code package s3://%s/%s does not exist", strOf(s3, "bucket"), strOf(s3, "prefix"))
	}

	return nil
}

// checkRoles rejects a request naming an IAM role the emulated IAM does not hold.
func (b *InMemoryBackend) checkRoles(in args) error {
	b.mu.RLock("checkRoles")
	rc, ok := b.refs.(RoleChecker)
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	var arns []string

	collectRoleARNs(map[string]any(in), &arns)

	for _, r := range arns {
		if !rc.RoleExists(r) {
			return invalid("IAM role %q does not exist", r)
		}
	}

	return nil
}

func (b *InMemoryBackend) hasRefs() bool {
	b.mu.RLock("hasRefs")
	defer b.mu.RUnlock()

	return b.refs != nil
}

func usesS3(cfg map[string]any, keys ...string) bool {
	if objOf(cfg, "s3") != nil {
		return true
	}

	for _, k := range keys {
		if objOf(objOf(cfg, k), "s3") != nil {
			return true
		}
	}

	return false
}

// checkTargetSchemas reads the schema a gateway target points at (inline or in S3) and rejects one the gateway
// could not serve tools from, so a malformed schema fails the write instead of the first tools/list.
func (b *InMemoryBackend) checkTargetSchemas(in args) error {
	kind, cfg := mcpKind(in.obj("targetConfiguration"))

	names, err := b.schemaToolNames(kind, cfg)
	if err != nil {
		return invalid("%s target schema is unusable: %v", kind, err)
	}

	return checkToolLimits(names)
}

func (b *InMemoryBackend) schemaToolNames(kind string, cfg map[string]any) ([]string, error) {
	switch kind {
	case targetLambda:
		if usesS3(cfg, "toolSchema") && !b.hasRefs() {
			return nil, nil
		}

		defs, err := b.lambdaToolDefs(cfg)
		names := make([]string, 0, len(defs))

		for _, d := range defs {
			names = append(names, strOf(mapOf(d), keyName))
		}

		return names, err
	case targetOpenAPI:
		if usesS3(cfg) && !b.hasRefs() {
			return nil, nil
		}

		ops, err := b.openAPIOps(cfg)
		names := make([]string, 0, len(ops))

		for _, o := range ops {
			names = append(names, o.id)
		}

		return names, err
	case targetMCPSrv:
		return b.staticToolNames(objOf(cfg, "mcpToolSchema"))
	default:
		return nil, nil
	}
}

func (b *InMemoryBackend) staticToolNames(static map[string]any) ([]string, error) {
	if static == nil || (usesS3(static) && !b.hasRefs()) {
		return nil, nil
	}

	entries, err := b.staticMCPTools(static)
	names := make([]string, 0, len(entries))

	for _, e := range entries {
		names = append(names, e.name)
	}

	return names, err
}

func mcpKind(tc map[string]any) (string, map[string]any) {
	mcp := objOf(tc, "mcp")

	for _, k := range mcpTargetTypes() {
		if v := objOf(mcp, k); v != nil {
			return k, v
		}
	}

	return "", nil
}

// crossChecks verifies the references a request makes to resources of other services before the backend lock.
func (b *InMemoryBackend) crossChecks(ctx context.Context, op string, in args) error {
	if err := b.checkKeys(ctx, in); err != nil {
		return err
	}

	if err := b.checkRoles(in); err != nil {
		return err
	}

	switch op {
	case opCreateRuntime, opUpdateRuntime:
		return b.checkCodeObject(in)
	case opCreateTarget, opUpdateTarget:
		return b.checkTargetSchemas(in)
	default:
		return nil
	}
}
