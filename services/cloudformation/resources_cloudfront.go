package cloudformation

import "fmt"

// createCloudFrontResource handles CloudFront resource types not covered by the core
// resource dispatch chain (Function, CachePolicy, OriginAccessControl, ResponseHeadersPolicy).
func (rc *ResourceCreator) createCloudFrontResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::CloudFront::Function":
		id, err := rc.createCloudFrontFunction(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::CachePolicy":
		id, err := rc.createCloudFrontCachePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::OriginAccessControl":
		id, err := rc.createCloudFrontOriginAccessControl(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::CloudFront::ResponseHeadersPolicy":
		id, err := rc.createCloudFrontResponseHeadersPolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteCloudFrontResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::CloudFront::Function":

		return true, rc.deleteCloudFrontFunction(physicalID)
	case "AWS::CloudFront::CachePolicy":

		return true, rc.deleteCloudFrontCachePolicy(physicalID)
	case "AWS::CloudFront::OriginAccessControl":

		return true, rc.deleteCloudFrontOriginAccessControl(physicalID)
	case "AWS::CloudFront::ResponseHeadersPolicy":

		return true, rc.deleteCloudFrontResponseHeadersPolicy(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createCloudFrontFunction(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	code := strProp(props, "FunctionCode", params, physicalIDs)
	if code == "" {
		code = "function handler(event) { return event.request; }"
	}

	runtime := functionRuntime(props, params, physicalIDs)

	fn, err := rc.backends.CloudFront.Backend.CreateFunction(name, "", runtime, code)
	if err != nil {
		return "", fmt.Errorf("create CloudFront function %s: %w", name, err)
	}

	return fn.Name, nil
}

func functionRuntime(props map[string]any, params, physicalIDs map[string]string) string {
	if cfg, ok := props["FunctionConfig"].(map[string]any); ok {
		if rt := resolve(cfg["Runtime"], params, physicalIDs); rt != "" {
			return rt
		}
	}
	if rt := strProp(props, "Runtime", params, physicalIDs); rt != "" {
		return rt
	}

	return "cloudfront-js-2.0"
}

func (rc *ResourceCreator) deleteCloudFrontFunction(name string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteFunction(name)
}

func (rc *ResourceCreator) createCloudFrontCachePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg := cachePolicyConfig(logicalID, props, params, physicalIDs)

	policy, err := rc.backends.CloudFront.Backend.CreateCachePolicy(
		cfg.name, "", cfg.defaultTTL, cfg.maxTTL, cfg.minTTL,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront cache policy %s: %w", cfg.name, err)
	}

	return policy.ID, nil
}

type cachePolicySettings struct {
	name                       string
	defaultTTL, maxTTL, minTTL int64
}

func cachePolicyConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) cachePolicySettings {
	const (
		fallbackDefaultTTL = 86400
		fallbackMaxTTL     = 31536000
	)
	settings := cachePolicySettings{name: logicalID, defaultTTL: fallbackDefaultTTL, maxTTL: fallbackMaxTTL}

	cfg, ok := props["CachePolicyConfig"].(map[string]any)
	if !ok {
		return settings
	}
	if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
		settings.name = n
	}
	if v := int64Val(cfg["DefaultTTL"]); v != 0 {
		settings.defaultTTL = v
	}
	if v := int64Val(cfg["MaxTTL"]); v != 0 {
		settings.maxTTL = v
	}
	settings.minTTL = int64Val(cfg["MinTTL"])

	return settings
}

func (rc *ResourceCreator) deleteCloudFrontCachePolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteCachePolicy(id)
}

func (rc *ResourceCreator) createCloudFrontOriginAccessControl(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg := oacConfig(logicalID, props, params, physicalIDs)

	oac, err := rc.backends.CloudFront.Backend.CreateOriginAccessControl(
		cfg.name, "", cfg.originType, cfg.signingBehavior, cfg.signingProtocol,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront origin access control %s: %w", cfg.name, err)
	}

	return oac.ID, nil
}

type oacSettings struct {
	name            string
	originType      string
	signingBehavior string
	signingProtocol string
}

func oacConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) oacSettings {
	settings := oacSettings{name: logicalID, originType: "s3", signingBehavior: "always", signingProtocol: "sigv4"}

	cfg, ok := props["OriginAccessControlConfig"].(map[string]any)
	if !ok {
		return settings
	}
	if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
		settings.name = n
	}
	if v := resolve(cfg["OriginAccessControlOriginType"], params, physicalIDs); v != "" {
		settings.originType = v
	}
	if v := resolve(cfg["SigningBehavior"], params, physicalIDs); v != "" {
		settings.signingBehavior = v
	}
	if v := resolve(cfg["SigningProtocol"], params, physicalIDs); v != "" {
		settings.signingProtocol = v
	}

	return settings
}

func (rc *ResourceCreator) deleteCloudFrontOriginAccessControl(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteOriginAccessControl(id)
}

func (rc *ResourceCreator) createCloudFrontResponseHeadersPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID
	if cfg, ok := props["ResponseHeadersPolicyConfig"].(map[string]any); ok {
		if n := resolve(cfg["Name"], params, physicalIDs); n != "" {
			name = n
		}
	}

	policy, err := rc.backends.CloudFront.Backend.CreateResponseHeadersPolicy(name, "")
	if err != nil {
		return "", fmt.Errorf("create CloudFront response headers policy %s: %w", name, err)
	}

	return policy.ID, nil
}

func (rc *ResourceCreator) deleteCloudFrontResponseHeadersPolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	return rc.backends.CloudFront.Backend.DeleteResponseHeadersPolicy(id)
}

// ---- CloudFront Distribution ----

func (rc *ResourceCreator) createCloudFrontDistribution(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	comment := logicalID
	enabled := true

	if cfg, ok := props["DistributionConfig"].(map[string]any); ok {
		if c := resolve(cfg["Comment"], params, physicalIDs); c != "" {
			comment = c
		}

		if e, ok2 := cfg["Enabled"].(bool); ok2 {
			enabled = e
		}
	}

	dist, err := rc.backends.CloudFront.Backend.CreateDistribution(logicalID, comment, enabled, nil)
	if err != nil {
		return "", fmt.Errorf("create CloudFront distribution: %w", err)
	}

	return dist.ARN, nil
}

func (rc *ResourceCreator) deleteCloudFrontDistribution(arn string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	// CloudFront requires a distribution to be disabled before deletion
	// (matching real AWS). Disable it first, preserving its existing config.
	if dist, err := rc.backends.CloudFront.Backend.GetDistribution(id); err == nil {
		if _, uerr := rc.backends.CloudFront.Backend.UpdateDistribution(
			id, dist.Comment, false, dist.RawConfig,
		); uerr != nil {
			return uerr
		}
	}

	return rc.backends.CloudFront.Backend.DeleteDistribution(id)
}
