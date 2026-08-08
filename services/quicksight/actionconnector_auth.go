package quicksight

import "maps"

// redactAuthenticationConfig projects the stored write-side AuthenticationConfig
// (types.AuthConfig wire shape) onto the read-side ReadAuthConfig shape AWS
// returns from DescribeActionConnector: aws-sdk-go-v2/service/quicksight@v1.123.1
// types/types.go:16760, deserializers.go:109643. connectorArn fills
// ReadIamConnectionMetadata.SourceArn, a field the write side has no equivalent
// for (types.go:16991).
func redactAuthenticationConfig(cfg map[string]any, connectorArn string) map[string]any {
	if cfg == nil {
		return nil
	}

	out := map[string]any{}
	if authType, ok := cfg["AuthenticationType"]; ok {
		out["AuthenticationType"] = authType
	}

	if meta, ok := cfg["AuthenticationMetadata"].(map[string]any); ok {
		out["AuthenticationMetadata"] = redactAuthenticationMetadata(meta, connectorArn)
	}

	return out
}

func redactAuthenticationMetadata(meta map[string]any, connectorArn string) map[string]any {
	out := make(map[string]any, len(meta))
	for kind, val := range meta {
		sub, ok := val.(map[string]any)
		if !ok {
			out[kind] = val

			continue
		}
		out[kind] = redactAuthMetadataMember(kind, sub, connectorArn)
	}

	return out
}

func redactAuthMetadataMember(kind string, sub map[string]any, connectorArn string) map[string]any {
	switch kind {
	case "ApiKeyConnectionMetadata":
		return withoutKeys(sub, "ApiKey")
	case "BasicAuthConnectionMetadata":
		return withoutKeys(sub, "Password")
	case "IamConnectionMetadata":
		return withSourceArn(sub, connectorArn)
	case "ClientCredentialsGrantMetadata":
		return redactClientCredentialsGrantMetadata(sub)
	case "AuthorizationCodeGrantMetadata":
		return redactAuthorizationCodeGrantMetadata(sub)
	default:
		return sub
	}
}

// redactClientCredentialsGrantMetadata drops ClientSecret and renames the
// nested details wrapper/member to the read-side names (deserializers.go:110097,
// :110007..110016 -- ClientCredentialsDetails/ClientCredentialsGrantDetails on
// write become ReadClientCredentialsDetails/ReadClientCredentialsGrantDetails
// on read).
func redactClientCredentialsGrantMetadata(m map[string]any) map[string]any {
	out := withoutKeys(m, "ClientCredentialsDetails")

	details, ok := m["ClientCredentialsDetails"].(map[string]any)
	if !ok {
		return out
	}
	grant, ok := details["ClientCredentialsGrantDetails"].(map[string]any)
	if !ok {
		return out
	}

	out["ReadClientCredentialsDetails"] = map[string]any{
		"ReadClientCredentialsGrantDetails": withoutKeys(grant, "ClientSecret"),
	}

	return out
}

// redactAuthorizationCodeGrantMetadata drops ClientSecret and renames the
// nested details wrapper/member to the read-side names (deserializers.go:109798,
// :109907..109920 -- AuthorizationCodeGrantCredentialsDetails/
// AuthorizationCodeGrantDetails on write become
// ReadAuthorizationCodeGrantCredentialsDetails/ReadAuthorizationCodeGrantDetails
// on read).
func redactAuthorizationCodeGrantMetadata(m map[string]any) map[string]any {
	out := withoutKeys(m, "AuthorizationCodeGrantCredentialsDetails")

	details, ok := m["AuthorizationCodeGrantCredentialsDetails"].(map[string]any)
	if !ok {
		return out
	}
	grant, ok := details["AuthorizationCodeGrantDetails"].(map[string]any)
	if !ok {
		return out
	}

	out["ReadAuthorizationCodeGrantCredentialsDetails"] = map[string]any{
		"ReadAuthorizationCodeGrantDetails": withoutKeys(grant, "ClientSecret"),
	}

	return out
}

func withoutKeys(m map[string]any, keys ...string) map[string]any {
	out := make(map[string]any, len(m))
	maps.Copy(out, m)
	for _, k := range keys {
		delete(out, k)
	}

	return out
}

func withSourceArn(m map[string]any, connectorArn string) map[string]any {
	out := make(map[string]any, len(m)+1)
	maps.Copy(out, m)
	out["SourceArn"] = connectorArn

	return out
}
