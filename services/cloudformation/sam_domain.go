package cloudformation

import (
	"regexp"
	"slices"
	"strings"
)

const (
	samTypeV2Domain      = "AWS::ApiGatewayV2::DomainName"
	samTypeV2Mapping     = "AWS::ApiGatewayV2::ApiMapping"
	samTypeV1Domain      = "AWS::ApiGateway::DomainName"
	samTypeV1Mapping     = "AWS::ApiGateway::BasePathMapping"
	samTypeV1DomainV2    = "AWS::ApiGateway::DomainNameV2"
	samTypeV1MappingV2   = "AWS::ApiGateway::BasePathMappingV2"
	samTypeDomainAssoc   = "AWS::ApiGateway::DomainNameAccessAssociation"
	samEndpointPrivate   = "PRIVATE"
	samTypeRecordGroup   = "AWS::Route53::RecordSetGroup"
	samKeyEndpointConfig = "EndpointConfiguration"
	samEndpointRegional  = "REGIONAL"
	samEndpointEdge      = "EDGE"
	samCloudFrontZone    = "Z2FDTNDATAQYW2"
)

var samInvalidBasePath = regexp.MustCompile(`[^0-9a-zA-Z/\-_]+`)

func samDomainBasePaths(domain map[string]any) []any {
	switch b := domain["BasePath"].(type) {
	case string:
		if b != "" {
			return []any{b}
		}
	case []any:
		if len(b) > 0 {
			return b
		}
	}

	return nil
}

// samDomainConfig validates the Domain block shared by Api and HttpApi.
func samDomainConfig(id string, raw any) (map[string]any, error) {
	domain := asMap(raw)
	if domain == nil {
		return nil, samErr(id, "Domain must be a map.")
	}
	if domain[samKeyDomainName] == nil || domain["CertificateArn"] == nil {
		return nil, samErr(id, "Custom Domains only works if both DomainName and CertificateArn are provided.")
	}
	if mtls := domain["MutualTlsAuthentication"]; mtls != nil {
		m := asMap(mtls)
		if m == nil {
			return nil, samErr(id, "MutualTlsAuthentication must be a map with at least one of the following "+
				"fields [TruststoreUri TruststoreVersion].")
		}
		if err := rejectUnknown(id, m, keySet("TruststoreUri", "TruststoreVersion")); err != nil {
			return nil, err
		}
	}

	return domain, nil
}

func mutualTLS(domain map[string]any) map[string]any {
	m := asMap(domain["MutualTlsAuthentication"])
	if m == nil {
		return nil
	}
	out := map[string]any{}
	for _, k := range []string{"TruststoreUri", "TruststoreVersion"} {
		if v := m[k]; v != nil && v != "" && v != 0.0 {
			out[k] = v
		}
	}

	return out
}

func (t *samTranslator) emitHTTPDomain(a *samHTTPAPI) error {
	return t.emitV2Domain(a.id, a.props[samDomainKey], a.v2Tags(), true)
}

// emitV2Domain writes the ApiGatewayV2 custom domain, API mappings and Route53 records of an HTTP or WebSocket API.
func (t *samTranslator) emitV2Domain(apiID string, raw any, tags map[string]any, merge bool) error {
	if raw == nil {
		return nil
	}
	domain, err := samDomainConfig(apiID, t.resolveParamRefs(raw))
	if err != nil {
		return err
	}
	endpoint := samEndpointRegional
	if e, ok := domain[samKeyEndpointConfig]; ok {
		if endpoint, _ = t.paramDefault(e).(string); endpoint != samEndpointRegional {
			return samErr(apiID, "EndpointConfiguration for Custom Domains must be one of [REGIONAL].")
		}
	}
	domainID := "ApiGatewayDomainNameV2" + samLogicalID("", domain[samKeyDomainName])
	cfg := map[string]any{"CertificateArn": domain["CertificateArn"], "EndpointType": endpoint}
	for _, k := range []string{"OwnershipVerificationCertificateArn", samKSecurityPolicy} {
		if domain[k] != nil {
			cfg[k] = domain[k]
		}
	}
	props := map[string]any{
		samKeyDomainName: domain[samKeyDomainName], "DomainNameConfigurations": []any{cfg},
	}
	if tags != nil {
		props[samKeyTags] = tags
	}
	if m := mutualTLS(domain); m != nil {
		props["MutualTlsAuthentication"] = m
	}
	if err = t.put(domainID, map[string]any{samKeyType: samTypeV2Domain, samKeyProps: props}); err != nil {
		return err
	}
	t.refs[apiID+".DomainName"] = domainID
	if err = t.putHTTPMappings(apiID, domain, domainID); err != nil {
		return err
	}

	return t.putRecordSets(apiID, domain, domainID, endpoint, merge)
}

func (t *samTranslator) putHTTPMappings(apiID string, domain map[string]any, domainID string) error {
	paths := samDomainBasePaths(domain)
	if paths == nil {
		return t.put(apiID+"ApiMapping", httpMapping(apiID, domainID, ""))
	}
	for _, p := range paths {
		path, ok := p.(string)
		if !ok {
			return samErr(apiID, "Basepath must be a string.")
		}
		if samInvalidBasePath.MatchString(path) {
			return samErr(apiID, "Invalid Basepath name provided.")
		}
		id := apiID + strings.NewReplacer("-", "", "_", "", "/", "").Replace(path) + "ApiMapping"
		if err := t.put(id, httpMapping(apiID, domainID, strings.Trim(path, "/"))); err != nil {
			return err
		}
	}

	return nil
}

func httpMapping(apiID, domainID, key string) map[string]any {
	props := map[string]any{samKeyDomainName: samRef(domainID), samKeyAPIID: samRef(apiID),
		samDeploymentStub: samRef(apiID + ".Stage")}
	if key != "" {
		props["ApiMappingKey"] = key
	}

	return map[string]any{samKeyType: samTypeV2Mapping, samKeyProps: props}
}

// putRecordSets adds the Route53 alias records for a Domain.Route53 block, merging into one
// RecordSetGroup per hosted zone like SAM does.
func (t *samTranslator) putRecordSets(apiID string, domain map[string]any, domainID, endpoint string, merge bool) error {
	r53 := domain["Route53"]
	if r53 == nil {
		return nil
	}
	cfg := asMap(r53)
	if cfg == nil {
		return samErr(apiID, "Domain.Route53 must be a map.")
	}
	zone := cfg["HostedZoneId"]
	if zone == nil {
		zone = cfg["HostedZoneName"]
	}
	if zone == nil {
		return samErr(apiID, "HostedZoneId or HostedZoneName is required to enable Route53 support on Custom Domains.")
	}
	if v, has := cfg["SeparateRecordSetGroup"]; has && v != nil {
		if _, isBool := v.(bool); !isBool {
			return samErr(apiID, "Property 'Domain.Route53.SeparateRecordSetGroup' should be a boolean.")
		}
	}
	records := recordSetsFor(domain, cfg, domainID, endpoint)
	groupID := "RecordSetGroup" + samLogicalID("", zone)
	if cfg["SeparateRecordSetGroup"] == true {
		groupID = samLogicalID("RecordSetGroup", []any{zone, domain[samKeyDomainName]})

		return t.put(groupID, recordGroup(cfg, records))
	}
	if existing := asMap(t.out[groupID]); existing != nil && merge {
		props := asMap(existing[samKeyProps])
		props["RecordSets"] = slices.Concat(asList(props["RecordSets"]), records)

		return nil
	}

	return t.put(groupID, recordGroup(cfg, records))
}

func recordGroup(cfg map[string]any, records []any) map[string]any {
	props := map[string]any{"RecordSets": records}
	for _, k := range []string{"HostedZoneId", "HostedZoneName"} {
		if v, ok := cfg[k]; ok {
			props[k] = v
		}
	}

	return map[string]any{samKeyType: samTypeRecordGroup, samKeyProps: props}
}

func recordSetsFor(domain, cfg map[string]any, domainID, endpoint string) []any {
	alias := map[string]any{}
	if v := cfg["EvaluateTargetHealth"]; v != nil {
		alias["EvaluateTargetHealth"] = v
	}
	switch endpoint {
	case samEndpointRegional:
		alias["HostedZoneId"] = samGetAtt(domainID, "RegionalHostedZoneId")
		alias["DNSName"] = samGetAtt(domainID, "RegionalDomainName")
	case samEndpointEdge:
		dist := cfg["DistributionDomainName"]
		if dist == nil {
			dist = samGetAtt(domainID, "DistributionDomainName")
		}
		alias["HostedZoneId"], alias["DNSName"] = samCloudFrontZone, dist
	default:
		alias["HostedZoneId"], alias["DNSName"] = cfg["VpcEndpointHostedZoneId"], cfg["VpcEndpointDomainName"]
	}
	types := []string{"A"}
	if cfg["IpV6"] == true {
		types = append(types, "AAAA")
	}
	out := make([]any, 0, len(types))
	for _, typ := range types {
		rec := map[string]any{attrNameName: domain[samKeyDomainName], samKeyType: typ, "AliasTarget": alias}
		for _, k := range []string{"Region", "SetIdentifier"} {
			if cfg[k] != nil {
				rec[k] = cfg[k]
			}
		}
		out = append(out, rec)
	}

	return out
}

func (t *samTranslator) emitRESTDomain(a *samAPI) error {
	if a.props[samDomainKey] == nil {
		return nil
	}
	domain, err := samDomainConfig(a.id, t.resolveParamRefs(a.props[samDomainKey]))
	if err != nil {
		return err
	}
	endpoint := samEndpointRegional
	if e, ok := domain[samKeyEndpointConfig]; ok {
		endpoint, _ = t.paramDefault(e).(string)
	}
	switch endpoint {
	case samEndpointRegional, samEndpointEdge:
	case samEndpointPrivate:
		return t.emitPrivateRESTDomain(a, domain)
	default:
		return samErr(a.id, "EndpointConfiguration for Custom Domains must be one of [EDGE REGIONAL PRIVATE].")
	}
	domainID := "ApiGatewayDomainName" + samLogicalID("", domain[samKeyDomainName])
	cfg := map[string]any{samKTypes: []any{endpoint}}
	if v := domain["IpAddressType"]; v != nil {
		cfg["IpAddressType"] = v
	}
	props := map[string]any{samKeyDomainName: domain[samKeyDomainName], samKeyEndpointConfig: cfg}
	if endpoint == samEndpointRegional {
		props["RegionalCertificateArn"] = domain["CertificateArn"]
	} else {
		props["CertificateArn"] = domain["CertificateArn"]
	}
	if m := mutualTLS(domain); m != nil {
		props["MutualTlsAuthentication"] = m
	}
	optional := []string{samKSecurityPolicy, "EndpointAccessMode", samKPolicy, "OwnershipVerificationCertificateArn"}
	for _, k := range optional {
		if domain[k] != nil {
			props[k] = domain[k]
		}
	}
	if err = t.put(domainID, map[string]any{samKeyType: samTypeV1Domain, samKeyProps: props}); err != nil {
		return err
	}
	if err = t.putRESTMappings(a, domain, domainID); err != nil {
		return err
	}

	return t.putRecordSets(a.id, domain, domainID, endpoint, true)
}

func (t *samTranslator) emitPrivateRESTDomain(a *samAPI, domain map[string]any) error {
	assoc := domain["AccessAssociation"]
	if assoc != nil && asMap(assoc) == nil {
		return samErr(a.id, "Property 'Domain.AccessAssociation' should be a map.")
	}
	domainID := "ApiGatewayDomainNameV2" + samLogicalID("", domain[samKeyDomainName])
	cfg := map[string]any{samKTypes: []any{samEndpointPrivate}}
	if v := domain["IpAddressType"]; v != nil {
		cfg["IpAddressType"] = v
	}
	props := map[string]any{
		samKeyDomainName: domain[samKeyDomainName], samKeyEndpointConfig: cfg,
		"CertificateArn": domain["CertificateArn"],
	}
	for _, k := range []string{samKSecurityPolicy, "EndpointAccessMode", samKPolicy} {
		if domain[k] != nil {
			props[k] = domain[k]
		}
	}
	if err := t.put(domainID, map[string]any{samKeyType: samTypeV1DomainV2, samKeyProps: props}); err != nil {
		return err
	}
	t.refs[a.id+".DomainNameV2"] = domainID
	if assoc != nil {
		source := asMap(assoc)["VpcEndpointId"]
		if source == nil {
			return samErr(a.id, "Property 'Domain.AccessAssociation.VpcEndpointId' is required.")
		}
		assocID := samLogicalID("DomainNameAccessAssociation", []any{domain[samKeyDomainName], source})
		if err := t.put(assocID, map[string]any{samKeyType: samTypeDomainAssoc, samKeyProps: map[string]any{
			"AccessAssociationSource": source, "AccessAssociationSourceType": "VPCE",
			"DomainNameArn": samRef(domainID),
		}}); err != nil {
			return err
		}
	}
	if err := t.putRESTMappings(a, domain, domainID); err != nil {
		return err
	}

	return t.putRecordSets(a.id, domain, domainID, samEndpointPrivate, true)
}

func (t *samTranslator) putRESTMappings(a *samAPI, domain map[string]any, domainID string) error {
	mapping := func(id string, base *string) error {
		props := map[string]any{samKeyDomainName: samRef(domainID), samKRestAPIID: samRef(a.id),
			samDeploymentStub: samRef(a.id + ".Stage")}
		typ := samTypeV1Mapping
		if strings.HasPrefix(domainID, "ApiGatewayDomainNameV2") {
			typ = samTypeV1MappingV2
			props["DomainNameArn"] = props[samKeyDomainName]
			delete(props, samKeyDomainName)
		}
		if base != nil {
			props["BasePath"] = *base
		}

		return t.put(id, map[string]any{samKeyType: typ, samKeyProps: props})
	}
	paths := samDomainBasePaths(domain)
	if paths == nil {
		return mapping(a.id+"BasePathMapping", nil)
	}
	for _, p := range paths {
		path, ok := p.(string)
		if !ok {
			return samErr(a.id, "Domain.BasePath must be a list of strings.")
		}
		var alnum strings.Builder
		for _, r := range path {
			if isAlnum(string(r)) {
				alnum.WriteRune(r)
			}
		}
		base := alnum.String()
		if domain["NormalizeBasePath"] == false {
			base = path
		}
		if err := mapping(a.id+alnum.String()+"BasePathMapping", &base); err != nil {
			return err
		}
	}

	return nil
}
