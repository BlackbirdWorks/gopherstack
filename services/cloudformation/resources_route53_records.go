package cloudformation

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

// route53ZoneID resolves HostedZoneId, or HostedZoneName through the zone list.
func (rc *ResourceCreator) route53ZoneID(props map[string]any, params, physicalIDs map[string]string) (string, error) {
	if id := strProp(props, "HostedZoneId", params, physicalIDs); id != "" {
		return id, nil
	}
	name := strings.TrimSuffix(strProp(props, "HostedZoneName", params, physicalIDs), ".")
	if name == "" {
		return "", fmt.Errorf("%w: HostedZoneId or HostedZoneName is required", errMissingHostedZone)
	}
	zones, _, _, err := rc.backends.Route53.Backend.ListHostedZonesByName(name, "", route53ZoneLookupMax)
	if err != nil {
		return "", fmt.Errorf("look up hosted zone %s: %w", name, err)
	}
	for _, z := range zones {
		if strings.TrimSuffix(z.Name, ".") == name {
			return z.ID, nil
		}
	}

	return "", fmt.Errorf("%w: no hosted zone named %s", errMissingHostedZone, name)
}

const route53ZoneLookupMax = 100

var errMissingHostedZone = errors.New("hosted zone not resolvable")

// route53RecordSet converts a RecordSet / RecordSetGroup.RecordSets entry, including alias
// targets and the routing-policy members.
func route53RecordSet(m map[string]any, params, physicalIDs map[string]string) route53backend.ResourceRecordSet {
	rs := route53backend.ResourceRecordSet{
		Name:          strProp(m, "Name", params, physicalIDs),
		Type:          strProp(m, "Type", params, physicalIDs),
		SetIdentifier: strProp(m, "SetIdentifier", params, physicalIDs),
		Region:        strProp(m, "Region", params, physicalIDs),
		HealthCheckID: strProp(m, "HealthCheckId", params, physicalIDs),
		Failover:      route53backend.FailoverPolicy(strProp(m, "Failover", params, physicalIDs)),
	}
	if s := strProp(m, "TTL", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			rs.TTL = n
		}
	}
	if s := strProp(m, "Weight", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			rs.Weight = &n
		}
	}
	rs.MultiValueAnswer = strProp(m, "MultiValueAnswer", params, physicalIDs) == boolTrue
	if raw, ok := m["ResourceRecords"].([]any); ok {
		for _, r := range raw {
			if val := resolve(r, params, physicalIDs); val != "" {
				rs.Records = append(rs.Records, route53backend.ResourceRecord{Value: val})
			}
		}
	}
	if at, ok := m["AliasTarget"].(map[string]any); ok {
		rs.AliasTarget = &route53backend.AliasTarget{
			HostedZoneID:         strProp(at, "HostedZoneId", params, physicalIDs),
			DNSName:              strProp(at, "DNSName", params, physicalIDs),
			EvaluateTargetHealth: strProp(at, "EvaluateTargetHealth", params, physicalIDs) == boolTrue,
		}
	}
	if g, ok := m["GeoLocation"].(map[string]any); ok {
		rs.GeoLocation = &route53backend.GeoLocation{
			ContinentCode:   strProp(g, "ContinentCode", params, physicalIDs),
			CountryCode:     strProp(g, "CountryCode", params, physicalIDs),
			SubdivisionCode: strProp(g, "SubdivisionCode", params, physicalIDs),
		}
	}

	return rs
}

// route53RecordRef is "<name>,<type>,<setIdentifier>"; refs ride in the physical id so delete can find the records.
func route53RecordRef(rs route53backend.ResourceRecordSet) string {
	return strings.ToLower(strings.TrimSuffix(rs.Name, ".")) + "," + rs.Type + "," + rs.SetIdentifier
}

// deleteRoute53Records removes the records a RecordSet ("zone:name:type") or RecordSetGroup
// ("logical|zone|name,type,setId|...") created; the zone going away first is fine.
func (rc *ResourceCreator) deleteRoute53Records(physicalID string) error {
	if rc.backends.Route53 == nil {
		return nil
	}
	var zoneID string
	var refs []string
	if parts := strings.Split(physicalID, "|"); len(parts) >= route53GroupPhysParts {
		zoneID, refs = parts[1], parts[2:]
	} else if f := strings.SplitN(physicalID, ":", route53SetPhysParts); len(f) == route53SetPhysParts {
		zoneID, refs = f[0], []string{route53RecordRef(route53backend.ResourceRecordSet{Name: f[1], Type: f[2]})}
	} else {
		return nil
	}
	be := rc.backends.Route53.Backend
	page, err := be.ListResourceRecordSets(zoneID, "", "", "", route53DeleteListMax)
	if err != nil {
		return nil //nolint:nilerr // zone already gone: nothing left to delete
	}
	var changes []route53backend.Change
	for _, rs := range page.Records {
		if slices.Contains(refs, route53RecordRef(rs)) {
			changes = append(
				changes,
				route53backend.Change{Action: route53backend.ChangeActionDelete, ResourceRecordSet: rs},
			)
		}
	}
	if len(changes) == 0 {
		return nil
	}
	if _, err = be.ChangeResourceRecordSets(zoneID, changes); err != nil {
		return fmt.Errorf("delete Route53 records: %w", err)
	}

	return nil
}

const (
	route53GroupPhysParts = 3
	route53SetPhysParts   = 3
	route53DeleteListMax  = 1000
)
