package azurearm

import (
	"maps"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

func (p *FlexDBProvider) role(id ResourceID, rec *flexServer) string {
	key := p.key(id)

	switch {
	case rec.CreateMode == flexModeReplica && rec.SourceID != "":
		return p.flavor.roleReplica
	case len(p.replicasOf(key)) > 0:
		return p.flavor.roleSource
	}

	return flexRoleNone
}

func (p *FlexDBProvider) skuMaxIops(rec *flexServer) int {
	if s, ok := p.flavor.skuByName(rec.SKUName); ok && s.maxIops > 0 {
		return s.maxIops
	}

	return mysqlMaxIopsAll
}

func (p *FlexDBProvider) maxConnections(rec *flexServer) string {
	if s, ok := p.flavor.skuByName(rec.SKUName); ok && s.maxConn > 0 {
		return strconv.Itoa(s.maxConn)
	}

	return ""
}

func (p *FlexDBProvider) fqdn(rec *flexServer) string {
	if rec.Subnet != "" && rec.DNSZone != "" {
		if i := strings.LastIndex(rec.DNSZone, "/"); i >= 0 {
			return rec.Name + "." + rec.DNSZone[i+1:]
		}
	}

	return rec.Name + p.flavor.hostSuffix
}

func (p *FlexDBProvider) serverBody(id ResourceID, rec *flexServer) map[string]any {
	f := p.flavor
	haState := "NotEnabled"

	if rec.HAMode != flexHADisabled {
		haState = "Healthy"
	}

	ha := map[string]any{"mode": rec.HAMode, auxState: haState}
	if rec.StandbyZone != "" {
		ha["standbyAvailabilityZone"] = rec.StandbyZone
	}

	props := map[string]any{
		auxState: p.stateOf(rec), sqlPropVersion: rec.Version, flexPropFQDN: p.fqdn(rec),
		"administratorLogin": rec.Login, "storage": p.storageBody(rec), "backup": p.backupBody(rec),
		"network": p.networkBody(rec), "highAvailability": ha,
		"createMode": rec.CreateMode, "replicationRole": p.role(id, rec), "replicaCapacity": f.maxReplicas,
		"maintenanceWindow": p.maintenanceBody(rec),
	}

	if rec.Zone != "" {
		props["availabilityZone"] = rec.Zone
	}

	if rec.SourceID != "" {
		props["sourceServerResourceId"] = rec.SourceID
	}

	if !rec.PointInTime.IsZero() {
		props[f.sourceTimeField] = rec.PointInTime.Format("2006-01-02T15:04:05.000Z")
	}

	p.kindProps(id, props, rec)

	if ep, ok := p.endpointOf(id, rec); ok {
		props["gopherstackEndpoint"] = ep
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.Name, fieldType: f.ns + "/" + flexServersType,
		fieldLocation: rec.Location, fieldTags: tagsOrEmpty(rec.Tags), fieldProperties: props,
		acrFieldSKU: map[string]any{fieldName: rec.SKUName, aksFieldTier: rec.SKUTier},
		"systemData": map[string]any{
			"createdAt": rec.CreatedAt.Format("2006-01-02T15:04:05.0000000Z"),
		},
	}

	if len(rec.Identity) > 0 {
		out["identity"] = redisIdentity(id, rec.Identity)
	}

	return out
}

func (p *FlexDBProvider) kindProps(id ResourceID, props map[string]any, rec *flexServer) {
	if p.flavor.kind == flexKindPostgres {
		props["authConfig"] = mergeFlexMap(
			map[string]any{"activeDirectoryAuth": flexDisabled, "passwordAuth": flexEnabled},
			rec.AuthConfig,
		)
		props["dataEncryption"] = mergeFlexMap(map[string]any{fieldType: "SystemManaged"}, rec.Encryption)
		props["replica"] = map[string]any{"role": p.role(id, rec), cmpFieldCapacity: p.flavor.maxReplicas}

		return
	}

	props["databasePort"] = 3306
	props["privateEndpointConnections"] = []any{}

	if len(rec.Encryption) > 0 {
		props["dataEncryption"] = rec.Encryption
	}
}

func mergeFlexMap(base, over map[string]any) map[string]any {
	maps.Copy(base, over)

	return base
}

func (p *FlexDBProvider) storageBody(rec *flexServer) map[string]any {
	if p.flavor.kind == flexKindPostgres {
		tier, iops := pgStorageTier(rec.StorageGB)

		return map[string]any{
			"storageSizeGB": rec.StorageGB, "autoGrow": rec.AutoGrow, aksFieldTier: tier, "iops": iops,
		}
	}

	iops := rec.Iops
	if iops == 0 {
		iops = mysqlIops(rec.StorageGB, p.skuMaxIops(rec))
	}

	return map[string]any{
		"storageSizeGB": rec.StorageGB, "iops": iops, "autoGrow": rec.AutoGrow, "autoIoScaling": flexDisabled,
		"logOnDisk": flexDisabled, "storageSku": "Premium_LRS", "storageRedundancy": "LocalRedundancy",
	}
}

func (p *FlexDBProvider) backupBody(rec *flexServer) map[string]any {
	earliest := rec.CreatedAt
	out := map[string]any{
		"backupRetentionDays": rec.Retention, "geoRedundantBackup": rec.GeoBackup,
		"earliestRestoreDate": earliest.Format("2006-01-02T15:04:05.0000000Z"),
	}

	if p.flavor.kind == flexKindMySQL {
		out["backupIntervalHours"] = flexBackupHours
	}

	return out
}

func (p *FlexDBProvider) networkBody(rec *flexServer) map[string]any {
	out := map[string]any{cosmosFieldPublicNetwork: rec.PublicAcc}

	if rec.Subnet != "" {
		out["delegatedSubnetResourceId"] = rec.Subnet
	}

	if rec.DNSZone != "" {
		key := "privateDnsZoneArmResourceId"
		if p.flavor.kind == flexKindMySQL {
			key = "privateDnsZoneResourceId"
		}

		out[key] = rec.DNSZone
	}

	return out
}

func (p *FlexDBProvider) maintenanceBody(rec *flexServer) map[string]any {
	return mergeFlexMap(map[string]any{
		"customWindow": flexDisabled, "dayOfWeek": 0, "startHour": 0, "startMinute": 0,
	}, rec.Maintenance)
}

func (p *FlexDBProvider) endpointOf(id ResourceID, rec *flexServer) (string, bool) {
	if p.engine == nil {
		return "", false
	}

	ep, ok := p.engine.Endpoint(p.key(ResourceID{ResourceGroup: rec.ResourceGrp, SubscriptionID: id.SubscriptionID,
		Namespace: p.flavor.ns, Names: []string{rec.Name}}))
	if !ok || ep.Port == 0 {
		return "", false
	}

	return net.JoinHostPort(ep.Host, strconv.Itoa(ep.Port)), true
}

func (p *FlexDBProvider) childEnvelope(id ResourceID, typ string, props map[string]any) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: id.LeafName(), fieldType: p.flavor.ns + "/" + flexServersType + "/" + typ,
		fieldProperties: props,
	}
}

func (p *FlexDBProvider) firewallBody(id ResourceID, fw flexFirewall) map[string]any {
	rid := id
	rid.Names = []string{id.Names[0], fw.Name}

	return p.childEnvelope(rid, flexFirewallType, map[string]any{
		"startIpAddress": fw.Start, "endIpAddress": fw.End,
	})
}

func (p *FlexDBProvider) databaseBody(id ResourceID, db flexDatabase) map[string]any {
	rid := id
	rid.Names = []string{id.Names[0], db.Name}

	return p.childEnvelope(
		rid,
		flexDatabasesType,
		map[string]any{"charset": db.Charset, flexPropCollation: db.Collation},
	)
}

// ListChildren returns the firewall rules, databases, configurations or replicas of a server.
func (p *FlexDBProvider) ListChildren(id ResourceID, collection string) ([]map[string]any, error) {
	p.mu.RLock("ListChildren")
	defer p.mu.RUnlock()

	rec, err := p.server(id)
	if err != nil {
		return nil, err
	}

	out := []map[string]any{}

	switch strings.ToLower(collection) {
	case strings.ToLower(flexFirewallType):
		for _, fw := range rec.Firewall {
			out = append(out, p.firewallBody(id, fw))
		}
	case strings.ToLower(flexDatabasesType):
		for _, db := range rec.Databases {
			out = append(out, p.databaseBody(id, db))
		}
	case strings.ToLower(flexConfigsType):
		for _, c := range p.flavor.configs {
			cid := id
			cid.Types, cid.Names = []string{flexServersType, flexConfigsType}, []string{id.Names[0], c.name}
			out = append(out, p.configEnvelope(cid, rec, c))
		}
	case strings.ToLower(flexReplicasType):
		for _, r := range p.replicasOf(p.key(id)) {
			out = append(out, p.serverBody(p.recID(id.SubscriptionID, r), r))
		}
	default:
		return nil, errUnsupportedFlexType(id)
	}

	slices.SortFunc(out, func(a, b map[string]any) int { return strings.Compare(nameField(a), nameField(b)) })

	return out, nil
}

func validIPv4(s string) (net.IP, bool) {
	ip := net.ParseIP(s).To4()

	return ip, ip != nil && !strings.Contains(s, ":")
}

var flexRuleNameRE = regexp.MustCompile(`^[a-zA-Z0-9][-_a-zA-Z0-9]{0,79}$`)

func (p *FlexDBProvider) writeFirewall(id ResourceID, body map[string]any) (flexResult, error) {
	props := flexMap(body, fieldProperties)
	name := id.LeafName()

	if !flexRuleNameRE.MatchString(name) || strings.HasSuffix(name, "-") {
		return flexResult{}, flexInvalid(
			"The firewall rule name must be 1 to 80 letters, digits, hyphens or underscores, start with a letter " +
				"or digit and not end with a hyphen.",
		)
	}

	start, _ := flexStr(props, "startIpAddress")
	end, _ := flexStr(props, "endIpAddress")

	s, ok1 := validIPv4(start)
	e, ok2 := validIPv4(end)

	switch {
	case !ok1 || !ok2:
		return flexResult{}, flexInvalid("startIpAddress and endIpAddress must be valid IPv4 addresses.")
	case strings.Compare(string(s), string(e)) > 0:
		return flexResult{}, flexInvalid("startIpAddress must not be greater than endIpAddress.")
	}

	p.mu.Lock("writeFirewall")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return flexResult{}, err
	}

	if rec.Subnet != "" {
		return flexResult{}, flexInvalid(
			"Firewall rules are not supported on a server with virtual network integration.",
		)
	}

	if rec.Firewall == nil {
		rec.Firewall = map[string]flexFirewall{}
	}

	fw := flexFirewall{Name: name, Start: start, End: end}
	rec.Firewall[strings.ToLower(name)] = fw

	return flexResult{
		body: p.firewallBody(id, fw), opID: p.newOp(id.ARMID(), true, "", nil),
	}, nil
}

func (p *FlexDBProvider) deleteFirewall(id ResourceID) (string, error) {
	p.mu.Lock("deleteFirewall")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return "", err
	}

	k := strings.ToLower(id.LeafName())
	if _, ok := rec.Firewall[k]; !ok {
		return "", p.childNotFound(id)
	}

	delete(rec.Firewall, k)

	return p.newOp(id.ARMID(), false, "", nil), nil
}
