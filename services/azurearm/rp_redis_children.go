package azurearm

import (
	"fmt"
	"net/netip"
	"sort"
	"strings"
)

func (p *RedisProvider) putFirewall(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	start, _ := props["startIP"].(string)
	end, _ := props["endIP"].(string)

	if err = validIPRange(start, end); err != nil {
		return nil, badRedisRequest(err)
	}

	key := strings.ToLower(id.LeafName())
	if _, exists := rec.Firewall[key]; !exists && len(rec.Firewall) >= redisMaxChildren {
		return nil, badRedisRequest(fmt.Errorf("%w: at most %d firewall rules", ErrRedisInvalid, redisMaxChildren))
	}

	fw := redisFirewall{Name: id.LeafName(), Start: start, End: end}
	if rec.Firewall == nil {
		rec.Firewall = map[string]redisFirewall{}
	}

	rec.Firewall[key] = fw

	return firewallBody(id, rec, fw), nil
}

func validIPRange(start, end string) error {
	a, errA := netip.ParseAddr(start)
	b, errB := netip.ParseAddr(end)

	if errA != nil || errB != nil || !a.Is4() || !b.Is4() {
		return fmt.Errorf("%w: startIP and endIP must be IPv4 addresses", ErrRedisInvalid)
	}

	if a.Compare(b) > 0 {
		return fmt.Errorf("%w: startIP must not be greater than endIP", ErrRedisInvalid)
	}

	return nil
}

func firewallBody(id ResourceID, _ *redisRecord, fw redisFirewall) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: fw.Name, fieldType: namespaceMicrosoftCache + "/Redis/" + redisFirewallType,
		fieldProperties: map[string]any{"startIP": fw.Start, "endIP": fw.End},
	}
}

var redisDays = map[string]bool{ //nolint:gochecknoglobals // read-only
	"monday": true, "tuesday": true, "wednesday": true, "thursday": true, "friday": true, "saturday": true,
	"sunday": true, "everyday": true, "weekend": true, "weekday": true,
}

func (p *RedisProvider) putPatch(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	if rec.SKU != redisSKUPremium {
		return nil, badRedisRequest(fmt.Errorf("%w: patch schedules need a Premium cache", ErrRedisInvalid))
	}

	if !strings.EqualFold(id.LeafName(), redisPatchDefault) {
		return nil, badRedisRequest(
			fmt.Errorf("%w: the patch schedule is named %q", ErrRedisInvalid, redisPatchDefault))
	}

	props, _ := body[fieldProperties].(map[string]any)
	entries, _ := props["scheduleEntries"].([]any)

	if err = validScheduleEntries(entries); err != nil {
		return nil, badRedisRequest(err)
	}

	rec.Patch = entries

	return p.patchBody(id, rec)
}

func validScheduleEntries(entries []any) error {
	if len(entries) == 0 || len(entries) > redisMaxChildren {
		return fmt.Errorf("%w: scheduleEntries must hold at least one entry", ErrRedisInvalid)
	}

	for _, e := range entries {
		m, _ := e.(map[string]any)
		day, _ := m["dayOfWeek"].(string)
		hour, ok := m["startHourUtc"].(float64)

		if !redisDays[strings.ToLower(day)] || !ok || hour < 0 || hour > redisMaxHour {
			return fmt.Errorf("%w: each schedule entry needs a dayOfWeek and a startHourUtc of 0 to %d",
				ErrRedisInvalid, redisMaxHour)
		}
	}

	return nil
}

const redisMaxHour = 23

func (p *RedisProvider) patchBody(id ResourceID, rec *redisRecord) (map[string]any, error) {
	if rec.Patch == nil {
		return nil, ErrRedisPatchNotFound
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: redisPatchDefault, fieldType: namespaceMicrosoftCache + "/Redis/" + redisPatchType,
		fieldLocation:   rec.Location,
		fieldProperties: map[string]any{"scheduleEntries": rec.Patch},
	}, nil
}

func (p *RedisProvider) putLinked(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	ls := redisLinked{Name: id.LeafName()}
	ls.LinkedID, _ = props["linkedRedisCacheId"].(string)
	ls.Location, _ = props["linkedRedisCacheLocation"].(string)
	ls.Role, _ = props["serverRole"].(string)

	if err = p.validLinked(id, rec, ls); err != nil {
		return nil, badRedisRequest(err)
	}

	if _, exists := rec.Linked[strings.ToLower(ls.Name)]; !exists && len(rec.Linked) >= redisMaxChildren {
		return nil, badRedisRequest(fmt.Errorf("%w: at most %d linked servers", ErrRedisInvalid, redisMaxChildren))
	}

	if rec.Linked == nil {
		rec.Linked = map[string]redisLinked{}
	}

	rec.Linked[strings.ToLower(ls.Name)] = ls

	return linkedBody(id, rec, ls), nil
}

func (p *RedisProvider) validLinked(id ResourceID, rec *redisRecord, ls redisLinked) error {
	linkedID, err := ParseGenericResourcePath(ls.LinkedID)

	switch {
	case ls.LinkedID == "" || ls.Location == "":
		return fmt.Errorf("%w: linkedRedisCacheId and linkedRedisCacheLocation are required", ErrRedisInvalid)
	case ls.Role != "Primary" && ls.Role != "Secondary":
		return fmt.Errorf("%w: serverRole must be Primary or Secondary", ErrRedisInvalid)
	case rec.SKU != redisSKUPremium:
		return fmt.Errorf("%w: geo-replication needs a Premium cache", ErrRedisInvalid)
	case err != nil || classifyRedisPath(linkedID) != redisKindCache:
		return fmt.Errorf("%w: linkedRedisCacheId is not a Redis cache", ErrRedisInvalid)
	case !strings.EqualFold(linkedID.LeafName(), id.LeafName()):
		return fmt.Errorf("%w: the linked server is named after the linked cache", ErrRedisInvalid)
	}

	linked, ok := p.res[redisKey(linkedID)]
	if !ok || linked.SKU != redisSKUPremium {
		return fmt.Errorf("%w: the linked cache must be an existing Premium cache", ErrRedisInvalid)
	}

	return nil
}

func linkedBody(id ResourceID, rec *redisRecord, ls redisLinked) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: ls.Name, fieldType: namespaceMicrosoftCache + "/Redis/" + redisLinkedType,
		fieldProperties: map[string]any{
			"linkedRedisCacheId": ls.LinkedID, "linkedRedisCacheLocation": ls.Location, "serverRole": ls.Role,
			fieldProvisioningState:         provisioningStateSucceeded,
			"geoReplicatedPrimaryHostName": redisHostName(rec.Name),
			"primaryHostName":              redisHostName(rec.Name),
		},
	}
}

func sortedLinked(rec *redisRecord) []redisLinked {
	out := make([]redisLinked, 0, len(rec.Linked))
	for _, ls := range rec.Linked {
		out = append(out, ls)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (p *RedisProvider) deleteChild(id ResourceID, kind redisKind) error {
	rec, err := p.cache(id)
	if err != nil {
		return err
	}

	key := strings.ToLower(id.LeafName())

	switch kind {
	case redisKindFirewall:
		if _, ok := rec.Firewall[key]; !ok {
			return ErrRedisFirewallNotFound
		}

		delete(rec.Firewall, key)
	case redisKindPatch:
		if rec.Patch == nil {
			return ErrRedisPatchNotFound
		}

		rec.Patch = nil
	case redisKindLinked:
		if _, ok := rec.Linked[key]; !ok {
			return ErrRedisLinkedNotFound
		}

		delete(rec.Linked, key)
	case redisKindCache, redisKindUnsupported:
	}

	return nil
}

// ListChildren lists a cache's firewall rules, patch schedules or linked servers.
func (p *RedisProvider) ListChildren(parent ResourceID, childType string) ([]map[string]any, error) {
	p.mu.RLock("ListChildren")
	defer p.mu.RUnlock()

	rec, err := p.cache(parent)
	if err != nil {
		return nil, err
	}

	out := []map[string]any{}

	switch strings.ToLower(childType) {
	case strings.ToLower(redisFirewallType):
		for _, fw := range sortedFirewalls(rec) {
			out = append(out, firewallBody(childID(parent, redisFirewallType, fw.Name), rec, fw))
		}
	case strings.ToLower(redisLinkedType):
		for _, ls := range sortedLinked(rec) {
			out = append(out, linkedBody(childID(parent, redisLinkedType, ls.Name), rec, ls))
		}
	case strings.ToLower(redisPatchType):
		if body, perr := p.patchBody(childID(parent, redisPatchType, redisPatchDefault), rec); perr == nil {
			out = append(out, body)
		}
	}

	return out, nil
}

func childID(parent ResourceID, typ, name string) ResourceID {
	parent.Types = []string{redisType, typ}
	parent.Names = []string{parent.Names[0], name}

	return parent
}

func sortedFirewalls(rec *redisRecord) []redisFirewall {
	out := make([]redisFirewall, 0, len(rec.Firewall))
	for _, fw := range rec.Firewall {
		out = append(out, fw)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
