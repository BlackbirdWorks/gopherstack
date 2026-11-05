package gcpcompute

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxPatternDigits = 18
	shapeBalanced    = "BALANCED"
	shapeAny         = "ANY"
	bulkDone         = "DONE"
)

var hashRunRE = regexp.MustCompile(`^([^#]*)(#+)([^#]*)$`)

func (b *InMemoryBackend) bulkProps(
	project string, in *computeapi.BulkInsertInstanceResource, rawProps map[string]any,
) (*computeapi.InstanceProperties, *APIError) {
	if in.SourceInstanceTemplate == "" {
		if in.InstanceProperties == nil {
			return nil, errRequired("instanceProperties")
		}

		return in.InstanceProperties, nil
	}

	b.mu.RLock("BulkProps")
	defer b.mu.RUnlock()

	t, err := b.resolveTemplateLocked(project, in.SourceInstanceTemplate, "sourceInstanceTemplate")
	if err != nil {
		return nil, err
	}

	doc := toDoc(t.Properties)
	mergePatch(doc, rawProps)

	out, derr := fromDoc[computeapi.InstanceProperties](doc)
	if derr != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", derr)
	}

	return out, nil
}

func validBulk(in *computeapi.BulkInsertInstanceResource) *APIError {
	switch {
	case in.NamePattern == "" && len(in.PerInstanceProperties) == 0:
		return errInvalid("Invalid value for field 'namePattern': either namePattern or perInstanceProperties is required.")
	case in.NamePattern != "" && len(in.PerInstanceProperties) > 0:
		return errInvalid("Invalid value for field 'namePattern': it cannot be combined with perInstanceProperties.")
	case in.Count < 0 || in.MinCount < 0 || in.MinCount > in.Count && in.Count > 0:
		return errField("count", strconv.FormatInt(in.Count, 10), "count must not be negative and at least minCount.")
	}

	if in.NamePattern != "" {
		m := hashRunRE.FindStringSubmatch(in.NamePattern)
		if m == nil || len(m[2]) > maxPatternDigits {
			return errField("namePattern", in.NamePattern,
				"The pattern must contain one continuous sequence of up to 18 '#' characters.")
		}

		if in.Count < 1 {
			return errRequired("count")
		}
	}

	return nil
}

func (b *InMemoryBackend) bulkNamesLocked(project, zone string, in *computeapi.BulkInsertInstanceResource, n int) []string {
	if in.NamePattern == "" {
		names := make([]string, 0, len(in.PerInstanceProperties))
		for k := range in.PerInstanceProperties {
			names = append(names, k)
		}

		sort.Strings(names)

		return names
	}

	m := hashRunRE.FindStringSubmatch(in.NamePattern)
	pre, digits, post := m[1], len(m[2]), m[3]
	next := 1

	for _, r := range b.instances {
		if r.Inst.Name == "" || !strings.HasPrefix(r.Inst.SelfLink, zoneLink(project, zone, "instances", "")) {
			continue
		}

		num, ok := strings.CutPrefix(r.Inst.Name, pre)
		if num, ok2 := strings.CutSuffix(num, post); ok && ok2 && len(num) == digits {
			if v, err := strconv.Atoi(num); err == nil && v >= next {
				next = v + 1
			}
		}
	}

	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%0*d%s", pre, digits, next+i, post)
	}

	return out
}

func (b *InMemoryBackend) bulkCreateLocked(
	project string, zones []string, in *computeapi.BulkInsertInstanceResource,
	props *computeapi.InstanceProperties, shape string,
) (map[string]*computeapi.BulkInsertOperationStatus, *APIError) {
	want := int(in.Count)
	if want == 0 {
		want = len(in.PerInstanceProperties)
	}

	minCount := int(in.MinCount)
	if minCount == 0 {
		minCount = want
	}

	status := map[string]*computeapi.BulkInsertOperationStatus{}

	type made struct{ zone, name string }

	var created []made

	var firstErr *APIError

	namesByZone := map[string][]string{}
	for i := range want {
		z := zones[0]
		if shape == shapeBalanced || shape == shapeAny {
			z = zones[i%len(zones)]
		}

		if _, ok := namesByZone[z]; !ok {
			namesByZone[z] = nil
		}

		namesByZone[z] = append(namesByZone[z], "")
	}

	for _, z := range zones {
		cnt, ok := namesByZone[z]
		if !ok {
			continue
		}

		st := &computeapi.BulkInsertOperationStatus{TargetVmCount: int64(len(cnt)), Status: bulkDone}
		status["zones/"+z] = st

		for _, name := range b.bulkNamesLocked(project, z, in, len(cnt)) {
			inst := instanceFromProps(props, name)
			if pp, ok := in.PerInstanceProperties[name]; ok && pp.Hostname != "" {
				inst.Hostname = pp.Hostname
			}

			if _, err := b.insertInstanceLocked(project, z, inst); err != nil {
				st.FailedToCreateVmCount++

				if firstErr == nil {
					firstErr = err
				}

				continue
			}

			st.CreatedVmCount++

			created = append(created, made{z, name})
		}
	}

	if len(created) >= minCount {
		return status, nil
	}

	for _, c := range created {
		if r, ok := b.instances[key(project, c.zone, c.name)]; ok {
			b.removeInstanceLocked(project, c.zone, r)
		}
	}

	if firstErr == nil {
		firstErr = errInvalid("Could not create the minimum number of instances.")
	}

	return nil, firstErr
}

func (b *InMemoryBackend) bulkInsert(
	project, scope, loc string,
	in *computeapi.BulkInsertInstanceResource, rawProps map[string]any,
) (*computeapi.Operation, error) {
	if err := validBulk(in); err != nil {
		return nil, err
	}

	props, err := b.bulkProps(project, in, rawProps)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("BulkInsert")
	defer b.mu.Unlock()

	zones, shape, err := b.bulkZonesLocked(scope, loc, in)
	if err != nil {
		return nil, err
	}

	status, err := b.bulkCreateLocked(project, zones, in, props, shape)
	if err != nil {
		return nil, err
	}

	op := b.newOpLocked("bulkInsert", target{project: project, scope: scope, loc: loc, collection: "instances"}, b.timing.Stage)

	meta := &computeapi.InstancesBulkInsertOperationMetadata{PerLocationStatus: map[string]computeapi.BulkInsertOperationStatus{}}
	for k, v := range status {
		meta.PerLocationStatus[k] = *v
	}

	for _, rec := range b.ops {
		if rec.Op.Name == op.Name {
			rec.Op.InstancesBulkInsertOperationMetadata = meta
		}
	}

	op.InstancesBulkInsertOperationMetadata = meta

	return op, nil
}

func (b *InMemoryBackend) bulkZonesLocked(
	scope, loc string, in *computeapi.BulkInsertInstanceResource,
) ([]string, string, *APIError) {
	shape := ""
	if in.LocationPolicy != nil {
		shape = in.LocationPolicy.TargetShape
	}

	if scope == scopeZone {
		return []string{loc}, shape, nil
	}

	var zones []string

	for _, z := range zonesInRegion(loc) {
		deny := false

		if in.LocationPolicy != nil {
			if l, ok := in.LocationPolicy.Locations["zones/"+z]; ok && l.Preference == "DENY" {
				deny = true
			}
		}

		if !deny {
			zones = append(zones, z)
		}
	}

	if len(zones) == 0 {
		return nil, "", errInvalid("Invalid value for field 'locationPolicy': no zone is allowed.")
	}

	slices.Sort(zones)

	return zones, shape, nil
}
