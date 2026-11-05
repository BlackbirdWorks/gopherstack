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
		return errInvalid(
			"Invalid value for field 'namePattern': either namePattern or perInstanceProperties is required.",
		)
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

func (b *InMemoryBackend) bulkNamesLocked(
	project, zone string,
	in *computeapi.BulkInsertInstanceResource,
	n int,
) []string {
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
		if r.Inst.Name == "" || !strings.HasPrefix(r.Inst.SelfLink, zoneLink(project, zone, collInstances, "")) {
			continue
		}

		trimmed, hasPre := strings.CutPrefix(r.Inst.Name, pre)
		num, hasPost := strings.CutSuffix(trimmed, post)

		if hasPre && hasPost && len(num) == digits {
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

type bulkSlot struct{ zone, name string }

// bulkPlanLocked assigns every requested instance a zone and a name.
func (b *InMemoryBackend) bulkPlanLocked(
	project string, zones []string, in *computeapi.BulkInsertInstanceResource, shape string,
) []bulkSlot {
	want := int(in.Count)
	if want == 0 {
		want = len(in.PerInstanceProperties)
	}

	slots := make([]bulkSlot, want)
	perZone := map[string]int{}

	for i := range slots {
		z := zones[0]
		if shape == shapeBalanced || shape == shapeAny {
			z = zones[i%len(zones)]
		}

		slots[i].zone = z
		perZone[z]++
	}

	if in.NamePattern == "" {
		for i, n := range b.bulkNamesLocked(project, zones[0], in, want) {
			slots[i].name = n
		}

		return slots
	}

	names := map[string][]string{}
	for z, n := range perZone {
		names[z] = b.bulkNamesLocked(project, z, in, n)
	}

	for i := range slots {
		z := slots[i].zone
		slots[i].name, names[z] = names[z][0], names[z][1:]
	}

	return slots
}

func (b *InMemoryBackend) bulkCreateLocked(
	project string, zones []string, in *computeapi.BulkInsertInstanceResource,
	props *computeapi.InstanceProperties, shape string,
) (map[string]*computeapi.BulkInsertOperationStatus, *APIError) {
	slots := b.bulkPlanLocked(project, zones, in, shape)

	minCount := int(in.MinCount)
	if minCount == 0 {
		minCount = len(slots)
	}

	status := map[string]*computeapi.BulkInsertOperationStatus{}

	var (
		created  []bulkSlot
		firstErr *APIError
	)

	for _, sl := range slots {
		st := status["zones/"+sl.zone]
		if st == nil {
			st = &computeapi.BulkInsertOperationStatus{Status: bulkDone}
			status["zones/"+sl.zone] = st
		}

		st.TargetVmCount++

		inst := instanceFromProps(props, sl.name)
		if pp, found := in.PerInstanceProperties[sl.name]; found && pp.Hostname != "" {
			inst.Hostname = pp.Hostname
		}

		if _, err := b.insertInstanceLocked(project, sl.zone, inst); err != nil {
			st.FailedToCreateVmCount++
			firstErr = cmpErr(firstErr, err)

			continue
		}

		st.CreatedVmCount++

		created = append(created, sl)
	}

	if len(created) >= minCount {
		return status, nil
	}

	for _, c := range created {
		if r, found := b.instances[key(project, c.zone, c.name)]; found {
			b.removeInstanceLocked(project, c.zone, r)
		}
	}

	return nil, cmpErr(firstErr, errInvalid("Could not create the minimum number of instances."))
}

// cmpErr keeps the first error.
func cmpErr(first, next *APIError) *APIError {
	if first != nil {
		return first
	}

	return next
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

	op := b.newOpLocked(
		"bulkInsert",
		target{project: project, scope: scope, loc: loc, collection: collInstances},
		b.timing.Stage,
	)

	meta := &computeapi.InstancesBulkInsertOperationMetadata{
		PerLocationStatus: map[string]computeapi.BulkInsertOperationStatus{},
	}
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
