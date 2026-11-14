package gcpdatastore

import (
	"slices"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

const propRepresentation = "property_representation"

// indexRepresentation names the index type of v; ok is false for values that are not indexed.
func indexRepresentation(v *datastorepb.Value) (string, bool) {
	switch v.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_TimestampValue:
		return "INT64", true
	case *datastorepb.Value_DoubleValue:
		return "DOUBLE", true
	case *datastorepb.Value_BooleanValue:
		return "BOOLEAN", true
	case *datastorepb.Value_StringValue, *datastorepb.Value_BlobValue:
		return "STRING", true
	case *datastorepb.Value_KeyValue:
		return "REFERENCE", true
	case *datastorepb.Value_GeoPointValue:
		return "POINT", true
	case *datastorepb.Value_NullValue:
		return "NULL", true
	default:
		return "", false
	}
}

// collectRepresentations adds the index types of every indexed value of ent, flattening embedded entities
// into dotted property names.
func collectRepresentations(prefix string, props map[string]*datastorepb.Value, out map[string]map[string]struct{}) {
	for name, v := range props {
		collectValue(prefix+name, v, out)
	}
}

func collectValue(name string, v *datastorepb.Value, out map[string]map[string]struct{}) {
	if v.GetExcludeFromIndexes() && v.GetArrayValue() == nil {
		return
	}

	switch t := v.GetValueType().(type) {
	case *datastorepb.Value_ArrayValue:
		for _, el := range t.ArrayValue.GetValues() {
			collectValue(name, el, out)
		}
	case *datastorepb.Value_EntityValue:
		collectRepresentations(name+".", t.EntityValue.GetProperties(), out)
	default:
		if rep, ok := indexRepresentation(v); ok {
			if out[name] == nil {
				out[name] = map[string]struct{}{}
			}

			out[name][rep] = struct{}{}
		}
	}
}

// propertyCandidates serves __property__ queries: one entity per indexed property of every kind, keyed
// __kind__/<kind>/__property__/<property> with the sorted index types in property_representation.
func (b *InMemoryBackend) propertyCandidates(p *queryPlan, rv *readView) []*cand {
	byKind := map[string]map[string]map[string]struct{}{}

	for _, rec := range b.ents {
		kind := keyKind(rec.key)
		if keyPartition(rec.key) != p.part || isReserved(kind) {
			continue
		}

		ver := rec.at(rv.at)
		if ver == nil || ver.ent == nil {
			continue
		}

		if byKind[kind] == nil {
			byKind[kind] = map[string]map[string]struct{}{}
		}

		collectRepresentations("", ver.ent.GetProperties(), byKind[kind])
	}

	var out []*cand

	for kind, props := range byKind {
		for name, reps := range props {
			key := &datastorepb.Key{PartitionId: p.part.stored(), Path: []*datastorepb.Key_PathElement{
				{Kind: kindProperty, IdType: &datastorepb.Key_PathElement_Name{Name: kind}},
				{Kind: propProperty, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
			}}
			if p.ancestor != nil && !isDescendantOrSelf(key, p.ancestor) {
				continue
			}

			out = append(out, b.propertyCandidate(key, reps))
		}
	}

	return out
}

func (b *InMemoryBackend) propertyCandidate(key *datastorepb.Key, reps map[string]struct{}) *cand {
	names := make([]string, 0, len(reps))
	for r := range reps {
		names = append(names, r)
	}

	slices.Sort(names)

	vals := make([]*datastorepb.Value, len(names))
	for i, n := range names {
		vals[i] = &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: n}}
	}

	ent := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{
		propRepresentation: {
			ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: vals}},
		},
	}}

	return &cand{key: key, ent: ent, ver: &entVer{}}
}
