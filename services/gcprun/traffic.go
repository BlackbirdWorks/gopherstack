package gcprun

import (
	"regexp"

	"cloud.google.com/go/run/apiv2/runpb"
)

var tagRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,45}$`)

const (
	fullPercent  = 100
	tagSeparator = "---"
)

func trafficTypeOf(t *runpb.TrafficTarget) runpb.TrafficTargetAllocationType {
	switch {
	case t.GetType() != runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_UNSPECIFIED:
		return t.GetType()
	case t.GetRevision() != "":
		return runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION
	default:
		return runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST
	}
}

// normalizeTraffic validates targets against known revision short names and fills types and the default split.
func normalizeTraffic(in []*runpb.TrafficTarget, known map[string]bool) ([]*runpb.TrafficTarget, error) {
	if len(in) == 0 {
		return []*runpb.TrafficTarget{{
			Type:    runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST,
			Percent: fullPercent,
		}}, nil
	}

	var (
		sum    int32
		tags   = map[string]bool{}
		latest bool
	)

	out := make([]*runpb.TrafficTarget, 0, len(in))

	for _, t := range in {
		t = clone(t)
		t.Type = trafficTypeOf(t)

		if err := checkTarget(t, known, tags, &latest); err != nil {
			return nil, err
		}

		sum += t.GetPercent()
		out = append(out, t)
	}

	if sum != fullPercent {
		return nil, errInvalid("Traffic percentages must sum to 100, got %d.", sum)
	}

	return out, nil
}

func checkTarget(t *runpb.TrafficTarget, known, tags map[string]bool, latest *bool) error {
	if t.GetPercent() < 0 || t.GetPercent() > fullPercent {
		return errInvalid("Traffic percent %d is out of range.", t.GetPercent())
	}

	if t.GetType() == runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION {
		if !known[t.GetRevision()] {
			return errInvalid("Revision %q referenced by traffic does not exist.", t.GetRevision())
		}
	} else {
		if *latest {
			return errInvalid("Only one traffic target may point at the latest revision.")
		}

		*latest = true
		t.Revision = ""
	}

	if t.GetTag() == "" {
		return nil
	}

	if !tagRe.MatchString(t.GetTag()) || tags[t.GetTag()] {
		return errInvalid("Traffic tag %q is invalid or duplicated.", t.GetTag())
	}

	tags[t.GetTag()] = true

	return nil
}

// trafficStatuses resolves targets to concrete revisions; a LATEST target with no ready revision is omitted.
func trafficStatuses(targets []*runpb.TrafficTarget, latestReady string) []*runpb.TrafficTargetStatus {
	out := make([]*runpb.TrafficTargetStatus, 0, len(targets))

	for _, t := range targets {
		st := &runpb.TrafficTargetStatus{
			Type:     t.GetType(),
			Revision: t.GetRevision(),
			Percent:  t.GetPercent(),
			Tag:      t.GetTag(),
		}

		if t.GetType() == runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST {
			if latestReady == "" {
				continue
			}

			st.Revision = latestReady
		}

		out = append(out, st)
	}

	return out
}
