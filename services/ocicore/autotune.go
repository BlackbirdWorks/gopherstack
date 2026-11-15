package ocicore

import (
	"slices"
	"time"
)

const (
	autotuneDetached = "DETACHED_VOLUME"
	autotuneByPerf   = "PERFORMANCE_BASED"
	maxAutotunes     = 2
	detachedAutotune = 14 * 24 * time.Hour
)

// AutotunePolicy is one entry of autotunePolicies (autotuneType discriminates; maxVpusPerGB is PERFORMANCE_BASED only).
type AutotunePolicy struct {
	MaxVpusPerGB *int64 `json:"maxVpusPerGB,omitempty"`
	AutotuneType string `json:"autotuneType"`
}

func checkAutotune(vpus int64, want []AutotunePolicy) *APIError {
	if len(want) > maxAutotunes {
		return errParam("autotunePolicies holds at most one policy of each type")
	}

	seen := map[string]bool{}

	for _, p := range want {
		if seen[p.AutotuneType] {
			return errParam("autotunePolicies has more than one " + p.AutotuneType + " policy")
		}

		seen[p.AutotuneType] = true

		switch p.AutotuneType {
		case autotuneDetached:
		case autotuneByPerf:
			if p.MaxVpusPerGB == nil {
				return errMissing("maxVpusPerGB is required for a PERFORMANCE_BASED autotune policy")
			}

			if aerr := checkVpus(*p.MaxVpusPerGB); aerr != nil {
				return aerr
			}

			if *p.MaxVpusPerGB < vpus {
				return errParam("maxVpusPerGB cannot be lower than vpusPerGB")
			}
		default:
			return errParam("autotuneType must be DETACHED_VOLUME or PERFORMANCE_BASED")
		}
	}

	return nil
}

func hasAutotune(ps []AutotunePolicy, kind string) bool {
	return slices.ContainsFunc(ps, func(p AutotunePolicy) bool { return p.AutotuneType == kind })
}

func clonePolicies(ps []AutotunePolicy) []AutotunePolicy {
	if len(ps) == 0 {
		return nil
	}

	return slices.Clone(ps)
}

// tunedVpus is the VPUs/GB a volume is effectively tuned to: no load is ever generated, so a performance
// policy stays at the default; a detached policy drops an unattached volume to the lower-cost 0 after 14 days.
func tunedVpus(vpus int64, ps []AutotunePolicy, since *time.Time, attached bool, now time.Time) int64 {
	if hasAutotune(ps, autotuneDetached) && !attached && since != nil && now.Sub(*since) >= detachedAutotune {
		return 0
	}

	return vpus
}

// sweepAutotune tracks when a policy-carrying volume lost its last attachment and refreshes autoTunedVpusPerGB.
func (b *InMemoryBackend) sweepAutotune(now time.Time) {
	for _, r := range b.res {
		b.retune(r, now)
	}
}

func (b *InMemoryBackend) retune(r Resource, now time.Time) {
	switch v := r.(type) {
	case *Volume:
		attached := len(liveOf(b, func(a *VolumeAttachment) bool { return a.VolumeID == v.ID })) > 0
		v.AutoTunedVpusPerGB = b.refreshTuned(v, v.VpusPerGB, v.AutotunePolicies, attached, now)
	case *BootVolume:
		attached := len(liveOf(b, func(a *BootVolumeAttachment) bool { return a.BootVolumeID == v.ID })) > 0
		v.AutoTunedVpusPerGB = b.refreshTuned(v, v.VpusPerGB, v.AutotunePolicies, attached, now)
	}
}

func (b *InMemoryBackend) refreshTuned(
	r Resource, vpus int64, ps []AutotunePolicy, attached bool, now time.Time,
) *int64 {
	if len(ps) == 0 {
		return nil
	}

	in := r.base().Internal

	switch {
	case attached:
		in.UnattachedSince = nil
	case in.UnattachedSince == nil:
		in.UnattachedSince = &now
	}

	out := tunedVpus(vpus, ps, in.UnattachedSince, attached, now)

	return &out
}

// setAutotune applies a validated policy list; enabling or changing the detached policy restarts its 14 day clock.
func setAutotune(r Resource, cur *[]AutotunePolicy, want []AutotunePolicy) {
	r.base().Internal.UnattachedSince = nil
	*cur = clonePolicies(want)
}

// applyAutotune replaces the policies when the request carries them and re-checks the kept ones against vpusPerGB.
func applyAutotune(r Resource, vpus int64, cur *[]AutotunePolicy, want *[]AutotunePolicy) *APIError {
	if want == nil {
		return checkAutotune(vpus, *cur)
	}

	if aerr := checkAutotune(vpus, *want); aerr != nil {
		return aerr
	}

	setAutotune(r, cur, *want)

	return nil
}
