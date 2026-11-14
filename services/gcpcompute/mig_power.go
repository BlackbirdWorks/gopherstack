package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

const (
	actStarting     = "STARTING"
	actStopping     = "STOPPING"
	actSuspending   = "SUSPENDING"
	actResuming     = "RESUMING"
	parkedStopped   = "STOPPED"
	parkedSuspended = "SUSPENDED"
)

type powerSpec struct {
	check   func(b *InMemoryBackend, project string, m *memberRec, r *instRec) *APIError
	apply   func(b *InMemoryBackend, project string, m *memberRec, r *instRec) *APIError
	need    string
	parked  string
	startup string
}

func powerSpecs() map[string]powerSpec {
	return map[string]powerSpec{
		"stopInstances": {
			need: stRunning, parked: parkedStopped,
			apply: func(b *InMemoryBackend, _ string, _ *memberRec, r *instRec) *APIError {
				b.stopLocked(r)

				return nil
			},
		},
		"suspendInstances": {
			need: stRunning, parked: parkedSuspended,
			check: func(b *InMemoryBackend, project string, m *memberRec, r *instRec) *APIError {
				return b.suspendCheckLocked(project, m.Zone, r)
			},
			apply: func(b *InMemoryBackend, _ string, _ *memberRec, r *instRec) *APIError {
				b.suspendLocked(r)

				return nil
			},
		},
		"startInstances": {
			need: stTerminated, startup: actStarting,
			apply: func(b *InMemoryBackend, project string, m *memberRec, r *instRec) *APIError {
				_, err := b.startLocked(project, m.Zone, r)

				return err
			},
		},
		"resumeInstances": {
			need: stSuspended, startup: actResuming,
			apply: func(b *InMemoryBackend, _ string, _ *memberRec, r *instRec) *APIError {
				b.resumeLocked(r)

				return nil
			},
		},
	}
}

// powerMIG stops, suspends, starts or resumes group members and moves them between targetSize and the
// stopped or suspended target sizes.
func (b *InMemoryBackend) powerMIG(
	project, scope, loc, name, verb string, in migInstancesRequest,
) (*computeapi.Operation, error) {
	spec := powerSpecs()[verb]

	return b.memberOp(project, scope, loc, name, verb, in, func(rec *migRec, ms []*memberRec) *APIError {
		recs := make([]*instRec, len(ms))
		now := b.now().UTC()

		for i, m := range ms {
			r, ok := b.instances[key(project, m.Zone, m.Name)]
			if !ok || r.status(now, b.timing) != spec.need || spec.startup != "" && m.Parked == "" {
				return errInvalid("Invalid value for field 'instances': '%s'. The instance must be %s for %s.",
					m.Name, spec.need, verb)
			}

			if spec.check != nil {
				if err := spec.check(b, project, m, r); err != nil {
					return err
				}
			}

			recs[i] = r
		}

		for i, m := range ms {
			if err := spec.apply(b, project, m, recs[i]); err != nil {
				return err
			}

			b.parkLocked(rec, m, spec)
		}

		rec.Mgr.Fingerprint = newFingerprint()

		return nil
	})
}

func (b *InMemoryBackend) parkLocked(rec *migRec, m *memberRec, spec powerSpec) {
	was := m.Parked
	m.Parked, m.Startup = spec.parked, spec.startup

	delta := int64(-1)
	if spec.parked == "" {
		delta = 1
	}

	rec.Mgr.TargetSize = max(rec.Mgr.TargetSize+delta, 0)

	switch spec.parked {
	case parkedStopped:
		rec.Mgr.TargetStoppedSize++
	case parkedSuspended:
		rec.Mgr.TargetSuspendedSize++
	}

	switch was {
	case parkedStopped:
		rec.Mgr.TargetStoppedSize = max(rec.Mgr.TargetStoppedSize-1, 0)
	case parkedSuspended:
		rec.Mgr.TargetSuspendedSize = max(rec.Mgr.TargetSuspendedSize-1, 0)
	}
}
