package ocivault

import "time"

func due(t *time.Time, now time.Time) bool { return t != nil && !t.After(now) }

// Sweep applies elapsed deletion schedules and due key auto-rotations against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	for _, v := range b.vaults {
		if v.State == statePendingDeletion && due(v.TimeOfDeletion, now) {
			v.State, v.Etag = stateDeleted, newEtag()
		}
	}

	for _, k := range b.keys {
		b.sweepKey(k, now)
	}

	for _, s := range b.secrets {
		sweepSecret(s, now)
	}
}

func (b *InMemoryBackend) sweepKey(k *Key, now time.Time) {
	if k.State == statePendingDeletion && due(k.TimeOfDeletion, now) {
		k.State, k.Etag = stateDeleted, newEtag()

		for _, kv := range k.Versions {
			kv.State, kv.Material = stateDeleted, nil
		}

		return
	}

	for _, kv := range k.Versions {
		if kv.State == statePendingDeletion && due(kv.TimeOfDeletion, now) {
			kv.State, kv.Material = stateDeleted, nil
		}
	}

	if k.State == stateEnabled && k.AutoRotation && k.Rotation != nil && due(k.Rotation.NextRotation, now) {
		b.autoRotate(k, now)
	}
}

func (b *InMemoryBackend) autoRotate(k *Key, now time.Time) {
	v := b.vaults[k.VaultID]
	if v == nil || v.State != stateActive {
		return
	}

	material, err := generateMaterial(k.Shape)
	if err != nil {
		k.Rotation.LastStatus, k.Rotation.LastMessage = "FAILED", err.Error()

		return
	}

	kv := b.addVersion(k, v.Prefix, originInternal, material)
	kv.AutoRotated = true

	next := now.Add(time.Duration(k.Rotation.IntervalDays) * day)
	k.Rotation.LastRotation, k.Rotation.NextRotation = &now, &next
	k.Rotation.LastStatus, k.Rotation.LastMessage = "SUCCESS", ""
	k.Etag = newEtag()
}

func sweepSecret(s *Secret, now time.Time) {
	if s.State == statePendingDeletion && due(s.TimeOfDeletion, now) {
		s.State, s.Versions, s.Etag = stateDeleted, nil, newEtag()
		s.Current, s.Previous, s.Pending = 0, 0, 0

		return
	}

	kept := s.Versions[:0]

	for _, v := range s.Versions {
		if !due(v.TimeOfDeletion, now) {
			kept = append(kept, v)
		}
	}

	if len(kept) != len(s.Versions) {
		s.Versions = kept
		s.Etag = newEtag()
	}
}
