package ocivault

import "time"

func due(t *time.Time, now time.Time) bool { return t != nil && !t.After(now) }

// Sweep applies elapsed deletion schedules and due key auto-rotations against the backend clock.
func (b *InMemoryBackend) Sweep() {
	for _, id := range b.sweepLocked() {
		b.rotate(id)
	}
}

func (b *InMemoryBackend) sweepLocked() []string {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	now := b.nowUTC()

	b.pruneRepl(now)

	for _, v := range b.vaults {
		v.settle(&v.State, now)

		for _, r := range v.Replicas {
			r.settle(&r.Status, now)
		}

		if v.State == statePendingDeletion && due(v.TimeOfDeletion, now) {
			v.State, v.Etag = stateDeleted, newEtag()
			b.eraseVaultKeys(v.ID)
		}
	}

	for _, k := range b.keys {
		b.sweepKey(k, now)
	}

	for _, s := range b.secrets {
		sweepSecret(s, now)
	}

	return b.dueRotations(now)
}

func (b *InMemoryBackend) sweepKey(k *Key, now time.Time) {
	k.settle(&k.State, now)

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
	b.touchKey(k)
}

func sweepSecret(s *Secret, now time.Time) {
	s.settle(&s.State, now)

	if s.Rotation != nil {
		s.Rotation.settle(&s.Rotation.Status, now)
	}

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

func (b *InMemoryBackend) eraseVaultKeys(vaultID string) {
	for _, k := range b.keys {
		if k.VaultID != vaultID || k.State == stateDeleted {
			continue
		}

		k.State, k.Etag, k.transition = stateDeleted, newEtag(), transition{}
		b.stampKey(k)

		for _, kv := range k.Versions {
			kv.State, kv.Material = stateDeleted, nil
		}
	}
}
