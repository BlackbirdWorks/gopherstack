package ocicore

import "net/http"

const keyEnabled = "ENABLED"

// SetKeyChecker makes kmsKeyId arguments validated against OCI Vault: fn returns the key's lifecycle state.
func (b *InMemoryBackend) SetKeyChecker(fn func(id string) (string, bool)) {
	b.mu.Lock("SetKeyChecker")
	b.keyState = fn
	b.mu.Unlock()
}

// checkKmsKey validates a Vault key reference when a checker is wired; an empty id means Oracle-managed keys.
func (b *InMemoryBackend) checkKmsKey(id string) *APIError {
	if id == "" || b.keyState == nil {
		return nil
	}

	state, ok := b.keyState(id)
	if !ok {
		return errNotFound(id)
	}

	if state != keyEnabled {
		return errState("The Vault key " + id + " is " + state + " and cannot encrypt volumes")
	}

	return nil
}

// KmsKeyOut is the VolumeKmsKey and BootVolumeKmsKey response.
type KmsKeyOut struct {
	KmsKeyID *string `json:"kmsKeyId"`
}

func kmsOut(id string) KmsKeyOut {
	if id == "" {
		return KmsKeyOut{}
	}

	return KmsKeyOut{KmsKeyID: &id}
}

// volumeKey returns a pointer to the kmsKeyId of a volume or boot volume.
func volumeKey(r resource) *string {
	switch v := r.(type) {
	case *Volume:
		return &v.KmsKeyID
	case *BootVolume:
		return &v.KmsKeyID
	}

	return nil
}

// GetVolumeKmsKey returns the Vault key of a volume.
func GetVolumeKmsKey[T resource](b *InMemoryBackend, id string) (KmsKeyOut, *APIError) {
	b.mu.RLock("GetVolumeKmsKey")
	defer b.mu.RUnlock()

	v, aerr := get[T](b, id)
	if aerr != nil {
		return KmsKeyOut{}, aerr
	}

	return kmsOut(*volumeKey(v)), nil
}

// UpdateVolumeKmsKey assigns a Vault key to a volume or boot volume.
func UpdateVolumeKmsKey[T resource](b *InMemoryBackend, id, ifMatch, keyID string) (KmsKeyOut, *APIError) {
	b.mu.Lock("UpdateVolumeKmsKey")
	defer b.mu.Unlock()

	v, aerr := getLive[T](b, id)
	if aerr != nil {
		return KmsKeyOut{}, aerr
	}

	if aerr = checkEtag(v, ifMatch); aerr != nil {
		return KmsKeyOut{}, aerr
	}

	if keyID == "" {
		return KmsKeyOut{}, errMissing("kmsKeyId is required")
	}

	if aerr = b.checkKmsKey(keyID); aerr != nil {
		return KmsKeyOut{}, aerr
	}

	*volumeKey(v) = keyID
	b.bump(v)

	return kmsOut(keyID), nil
}

// DeleteVolumeKmsKey returns a volume or boot volume to Oracle-managed keys.
func DeleteVolumeKmsKey[T resource](b *InMemoryBackend, id, ifMatch string) *APIError {
	b.mu.Lock("DeleteVolumeKmsKey")
	defer b.mu.Unlock()

	v, aerr := getLive[T](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(v, ifMatch); aerr != nil {
		return aerr
	}

	*volumeKey(v) = ""
	b.bump(v)

	return nil
}

func kmsEntries() []entry {
	vol, boot := idPath("/volumes", "volumeId")+"/kmsKey", idPath("/bootVolumes", "bootVolumeId")+"/kmsKey"

	return []entry{
		{kmsGetOp[*Volume]("volumeId"), http.MethodGet, vol, "GetVolumeKmsKey"},
		{kmsUpdateOp[*Volume]("volumeId"), http.MethodPut, vol, "UpdateVolumeKmsKey"},
		{kmsDeleteOp[*Volume]("volumeId"), http.MethodDelete, vol, "DeleteVolumeKmsKey"},
		{kmsGetOp[*BootVolume]("bootVolumeId"), http.MethodGet, boot, "GetBootVolumeKmsKey"},
		{kmsUpdateOp[*BootVolume]("bootVolumeId"), http.MethodPut, boot, "UpdateBootVolumeKmsKey"},
		{kmsDeleteOp[*BootVolume]("bootVolumeId"), http.MethodDelete, boot, "DeleteBootVolumeKmsKey"},
	}
}

func kmsGetOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		out, aerr := GetVolumeKmsKey[T](h.Backend, q.param(param))
		if aerr != nil {
			return aerr
		}

		return q.json(http.StatusOK, out, "")
	}
}

func kmsUpdateOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		var in struct {
			KmsKeyID string `json:"kmsKeyId"`
		}

		if err := q.decode(&in); err != nil {
			return err
		}

		out, aerr := UpdateVolumeKmsKey[T](h.Backend, q.param(param), q.ifMatch(), in.KmsKeyID)
		if aerr != nil {
			return aerr
		}

		return q.json(http.StatusOK, out, "")
	}
}

func kmsDeleteOp[T resource](param string) opFunc {
	return func(h *Handler, q *req) error {
		return noBody(q, DeleteVolumeKmsKey[T](h.Backend, q.param(param), q.ifMatch()))
	}
}
