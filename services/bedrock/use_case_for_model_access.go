package bedrock

// GetUseCaseForModelAccess returns the raw FormData bytes previously stored by
// PutUseCaseForModelAccess (real AWS: GetUseCaseForModelAccessOutput.FormData is
// a required raw byte payload, not a structured {useCaseType,useCaseDescription}
// object -- see PutUseCaseForModelAccess's doc comment for the full shape note).
func (b *InMemoryBackend) GetUseCaseForModelAccess() []byte {
	b.mu.RLock("GetUseCaseForModelAccess")
	defer b.mu.RUnlock()

	return append([]byte(nil), b.useCaseFormData...)
}

// PutUseCaseForModelAccess stores the raw FormData bytes submitted for model
// access use-case registration.
//
// Real AWS's PutUseCaseForModelAccessInput has a single required field,
// FormData []byte, sent as {"formData": "<base64>"} over POST
// /use-case-for-model-access -- there is no structured useCaseType/
// useCaseDescription JSON body; that shape (and the PUT method, and the
// "/usecase-for-model-access" path typo) was a gopherstack invention with no
// basis in the real API and has been removed as part of the parity fix.
func (b *InMemoryBackend) PutUseCaseForModelAccess(formData []byte) {
	b.mu.Lock("PutUseCaseForModelAccess")
	defer b.mu.Unlock()

	b.useCaseFormData = append([]byte(nil), formData...)
}
