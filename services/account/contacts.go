package account

import "fmt"

// GetAlternateContact retrieves an alternate contact by type.
func (b *InMemoryBackend) GetAlternateContact(ct ContactType) (*AlternateContact, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	contact, ok := b.alternateContacts.Get(string(ct))
	if !ok {
		return nil, fmt.Errorf("%w: type %s", errNoAlternateContact, ct)
	}

	cp := *contact

	return &cp, nil
}

// PutAlternateContact creates or updates an alternate contact.
func (b *InMemoryBackend) PutAlternateContact(contact *AlternateContact) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *contact
	b.alternateContacts.Put(&cp)

	return nil
}

// DeleteAlternateContact removes an alternate contact by type.
func (b *InMemoryBackend) DeleteAlternateContact(ct ContactType) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.alternateContacts.Delete(string(ct)) {
		return fmt.Errorf("%w: type %s", errNoAlternateContact, ct)
	}

	return nil
}

// GetContactInformation retrieves primary contact information.
func (b *InMemoryBackend) GetContactInformation() (*ContactInformation, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.contactInfo == nil {
		return nil, errNoContactInfo
	}

	cp := *b.contactInfo

	return &cp, nil
}

// PutContactInformation sets primary contact information.
func (b *InMemoryBackend) PutContactInformation(info *ContactInformation) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *info
	b.contactInfo = &cp

	return nil
}
