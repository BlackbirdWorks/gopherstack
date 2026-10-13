package ssooidc

import (
	"time"
)

// StartDeviceAuthorization opens a device authorization for a registered client.
func (b *InMemoryBackend) StartDeviceAuthorization(
	clientID, clientSecret, startURL string,
) (*DeviceAuthorization, *APIError) {
	if startURL == "" {
		return nil, errInvalidRequest("startUrl is required")
	}

	now := time.Now().UTC()

	b.mu.Lock("StartDeviceAuthorization")
	defer b.mu.Unlock()

	c, err := b.clientLocked(clientID, clientSecret, true, now)
	if err != nil {
		return nil, err
	}

	if !c.allowsGrant(grantDeviceCode) {
		return nil, errUnauthorizedClient("The client is not registered for the device code grant")
	}

	b.pruneLocked(now)

	d := &Device{
		DeviceCode: randomString(deviceCodeBytes),
		UserCode:   newUserCode(),
		ClientID:   clientID,
		StartURL:   startURL,
		Status:     devicePending,
		Interval:   deviceInterval,
		ExpiresAt:  now.Add(deviceTTL),
	}
	b.devices.Put(d)

	return &DeviceAuthorization{
		DeviceCode: d.DeviceCode,
		UserCode:   d.UserCode,
		ExpiresIn:  int32(deviceTTL / time.Second),
		Interval:   d.Interval,
	}, nil
}

func (b *InMemoryBackend) deviceByUserCodeLocked(userCode string, now time.Time) (*Device, bool) {
	want := normalizeUserCode(userCode)

	for _, d := range b.devices.All() {
		if d.UserCode == want && now.Before(d.ExpiresAt) && d.Status == devicePending {
			return d, true
		}
	}

	return nil, false
}

// DeviceUserCodeValid reports whether userCode names a pending, unexpired authorization.
func (b *InMemoryBackend) DeviceUserCodeValid(userCode string) bool {
	b.mu.RLock("DeviceUserCodeValid")
	defer b.mu.RUnlock()

	_, ok := b.deviceByUserCodeLocked(userCode, time.Now())

	return ok
}

// ApproveDevice marks the authorization for userCode approved for subject.
func (b *InMemoryBackend) ApproveDevice(userCode string, subject Subject) error {
	b.mu.Lock("ApproveDevice")
	defer b.mu.Unlock()

	d, ok := b.deviceByUserCodeLocked(userCode, time.Now())
	if !ok {
		return ErrUnknownUserCode
	}

	cp := *d
	cp.Status = deviceApproved
	cp.Subject = subject
	b.devices.Put(&cp)

	return nil
}

// DenyDevice marks the authorization for userCode denied.
func (b *InMemoryBackend) DenyDevice(userCode string) error {
	b.mu.Lock("DenyDevice")
	defer b.mu.Unlock()

	d, ok := b.deviceByUserCodeLocked(userCode, time.Now())
	if !ok {
		return ErrUnknownUserCode
	}

	cp := *d
	cp.Status = deviceDenied
	b.devices.Put(&cp)

	return nil
}
