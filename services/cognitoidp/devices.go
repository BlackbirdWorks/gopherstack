package cognitoidp

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// AdminForgetDevice removes a tracked device for a user. A device that is on
// record (registered via ConfirmDevice) is really deleted. A missing
// deviceKey is treated as a no-op rather than ResourceNotFoundException:
// pre-existing callers invoke this operation without ever having confirmed a
// device, and historically received success once the user was found; this
// keeps that contract while making the operation state-aware for devices
// that do exist.
func (b *InMemoryBackend) AdminForgetDevice(userPoolID, username, deviceKey string) error {
	b.mu.Lock("AdminForgetDevice")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if key := userStateKey(userPoolID, username); b.devices[key] != nil {
		delete(b.devices[key], deviceKey)
	}

	return nil
}

// Device tracking status values (DeviceRememberedStatusType).
const (
	DeviceStatusRemembered    = "remembered"
	DeviceStatusNotRemembered = "not_remembered"
)

// paginateDevicesLocked returns a page of devices for the given store key,
// sorted by DeviceKey for stable pagination. Caller must hold b.mu.
func (b *InMemoryBackend) paginateDevicesLocked(key string, limit int, nextToken string) ([]*Device, string) {
	devices := b.devices[key]
	all := make([]*Device, 0, len(devices))

	for _, d := range devices {
		cp := *d
		cp.Attributes = maps.Clone(d.Attributes)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DeviceKey < all[j].DeviceKey })

	startIdx := 0

	if nextToken != "" {
		for i, d := range all {
			if d.DeviceKey == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, ""
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].DeviceKey
	}

	return page, newToken
}

// ConfirmDevice registers a new device for the authenticated user, or
// refreshes last-authenticated/attributes if the device is already known. If
// deviceKey is empty, a new one is generated: AWS normally derives DeviceKey
// client-side from SRP device-verifier material handed out during
// authentication, but this emulator does not mint device metadata during
// InitiateAuth, so it provisions a key here so ListDevices/GetDevice can
// enumerate the device afterward. Returns the confirmed device key and
// whether user confirmation is necessary (always false: this emulator does
// not model the adaptive-auth device-confirmation workflow).
func (b *InMemoryBackend) ConfirmDevice(accessToken, deviceKey, deviceName string) (string, bool, error) {
	b.mu.Lock("ConfirmDevice")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return "", false, err
	}

	if deviceKey == "" {
		deviceKey = b.region + "_" + uuid.New().String()
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if b.devices[key] == nil {
		b.devices[key] = make(map[string]*Device)
	}

	now := time.Now()

	if existing, ok := b.devices[key][deviceKey]; ok {
		existing.LastAuthenticatedAt = now
		existing.LastModifiedAt = now

		if deviceName != "" {
			if existing.Attributes == nil {
				existing.Attributes = map[string]string{}
			}

			existing.Attributes["device_name"] = deviceName
		}

		return deviceKey, false, nil
	}

	attrs := map[string]string{}
	if deviceName != "" {
		attrs["device_name"] = deviceName
	}

	b.devices[key][deviceKey] = &Device{
		DeviceKey:           deviceKey,
		CreatedAt:           now,
		LastModifiedAt:      now,
		LastAuthenticatedAt: now,
		Attributes:          attrs,
		Status:              DeviceStatusNotRemembered,
	}

	return deviceKey, false, nil
}

// AdminGetDevice returns a single tracked device for a user (admin operation).
func (b *InMemoryBackend) AdminGetDevice(userPoolID, username, deviceKey string) (*Device, error) {
	b.mu.RLock("AdminGetDevice")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		// AdminGetDevice's own deserializer models ResourceNotFoundException,
		// not UserNotFoundException, for a missing user — unlike AdminGetUser
		// and similar ops (aws-sdk-go-v2/service/cognitoidentityprovider
		// @v1.67.4 deserializers.go).
		return nil, fmt.Errorf("%w: user %q not found", ErrDeviceNotFound, username)
	}

	dev, ok := b.devices[userStateKey(userPoolID, username)][deviceKey]
	if !ok {
		return nil, fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	cp := *dev
	cp.Attributes = maps.Clone(dev.Attributes)

	return &cp, nil
}

// GetDevice returns a single tracked device for the authenticated user.
func (b *InMemoryBackend) GetDevice(accessToken, deviceKey string) (*Device, error) {
	b.mu.RLock("GetDevice")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	dev, ok := b.devices[userStateKey(user.UserPoolID, user.Username)][deviceKey]
	if !ok {
		return nil, fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	cp := *dev
	cp.Attributes = maps.Clone(dev.Attributes)

	return &cp, nil
}

// AdminListDevices returns a page of tracked devices for a user (admin operation).
func (b *InMemoryBackend) AdminListDevices(
	userPoolID, username string,
	limit int,
	nextToken string,
) ([]*Device, string, error) {
	b.mu.RLock("AdminListDevices")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		// AdminListDevices's own deserializer models ResourceNotFoundException,
		// not UserNotFoundException, for a missing user (same as AdminGetDevice).
		return nil, "", fmt.Errorf("%w: user %q not found", ErrDeviceNotFound, username)
	}

	devices, token := b.paginateDevicesLocked(userStateKey(userPoolID, username), limit, nextToken)

	return devices, token, nil
}

// ListDevices returns a page of tracked devices for the authenticated user.
func (b *InMemoryBackend) ListDevices(accessToken string, limit int, nextToken string) ([]*Device, string, error) {
	b.mu.RLock("ListDevices")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, "", err
	}

	devices, token := b.paginateDevicesLocked(userStateKey(user.UserPoolID, user.Username), limit, nextToken)

	return devices, token, nil
}

// validDeviceStatus reports whether status is a recognized DeviceRememberedStatusType value.
func validDeviceStatus(status string) bool {
	return status == DeviceStatusRemembered || status == DeviceStatusNotRemembered
}

// AdminUpdateDeviceStatus updates a tracked device's remembered status (admin operation).
func (b *InMemoryBackend) AdminUpdateDeviceStatus(userPoolID, username, deviceKey, status string) error {
	b.mu.Lock("AdminUpdateDeviceStatus")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !validDeviceStatus(status) {
		return fmt.Errorf("%w: DeviceRememberedStatus must be %q or %q",
			ErrInvalidParameter, DeviceStatusRemembered, DeviceStatusNotRemembered)
	}

	dev, ok := b.devices[userStateKey(userPoolID, username)][deviceKey]
	if !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	dev.Status = status
	dev.LastModifiedAt = time.Now()

	return nil
}

// UpdateDeviceStatus updates a tracked device's remembered status for the authenticated user.
func (b *InMemoryBackend) UpdateDeviceStatus(accessToken, deviceKey, status string) error {
	b.mu.Lock("UpdateDeviceStatus")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	if !validDeviceStatus(status) {
		return fmt.Errorf("%w: DeviceRememberedStatus must be %q or %q",
			ErrInvalidParameter, DeviceStatusRemembered, DeviceStatusNotRemembered)
	}

	dev, ok := b.devices[userStateKey(user.UserPoolID, user.Username)][deviceKey]
	if !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	dev.Status = status
	dev.LastModifiedAt = time.Now()

	return nil
}

// ForgetDevice deletes a tracked device for the authenticated user.
func (b *InMemoryBackend) ForgetDevice(accessToken, deviceKey string) error {
	b.mu.Lock("ForgetDevice")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if _, ok := b.devices[key][deviceKey]; !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	delete(b.devices[key], deviceKey)

	return nil
}
