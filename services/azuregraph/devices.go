package azuregraph

import (
	"encoding/json/jsontext"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	collDevices    = "devices"
	kindDevice     = "device"
	typeDevice     = "Device"
	maxDevices     = 5000
	maxDeviceName  = 256
	maxDeviceLinks = 100
	relDevOwners   = "registeredOwners"
	relDevUsers    = "registeredUsers"
)

// AlternativeSecurityID is a device alternativeSecurityId (internal use only in Graph).
type AlternativeSecurityID struct {
	IdentityProvider *string `json:"identityProvider"`
	Key              string  `json:"key"`
	Type             int     `json:"type"`
}

// Device is a registered or joined device.
type Device struct {
	RegistrationDateTime          time.Time               `json:"registrationDateTime"`
	ApproximateLastSignInDateTime *time.Time              `json:"approximateLastSignInDateTime"`
	ComplianceExpirationDateTime  *time.Time              `json:"complianceExpirationDateTime"`
	OnPremisesSyncEnabled         *bool                   `json:"onPremisesSyncEnabled"`
	IsCompliant                   *bool                   `json:"isCompliant"`
	IsManaged                     *bool                   `json:"isManaged"`
	IsRooted                      *bool                   `json:"isRooted"`
	DeviceMetadata                *string                 `json:"deviceMetadata"`
	DeviceVersion                 *int                    `json:"deviceVersion"`
	ID                            string                  `json:"id"`
	DeviceID                      string                  `json:"deviceId"`
	DisplayName                   string                  `json:"displayName"`
	DeviceCategory                string                  `json:"deviceCategory,omitempty"`
	DeviceOwnership               string                  `json:"deviceOwnership,omitempty"`
	EnrollmentProfileName         string                  `json:"enrollmentProfileName,omitempty"`
	EnrollmentType                string                  `json:"enrollmentType,omitempty"`
	ManagementType                string                  `json:"managementType,omitempty"`
	Manufacturer                  string                  `json:"manufacturer,omitempty"`
	MDMAppID                      string                  `json:"mdmAppId,omitempty"`
	Model                         string                  `json:"model,omitempty"`
	OperatingSystem               string                  `json:"operatingSystem"`
	OperatingSystemVersion        string                  `json:"operatingSystemVersion"`
	ProfileType                   string                  `json:"profileType,omitempty"`
	TrustType                     string                  `json:"trustType,omitempty"`
	AlternativeSecurityIDs        []AlternativeSecurityID `json:"alternativeSecurityIds"`
	PhysicalIDs                   []string                `json:"physicalIds"`
	SystemLabels                  []string                `json:"systemLabels"`
	AccountEnabled                bool                    `json:"accountEnabled"`
}

func (d *Device) normalize() {
	d.AlternativeSecurityIDs, d.PhysicalIDs = nonNil(d.AlternativeSecurityIDs), nonNil(d.PhysicalIDs)
	d.SystemLabels = nonNil(d.SystemLabels)
}

func (d *Device) objID() string   { return d.ID }
func (d *Device) objKind() string { return kindDevice }
func (d *Device) objName() string { return d.DisplayName }

func deviceFields() map[string]bool { return jsonFields(reflect.TypeFor[Device]()) }

func deviceReadOnly() []string {
	return []string{
		"id", "approximateLastSignInDateTime", "registrationDateTime",
	}
}

// deviceUpdatable is the property set Learn 'Update device' lists as writable.
func deviceUpdatable() []string {
	return []string{
		"accountEnabled",
		"operatingSystem",
		"operatingSystemVersion",
		propDisplayName,
		"isCompliant",
		"isManaged",
	}
}

func oneOfFold(v string, allowed ...string) bool {
	return v == "" || slices.ContainsFunc(allowed, func(a string) bool { return strings.EqualFold(a, v) })
}

func validateDevice(d *Device) error {
	if strings.TrimSpace(d.DisplayName) == "" || len(d.DisplayName) > maxDeviceName {
		return badRequest("Invalid value specified for property 'displayName' of resource '%s'.", typeDevice)
	}

	if !isGUID(d.DeviceID) {
		return badRequest("Invalid value specified for property 'deviceId' of resource '%s'.", typeDevice)
	}

	if !oneOfFold(d.ProfileType, "RegisteredDevice", "SecureVM", "Printer", "Shared", "IoT") ||
		!oneOfFold(d.DeviceOwnership, "unknown", "company", "personal") ||
		!oneOfFold(d.TrustType, "Workplace", "AzureAd", "ServerAd") {
		return badRequest("Invalid value specified for an enumeration property of resource '%s'.", typeDevice)
	}

	return nil
}

func (b *Backend) findDevice(r Ref) (*Device, error) {
	switch {
	case r.Field == "deviceId":
		for _, d := range b.devices.all() {
			if strings.EqualFold(d.DeviceID, r.Value) {
				return d, nil
			}
		}

		return nil, notFound(r.Value)
	case r.Field != "":
		return nil, badRequest("The alternate key '%s' is not supported on devices.", r.Field)
	case !isGUID(r.ID):
		return nil, badRequest("Invalid object identifier '%s'.", r.ID)
	}

	if d, ok := b.devices.get(lc(r.ID)); ok {
		return d, nil
	}

	return nil, notFound(r.ID)
}

func (b *Backend) deviceIDTaken(deviceID, self string) bool {
	return slices.ContainsFunc(b.devices.all(), func(o *Device) bool {
		return o.ID != self && strings.EqualFold(o.DeviceID, deviceID)
	})
}

// CreateDevice registers a device.
func (b *Backend) CreateDevice(body map[string]jsontext.Value) (Device, error) {
	required := []string{"displayName", "deviceId", "operatingSystem", "operatingSystemVersion"}
	if err := requireProps(typeDevice, body, required...); err != nil {
		return Device{}, err
	}

	b.mu.Lock("CreateDevice")
	defer b.mu.Unlock()

	seed := &Device{ID: newID(), RegistrationDateTime: b.stamp(), AccountEnabled: true}
	seed.normalize()

	d, err := applyBody(seed, body, deviceReadOnly()...)
	if err != nil {
		return Device{}, err
	}

	if err = validateDevice(d); err != nil {
		return Device{}, err
	}

	if len(b.devices.order) >= maxDevices {
		return Device{}, badRequest("The maximum number of devices (%d) has been reached.", maxDevices)
	}

	if b.deviceIDTaken(d.DeviceID, d.ID) {
		return Device{}, badRequest("Another object with the same value for property deviceId already exists.")
	}

	d.DeviceID = lc(d.DeviceID)
	b.devices.put(d.ID, d)

	return *d, nil
}

// GetDevice returns a device by object ID or deviceId.
func (b *Backend) GetDevice(r Ref) (Device, error) {
	b.mu.RLock("GetDevice")
	defer b.mu.RUnlock()

	d, err := b.findDevice(r)
	if err != nil {
		return Device{}, err
	}

	return *d, nil
}

// ListDevices returns every device in creation order.
func (b *Backend) ListDevices() []Device {
	b.mu.RLock("ListDevices")
	defer b.mu.RUnlock()

	return b.devices.slice()
}

// UpdateDevice applies a PATCH body; only the properties Learn lists as writable are accepted.
func (b *Backend) UpdateDevice(r Ref, body map[string]jsontext.Value) error {
	fields := deviceFields()

	for k := range body {
		if !strings.Contains(k, "@") && fields[k] && !slices.Contains(deviceUpdatable(), k) {
			return badRequest("Property '%s' is read-only and cannot be set.", k)
		}
	}

	b.mu.Lock("UpdateDevice")
	defer b.mu.Unlock()

	cur, err := b.findDevice(r)
	if err != nil {
		return err
	}

	next, err := applyBody(cur, body, deviceReadOnly()...)
	if err != nil {
		return err
	}

	if err = validateDevice(next); err != nil {
		return err
	}

	b.devices.put(cur.ID, next)

	return nil
}

// DeleteDevice permanently removes a device and its registered owner and user links; Learn lists no soft delete.
func (b *Backend) DeleteDevice(r Ref) error {
	b.mu.Lock("DeleteDevice")
	defer b.mu.Unlock()

	d, err := b.findDevice(r)
	if err != nil {
		return err
	}

	b.devices.del(d.ID)
	delete(b.devOwners, d.ID)
	delete(b.devUsers, d.ID)

	return nil
}

func (b *Backend) deviceLinks(rel string) map[string][]string {
	if rel == relDevOwners {
		return b.devOwners
	}

	return b.devUsers
}

func (b *Backend) editDeviceLink(rel, deviceID, userID string, add bool) error {
	b.mu.Lock("EditDeviceLink")
	defer b.mu.Unlock()

	dev, err := b.findDevice(Ref{ID: deviceID})
	if err != nil {
		return err
	}

	userID = lc(userID)

	if _, ok := b.users.get(userID); !ok {
		if _, other := b.lookup(userID); other {
			return badRequest("Invalid object type for the %s relationship.", rel)
		}

		return notFound(userID)
	}

	links := b.deviceLinks(rel)
	cur := links[dev.ID]

	if !add {
		if !slices.Contains(cur, userID) {
			return notFound(userID)
		}

		links[dev.ID] = without(cur, userID)

		return nil
	}

	if slices.Contains(cur, userID) {
		return badRequest(
			"One or more added object references already exist for the following modified properties: '%s'.",
			rel,
		)
	}

	if len(cur) >= maxDeviceLinks {
		return badRequest("The maximum number of %s (%d) has been reached.", rel, maxDeviceLinks)
	}

	links[dev.ID] = append(slices.Clone(cur), userID)

	return nil
}

func (b *Backend) deviceUsers(rel, deviceID string) ([]Object, error) {
	b.mu.RLock("DeviceUsers")
	defer b.mu.RUnlock()

	dev, err := b.findDevice(Ref{ID: deviceID})
	if err != nil {
		return nil, err
	}

	return b.objects(b.deviceLinks(rel)[dev.ID]), nil
}

func (b *Backend) dropDeviceRefs(id string) {
	for k, v := range b.devOwners {
		b.devOwners[k] = without(v, id)
	}

	for k, v := range b.devUsers {
		b.devUsers[k] = without(v, id)
	}
}

func deviceRefOps(b *Backend, rel string) refOps {
	return refOps{
		add:    func(id, ref string) error { return b.editDeviceLink(rel, id, ref, true) },
		remove: func(id, ref string) error { return b.editDeviceLink(rel, id, ref, false) },
		load:   func(id string) ([]Object, error) { return b.deviceUsers(rel, id) },
	}
}
