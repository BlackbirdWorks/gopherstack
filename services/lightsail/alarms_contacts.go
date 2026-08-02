package lightsail

// This file backs family W (8 ops: PutAlarm, DeleteAlarm, GetAlarms,
// TestAlarm, CreateContactMethod, DeleteContactMethod, GetContactMethods,
// SendContactMethodVerification). Automatic threshold evaluation against
// real metric data is NOT implemented here -- PARITY.md 4.8 explicitly
// flags this as the central telemetry-fabrication risk for this family
// (PutAlarm's evaluation is meaningless without real MetricDatapoint values
// this emulator does not honestly have, per family E/L/N/S/T/Q's identical
// empty-MetricData decision). Alarm CRUD/state-storage is fully real;
// TestAlarm (a pure caller-driven State set, not an evaluation) is
// implemented faithfully since it takes an explicit State input.

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opTypePutAlarm                      = "PutAlarm"
	opTypeDeleteAlarm                   = "DeleteAlarm"
	opTypeTestAlarm                     = "TestAlarm"
	opTypeCreateContactMethod           = "CreateContactMethod"
	opTypeDeleteContactMethod           = "DeleteContactMethod"
	opTypeSendContactMethodVerification = "SendContactMethodVerification"
)

// PutAlarm creates or updates the named alarm against monitoredResourceName.
func (b *InMemoryBackend) PutAlarm(
	name, comparisonOperator, metricName, monitoredResourceName, statistic, unit, treatMissingData string,
	threshold float64, evaluationPeriods, datapointsToAlarm int32,
	contactProtocols, notificationTriggers []string, notificationEnabled bool, userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("PutAlarm")
	defer b.mu.Unlock()

	kind, ok := b.activeNames[monitoredResourceName]
	if !ok {
		return nil, notFoundError("monitored resource", monitoredResourceName)
	}

	existing, alreadyExists := b.alarms.Get(name)

	if !alreadyExists {
		if err := b.registerNameLocked(ResourceTypeAlarm, name); err != nil {
			return nil, err
		}
	}

	a := existing
	if a == nil {
		a = &Alarm{
			Name:        name,
			Arn:         b.regionalARN(ResourceTypeAlarm, newUUID()),
			SupportCode: newSupportCode(),
			CreatedAt:   nowUTC(),
			Location:    ResourceLocation{RegionName: b.region, AvailabilityZone: availabilityZoneA(b.region)},
			State:       AlarmStateInsufficientData,
			Tags:        tags.New("lightsail.alarm." + name + ".tags"),
		}
	}

	a.ComparisonOperator = comparisonOperator
	a.MetricName = metricName
	a.MonitoredResourceName = monitoredResourceName
	a.MonitoredResourceArn = kind
	a.Statistic = statistic
	a.Unit = unit
	a.TreatMissingData = treatMissingData
	a.Threshold = threshold
	a.EvaluationPeriods = evaluationPeriods
	a.DatapointsToAlarm = datapointsToAlarm
	a.ContactProtocols = contactProtocols
	a.NotificationTriggers = notificationTriggers
	a.NotificationEnabled = notificationEnabled
	a.Tags.Merge(userTags)

	b.alarms.Put(a)

	return b.newOperationsLocked(opTypePutAlarm, ResourceTypeAlarm, []string{name}), nil
}

// DeleteAlarm deletes the named alarm.
func (b *InMemoryBackend) DeleteAlarm(name string) ([]Operation, error) {
	b.mu.Lock("DeleteAlarm")
	defer b.mu.Unlock()

	a, ok := b.alarms.Get(name)
	if !ok {
		return nil, notFoundError("Alarm", name)
	}

	if a.Tags != nil {
		a.Tags.Close()
	}

	b.alarms.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(opTypeDeleteAlarm, ResourceTypeAlarm, []string{name}), nil
}

// GetAlarms returns alarms, optionally filtered by name and/or
// monitoredResourceName.
func (b *InMemoryBackend) GetAlarms(name, monitoredResourceName string) ([]*Alarm, error) {
	b.mu.RLock("GetAlarms")
	defer b.mu.RUnlock()

	all := b.alarms.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*Alarm, 0, len(all))

	for _, a := range all {
		if name != "" && a.Name != name {
			continue
		}

		if monitoredResourceName != "" && a.MonitoredResourceName != monitoredResourceName {
			continue
		}

		out = append(out, a.clone())
	}

	return out, nil
}

// TestAlarm forces the named alarm to state -- a pure caller-driven state
// set for exercising notification wiring, not a real metric evaluation
// (PARITY.md 4.8).
func (b *InMemoryBackend) TestAlarm(name, state string) ([]Operation, error) {
	b.mu.Lock("TestAlarm")
	defer b.mu.Unlock()

	a, ok := b.alarms.Get(name)
	if !ok {
		return nil, notFoundError("Alarm", name)
	}

	a.State = state

	return b.newOperationsLocked(opTypeTestAlarm, ResourceTypeAlarm, []string{name}), nil
}

// CreateContactMethod creates or replaces the contact method for protocol
// (there is no separate Name concept -- protocol IS the identity,
// PARITY.md 4.8).
func (b *InMemoryBackend) CreateContactMethod(
	protocol, contactEndpoint string,
	userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateContactMethod")
	defer b.mu.Unlock()

	cm := &ContactMethod{
		Protocol:        protocol,
		ContactEndpoint: contactEndpoint,
		Status:          ContactMethodStatusPendingVerification,
		Arn:             b.regionalARN(ResourceTypeContactMethod, newUUID()),
		SupportCode:     newSupportCode(),
		CreatedAt:       nowUTC(),
		Location:        ResourceLocation{RegionName: b.region, AvailabilityZone: availabilityZoneA(b.region)},
		Tags:            tags.New("lightsail.contactmethod." + protocol + ".tags"),
	}
	cm.Tags.Merge(userTags)
	b.contactMethods.Put(cm)

	return b.newOperationsLocked(opTypeCreateContactMethod, ResourceTypeContactMethod, []string{protocol}), nil
}

// DeleteContactMethod deletes the contact method for protocol.
func (b *InMemoryBackend) DeleteContactMethod(protocol string) ([]Operation, error) {
	b.mu.Lock("DeleteContactMethod")
	defer b.mu.Unlock()

	cm, ok := b.contactMethods.Get(protocol)
	if !ok {
		return nil, notFoundError("ContactMethod", protocol)
	}

	if cm.Tags != nil {
		cm.Tags.Close()
	}

	b.contactMethods.Delete(protocol)

	return b.newOperationsLocked(opTypeDeleteContactMethod, ResourceTypeContactMethod, []string{protocol}), nil
}

// GetContactMethods returns contact methods, optionally filtered by
// protocols.
func (b *InMemoryBackend) GetContactMethods(protocols []string) ([]*ContactMethod, error) {
	b.mu.RLock("GetContactMethods")
	defer b.mu.RUnlock()

	want := make(map[string]bool, len(protocols))
	for _, p := range protocols {
		want[p] = true
	}

	all := b.contactMethods.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Protocol < all[j].Protocol })

	out := make([]*ContactMethod, 0, len(all))

	for _, cm := range all {
		if len(want) > 0 && !want[cm.Protocol] {
			continue
		}

		out = append(out, cm.clone())
	}

	return out, nil
}

// SendContactMethodVerification marks the contact method for protocol
// Valid -- this backend never sends a real email/SMS message (PARITY.md
// 4.8: an honest first pass models the status transition as caller-driven,
// clearly documented as not performing real delivery).
func (b *InMemoryBackend) SendContactMethodVerification(protocol string) ([]Operation, error) {
	b.mu.Lock("SendContactMethodVerification")
	defer b.mu.Unlock()

	cm, ok := b.contactMethods.Get(protocol)
	if !ok {
		return nil, notFoundError("ContactMethod", protocol)
	}

	cm.Status = ContactMethodStatusValid

	return b.newOperationsLocked(
		opTypeSendContactMethodVerification,
		ResourceTypeContactMethod,
		[]string{protocol},
	), nil
}
