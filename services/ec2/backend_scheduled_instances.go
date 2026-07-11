package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// backend_scheduled_instances.go implements the Scheduled Instances family: a static
// catalog of purchasable schedules (DescribeScheduledInstanceAvailability), purchasing a
// schedule (PurchaseScheduledInstances) to create a ScheduledInstance, launching instances
// against a purchased schedule (RunScheduledInstances), and listing purchased schedules
// (DescribeScheduledInstances).

// ErrScheduledInstanceNotFound is returned when a Scheduled Instance ID does not exist.
var ErrScheduledInstanceNotFound = errors.New("InvalidScheduledInstance.NotFound")

// ErrScheduledInstancePurchaseToken is returned when a purchase token does not match any
// entry in the static availability catalog.
var ErrScheduledInstancePurchaseToken = errors.New("InvalidParameterValue")

// scheduledInstancePlatform and scheduledInstanceNetworkPlatform are the fixed
// platform/network values used by every catalog entry and purchased schedule in this mock.
const (
	scheduledInstancePlatform        = "Linux/UNIX"
	scheduledInstanceNetworkPlatform = "EC2-VPC"
	scheduledInstanceMaxTermDays     = 365
	scheduledInstanceMinTermDays     = 365
)

// Static availability catalog figures: slot duration / total term hours / available
// instance count per schedule, and the number of days ahead the first slot of each catalog
// entry starts.
const (
	scheduledInstanceFirstSlotOffsetDays = 7

	weeklyCatalogSlotHours      = 24
	weeklyCatalogTotalHours     = 1248
	weeklyCatalogAvailableCount = 10
	weeklyCatalogOccurrenceDay  = 2

	dailyCatalogSlotHours      = 4
	dailyCatalogTotalHours     = 1460
	dailyCatalogAvailableCount = 5

	monthlyCatalogSlotHours      = 100
	monthlyCatalogTotalHours     = 1200
	monthlyCatalogAvailableCount = 3
	monthlyCatalogOccurrenceDay  = 1

	scheduledInstanceRunResyncDays = 7
)

// ScheduledInstanceRecurrence describes the recurring schedule of a Scheduled Instance or
// availability catalog entry.
type ScheduledInstanceRecurrence struct {
	Frequency               string  `json:"frequency,omitempty"`
	OccurrenceUnit          string  `json:"occurrenceUnit,omitempty"`
	OccurrenceDaySet        []int32 `json:"occurrenceDaySet,omitempty"`
	Interval                int32   `json:"interval,omitempty"`
	OccurrenceRelativeToEnd bool    `json:"occurrenceRelativeToEnd,omitempty"`
}

// ScheduledInstanceAvailability describes a single purchasable schedule in the static
// availability catalog returned by DescribeScheduledInstanceAvailability.
type ScheduledInstanceAvailability struct {
	FirstSlotStartTime          time.Time                   `json:"firstSlotStartTime"`
	AvailabilityZone            string                      `json:"availabilityZone,omitempty"`
	InstanceType                string                      `json:"instanceType,omitempty"`
	Platform                    string                      `json:"platform,omitempty"`
	NetworkPlatform             string                      `json:"networkPlatform,omitempty"`
	PurchaseToken               string                      `json:"purchaseToken,omitempty"`
	HourlyPrice                 string                      `json:"hourlyPrice,omitempty"`
	Recurrence                  ScheduledInstanceRecurrence `json:"recurrence"`
	AvailableInstanceCount      int32                       `json:"availableInstanceCount,omitempty"`
	SlotDurationInHours         int32                       `json:"slotDurationInHours,omitempty"`
	TotalScheduledInstanceHours int32                       `json:"totalScheduledInstanceHours,omitempty"`
	MaxTermDurationInDays       int32                       `json:"maxTermDurationInDays,omitempty"`
	MinTermDurationInDays       int32                       `json:"minTermDurationInDays,omitempty"`
}

// ScheduledInstance represents a purchased Scheduled Instance schedule.
type ScheduledInstance struct {
	CreateDate                  time.Time                   `json:"createDate"`
	NextSlotStartTime           time.Time                   `json:"nextSlotStartTime"`
	PreviousSlotEndTime         time.Time                   `json:"previousSlotEndTime"`
	TermStartDate               time.Time                   `json:"termStartDate"`
	TermEndDate                 time.Time                   `json:"termEndDate"`
	ScheduledInstanceID         string                      `json:"scheduledInstanceId,omitempty"`
	AvailabilityZone            string                      `json:"availabilityZone,omitempty"`
	InstanceType                string                      `json:"instanceType,omitempty"`
	Platform                    string                      `json:"platform,omitempty"`
	NetworkPlatform             string                      `json:"networkPlatform,omitempty"`
	HourlyPrice                 string                      `json:"hourlyPrice,omitempty"`
	Recurrence                  ScheduledInstanceRecurrence `json:"recurrence"`
	InstanceCount               int32                       `json:"instanceCount,omitempty"`
	SlotDurationInHours         int32                       `json:"slotDurationInHours,omitempty"`
	TotalScheduledInstanceHours int32                       `json:"totalScheduledInstanceHours,omitempty"`
}

// resetScheduledInstanceMapsLocked re-initialises the Scheduled Instance state maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetScheduledInstanceMapsLocked() {
	b.scheduledInstanceLaunched = make(map[string]int32)
}

// scheduledInstanceCatalog returns the static catalog of purchasable Scheduled Instance
// schedules. Purchase tokens are stable for the lifetime of the process so tests and callers
// can round-trip DescribeScheduledInstanceAvailability -> PurchaseScheduledInstances.
func scheduledInstanceCatalog(region string) []ScheduledInstanceAvailability {
	firstSlot := time.Now().UTC().Truncate(time.Hour).Add(scheduledInstanceFirstSlotOffsetDays * 24 * time.Hour)

	return []ScheduledInstanceAvailability{
		{
			PurchaseToken:               "sit-" + region + "-c4large-weekly",
			AvailabilityZone:            region + "a",
			InstanceType:                "c4.large",
			Platform:                    scheduledInstancePlatform,
			NetworkPlatform:             scheduledInstanceNetworkPlatform,
			HourlyPrice:                 "0.0700",
			SlotDurationInHours:         weeklyCatalogSlotHours,
			TotalScheduledInstanceHours: weeklyCatalogTotalHours,
			MaxTermDurationInDays:       scheduledInstanceMaxTermDays,
			MinTermDurationInDays:       scheduledInstanceMinTermDays,
			AvailableInstanceCount:      weeklyCatalogAvailableCount,
			FirstSlotStartTime:          firstSlot,
			Recurrence: ScheduledInstanceRecurrence{
				Frequency:        "Weekly",
				Interval:         1,
				OccurrenceUnit:   "DayOfWeek",
				OccurrenceDaySet: []int32{weeklyCatalogOccurrenceDay},
			},
		},
		{
			PurchaseToken:               "sit-" + region + "-m4large-daily",
			AvailabilityZone:            region + "b",
			InstanceType:                "m4.large",
			Platform:                    scheduledInstancePlatform,
			NetworkPlatform:             scheduledInstanceNetworkPlatform,
			HourlyPrice:                 "0.0900",
			SlotDurationInHours:         dailyCatalogSlotHours,
			TotalScheduledInstanceHours: dailyCatalogTotalHours,
			MaxTermDurationInDays:       scheduledInstanceMaxTermDays,
			MinTermDurationInDays:       scheduledInstanceMinTermDays,
			AvailableInstanceCount:      dailyCatalogAvailableCount,
			FirstSlotStartTime:          firstSlot,
			Recurrence: ScheduledInstanceRecurrence{
				Frequency: "Daily",
				Interval:  1,
			},
		},
		{
			PurchaseToken:               "sit-" + region + "-r3large-monthly",
			AvailabilityZone:            region + "a",
			InstanceType:                "r3.large",
			Platform:                    scheduledInstancePlatform,
			NetworkPlatform:             scheduledInstanceNetworkPlatform,
			HourlyPrice:                 "0.1750",
			SlotDurationInHours:         monthlyCatalogSlotHours,
			TotalScheduledInstanceHours: monthlyCatalogTotalHours,
			MaxTermDurationInDays:       scheduledInstanceMaxTermDays,
			MinTermDurationInDays:       scheduledInstanceMinTermDays,
			AvailableInstanceCount:      monthlyCatalogAvailableCount,
			FirstSlotStartTime:          firstSlot,
			Recurrence: ScheduledInstanceRecurrence{
				Frequency:        "Monthly",
				Interval:         1,
				OccurrenceUnit:   "DayOfMonth",
				OccurrenceDaySet: []int32{monthlyCatalogOccurrenceDay},
			},
		},
	}
}

// DescribeScheduledInstanceAvailability returns the static catalog entries matching the
// given instance type / availability zone filters and slot duration bounds.
func (b *InMemoryBackend) DescribeScheduledInstanceAvailability(
	filters map[string][]string, minSlotDurationHours, maxSlotDurationHours int32,
) []ScheduledInstanceAvailability {
	b.mu.RLock("DescribeScheduledInstanceAvailability")
	region := b.Region
	b.mu.RUnlock()

	out := make([]ScheduledInstanceAvailability, 0)

	for _, entry := range scheduledInstanceCatalog(region) {
		if !matchesScheduledInstanceFilters(filters, entry.AvailabilityZone, entry.InstanceType, entry.Platform) {
			continue
		}

		if minSlotDurationHours > 0 && entry.SlotDurationInHours < minSlotDurationHours {
			continue
		}

		if maxSlotDurationHours > 0 && entry.SlotDurationInHours > maxSlotDurationHours {
			continue
		}

		out = append(out, entry)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PurchaseToken < out[j].PurchaseToken })

	return out
}

// matchesScheduledInstanceFilters applies the availability-zone / instance-type / platform
// filters accepted by the Scheduled Instance Describe operations.
func matchesScheduledInstanceFilters(filters map[string][]string, az, instanceType, platform string) bool {
	for name, values := range filters {
		var field string

		switch name {
		case "availability-zone":
			field = az
		case "instance-type":
			field = instanceType
		case "platform":
			field = platform
		default:
			continue
		}

		if !slices.Contains(values, field) {
			return false
		}
	}

	return true
}

// PurchaseScheduledInstances purchases one or more schedules from the static availability
// catalog identified by purchase token, creating a ScheduledInstance for each request.
func (b *InMemoryBackend) PurchaseScheduledInstances(
	requests []ScheduledInstancePurchaseRequest,
) ([]*ScheduledInstance, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("%w: PurchaseRequest is required", ErrInvalidParameter)
	}

	b.mu.Lock("PurchaseScheduledInstances")
	defer b.mu.Unlock()

	catalog := make(map[string]ScheduledInstanceAvailability)
	for _, entry := range scheduledInstanceCatalog(b.Region) {
		catalog[entry.PurchaseToken] = entry
	}

	out := make([]*ScheduledInstance, 0, len(requests))

	for _, req := range requests {
		entry, ok := catalog[req.PurchaseToken]
		if !ok {
			return nil, fmt.Errorf(
				"%w: purchase token %q is not valid", ErrScheduledInstancePurchaseToken, req.PurchaseToken,
			)
		}

		instanceCount := req.InstanceCount
		if instanceCount <= 0 {
			instanceCount = 1
		}

		now := time.Now().UTC()
		sci := &ScheduledInstance{
			ScheduledInstanceID:         "sci-" + uuid.New().String()[:17],
			AvailabilityZone:            entry.AvailabilityZone,
			InstanceType:                entry.InstanceType,
			Platform:                    entry.Platform,
			NetworkPlatform:             entry.NetworkPlatform,
			HourlyPrice:                 entry.HourlyPrice,
			InstanceCount:               instanceCount,
			SlotDurationInHours:         entry.SlotDurationInHours,
			TotalScheduledInstanceHours: entry.TotalScheduledInstanceHours,
			Recurrence:                  entry.Recurrence,
			CreateDate:                  now,
			TermStartDate:               entry.FirstSlotStartTime,
			TermEndDate:                 entry.FirstSlotStartTime.AddDate(0, 0, int(scheduledInstanceMaxTermDays)),
			NextSlotStartTime:           entry.FirstSlotStartTime,
		}
		b.scheduledInstances.Put(sci)

		cp := *sci
		out = append(out, &cp)
	}

	return out, nil
}

// ScheduledInstancePurchaseRequest is a single purchase request accepted by
// PurchaseScheduledInstances.
type ScheduledInstancePurchaseRequest struct {
	PurchaseToken string
	InstanceCount int32
}

// DescribeScheduledInstances returns purchased Scheduled Instances, optionally filtered by
// ID.
func (b *InMemoryBackend) DescribeScheduledInstances(ids []string) []*ScheduledInstance {
	b.mu.RLock("DescribeScheduledInstances")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*ScheduledInstance, 0, b.scheduledInstances.Len())

	for _, sci := range b.scheduledInstances.All() {
		if len(filter) > 0 && !filter[sci.ScheduledInstanceID] {
			continue
		}

		cp := *sci
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ScheduledInstanceID < out[j].ScheduledInstanceID })

	return out
}

// RunScheduledInstances launches instanceCount instances against a previously-purchased
// Scheduled Instance, up to its purchased InstanceCount, returning the newly-created
// instance IDs.
func (b *InMemoryBackend) RunScheduledInstances(
	scheduledInstanceID, imageID, keyName string, instanceCount int32,
) ([]string, error) {
	if scheduledInstanceID == "" {
		return nil, fmt.Errorf("%w: ScheduledInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RunScheduledInstances")
	defer b.mu.Unlock()

	sci, ok := b.scheduledInstances.Get(scheduledInstanceID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrScheduledInstanceNotFound, scheduledInstanceID)
	}

	if instanceCount <= 0 {
		instanceCount = 1
	}

	already := b.scheduledInstanceLaunched[scheduledInstanceID]
	if already+instanceCount > sci.InstanceCount {
		return nil, fmt.Errorf(
			"%w: requested count exceeds the purchased InstanceCount for %s", ErrInvalidParameter, scheduledInstanceID,
		)
	}

	now := time.Now().UTC()
	ids := make([]string, 0, instanceCount)

	for range instanceCount {
		instID := "i-" + uuid.New().String()[:17]
		b.instances.Put(&Instance{
			ID:           instID,
			ImageID:      imageID,
			InstanceType: sci.InstanceType,
			KeyName:      keyName,
			LaunchTime:   now,
			State:        StateRunning,
		})
		ids = append(ids, instID)
	}

	b.scheduledInstanceLaunched[scheduledInstanceID] = already + instanceCount
	sci.PreviousSlotEndTime = sci.NextSlotStartTime
	sci.NextSlotStartTime = sci.NextSlotStartTime.AddDate(0, 0, scheduledInstanceRunResyncDays)

	return ids, nil
}
