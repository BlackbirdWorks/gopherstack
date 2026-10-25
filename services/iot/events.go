package iot

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// UpdateEventConfigurations keys that gate registry events.
const millisPerSecond = 1000

const (
	keyClientID    = "clientId"
	keyEventType   = "eventType"
	keyEventID     = "eventId"
	keyOperation   = "operation"
	keyThingID     = "thingId"
	keySessionID   = "sessionIdentifier"
	keyPrincipalID = "principalIdentifier"
)

const (
	eventCatThing            = "THING"
	eventCatThingGroup       = "THING_GROUP"
	eventCatThingType        = "THING_TYPE"
	eventCatMembership       = "THING_GROUP_MEMBERSHIP"
	eventCatHierarchy        = "THING_GROUP_HIERARCHY"
	eventCatTypeAssociation  = "THING_TYPE_ASSOCIATION"
	eventCatJob              = "JOB"
	eventCatJobExecution     = "JOB_EXECUTION"
	eventsTopicPrefix        = "$aws/events/"
	opCreated                = "CREATED"
	opUpdated                = "UPDATED"
	opDeleted                = "DELETED"
	opAdded                  = "ADDED"
	opRemoved                = "REMOVED"
	registryEventTypeThing   = "THING_EVENT"
	registryEventTypeType    = "THING_TYPE_EVENT"
	registryEventTypeGroup   = "THING_GROUP_EVENT"
	registryEventTypeMember  = "THING_GROUP_MEMBERSHIP_EVENT"
	registryEventTypeHier    = "THING_GROUP_HIERARCHY_EVENT"
	registryEventTypeAssoc   = "THING_TYPE_ASSOCIATION_EVENT"
	registryEventTypeJob     = "JOB"
	registryEventTypeJobExec = "JOB_EXECUTION"
)

// registryEvent is one $aws/events message waiting to be published once b.mu is released.
type registryEvent struct {
	payload map[string]any
	topic   string
}

// setEventSink registers the publisher registry events go to (the MQTT broker).
func (b *InMemoryBackend) setEventSink(fn func(topic string, payload []byte)) {
	b.eventSink.Store(&fn)
}

// eventEnabledLocked reports whether UpdateEventConfigurations enabled category; call with b.mu held.
func (b *InMemoryBackend) eventEnabledLocked(category string) bool {
	if b.eventSink.Load() == nil || b.eventConfigurations == nil {
		return false
	}

	e, ok := b.eventConfigurations.EventConfigurations[category]

	return ok && e != nil && e.Enabled
}

// queueEventLocked stages a registry event; the caller must defer flushEvents before taking b.mu.
func (b *InMemoryBackend) queueEventLocked(category, topic string, payload map[string]any) {
	if !b.eventEnabledLocked(category) {
		return
	}

	b.pendingEvents = append(b.pendingEvents, registryEvent{topic: topic, payload: payload})
}

// flushEvents publishes the staged events outside b.mu, so rules they trigger can call back into the backend.
func (b *InMemoryBackend) flushEvents() {
	b.mu.Lock("flushEvents")
	events := b.pendingEvents
	b.pendingEvents = nil
	b.mu.Unlock()

	sink := b.eventSink.Load()
	if sink == nil {
		return
	}

	for _, ev := range events {
		raw, err := json.Marshal(ev.payload)
		if err != nil {
			continue
		}

		(*sink)(ev.topic, raw)
	}
}

func eventTimestamp() int64 { return time.Now().UnixMilli() }

func eventBase(eventType, operation, accountID string) map[string]any {
	return map[string]any{
		keyEventType: eventType,
		keyEventID:   uuid.NewString(),
		keyTimestamp: eventTimestamp(),
		keyOperation: operation,
		"accountId":  accountID,
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func arnOrName(s string) string {
	if !strings.HasPrefix(s, "arn:") {
		return s
	}

	return s[strings.LastIndex(s, "/")+1:]
}

func (b *InMemoryBackend) queueThingEventLocked(op string, t *Thing) {
	if !b.eventEnabledLocked(eventCatThing) {
		return
	}

	p := eventBase(registryEventTypeThing, op, b.accountID)
	p[keyThingID] = t.ThingID
	p["thingName"] = t.ThingName
	p[jobsExecutionStateVersion] = t.Version
	p["thingTypeName"] = nilIfEmpty(t.ThingTypeName)
	p["attributes"] = t.Attributes

	b.queueEventLocked(eventCatThing, eventsTopicPrefix+"thing/"+t.ThingName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) queueTypeAssociationEventLocked(op string, t *Thing, typeName string) {
	if typeName == "" || !b.eventEnabledLocked(eventCatTypeAssociation) {
		return
	}

	p := map[string]any{
		keyEventID:      uuid.NewString(),
		keyEventType:    registryEventTypeAssoc,
		keyOperation:    op,
		keyThingID:      t.ThingID,
		"thingName":     t.ThingName,
		"thingTypeName": typeName,
		keyTimestamp:    eventTimestamp(),
	}

	b.queueEventLocked(eventCatTypeAssociation,
		eventsTopicPrefix+"thingTypeAssociation/thing/"+t.ThingName+"/thingType/"+typeName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) queueThingTypeEventLocked(op string, tt *ThingType) {
	if !b.eventEnabledLocked(eventCatThingType) {
		return
	}

	p := eventBase(registryEventTypeType, op, b.accountID)
	p["thingTypeId"] = tt.ThingTypeID
	p["thingTypeName"] = tt.ThingTypeName
	p["isDeprecated"] = tt.Deprecated
	p["deprecationDate"] = nil

	if tt.Deprecated {
		p["deprecationDate"] = tt.DeprecationDate.UnixMilli()
	}

	p["searchableAttributes"] = append([]string{}, tt.SearchableAttributes...)
	p["description"] = tt.Description

	b.queueEventLocked(eventCatThingType, eventsTopicPrefix+"thingType/"+tt.ThingTypeName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) queueThingGroupEventLocked(op string, tg *ThingGroup) {
	if !b.eventEnabledLocked(eventCatThingGroup) {
		return
	}

	p := eventBase(registryEventTypeGroup, op, b.accountID)
	p["thingGroupId"] = tg.ThingGroupID
	p["thingGroupName"] = tg.ThingGroupName
	p[jobsExecutionStateVersion] = tg.Version
	p["parentGroupName"] = nilIfEmpty(tg.ParentGroupName)
	p["parentGroupId"] = nil
	p["description"] = tg.Description
	p["attributes"] = tg.Attributes
	p["dynamicGroupMappingId"] = nil

	ancestors := []map[string]string{}

	for name := tg.ParentGroupName; name != ""; {
		parent, ok := b.thingGroups.Get(name)
		if !ok {
			break
		}

		if name == tg.ParentGroupName {
			p["parentGroupId"] = parent.ThingGroupID
		}

		ancestors = append(ancestors, map[string]string{
			"groupArn": parent.ThingGroupARN, "groupId": parent.ThingGroupID,
		})
		name = parent.ParentGroupName
	}

	slices.Reverse(ancestors)
	p["rootToParentThingGroups"] = ancestors

	b.queueEventLocked(eventCatThingGroup, eventsTopicPrefix+"thingGroup/"+tg.ThingGroupName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) queueHierarchyEventLocked(op string, child *ThingGroup) {
	if child.ParentGroupName == "" || !b.eventEnabledLocked(eventCatHierarchy) {
		return
	}

	parent, ok := b.thingGroups.Get(child.ParentGroupName)
	if !ok {
		return
	}

	p := eventBase(registryEventTypeHier, op, b.accountID)
	p["thingGroupId"] = parent.ThingGroupID
	p["thingGroupName"] = parent.ThingGroupName
	p["childGroupId"] = child.ThingGroupID
	p["childGroupName"] = child.ThingGroupName

	b.queueEventLocked(eventCatHierarchy, eventsTopicPrefix+"thingGroupHierarchy/thingGroup/"+parent.ThingGroupName+
		"/childThingGroup/"+child.ThingGroupName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) queueMembershipEventLocked(op, groupName, thingName string) {
	if !b.eventEnabledLocked(eventCatMembership) {
		return
	}

	tg, gok := b.thingGroups.Get(arnOrName(groupName))
	t, tok := b.things.Get(arnOrName(thingName))

	if !gok || !tok {
		return
	}

	p := eventBase(registryEventTypeMember, op, b.accountID)
	p["groupArn"] = tg.ThingGroupARN
	p["groupId"] = tg.ThingGroupID
	p["thingArn"] = t.ARN
	p[keyThingID] = t.ThingID
	p["membershipId"] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(tg.ThingGroupID+"/"+t.ThingID)).String()

	b.queueEventLocked(eventCatMembership, eventsTopicPrefix+"thingGroupMembership/thingGroup/"+tg.ThingGroupName+
		"/thing/"+t.ThingName+"/"+strings.ToLower(op), p)
}

func (b *InMemoryBackend) jobEventTargets(j *Job) []string {
	return append([]string{}, j.Targets...)
}

// queueJobEventLocked stages $aws/events/job/<id>/<operation>; operation is the lowercase topic suffix.
func (b *InMemoryBackend) queueJobEventLocked(operation string, j *Job) {
	if !b.eventEnabledLocked(eventCatJob) {
		return
	}

	status := strings.ToUpper(operation)

	p := map[string]any{
		keyEventType:      registryEventTypeJob,
		keyEventID:        uuid.NewString(),
		keyTimestamp:      time.Now().Unix(),
		keyOperation:      operation,
		keyJobID:          j.JobID,
		keyStatus:         status,
		"targetSelection": j.TargetSelection,
		"targets":         b.jobEventTargets(j),
		"description":     j.Description,
		keyCreatedAt:      int64(j.CreatedAt) * millisPerSecond,
		"lastUpdatedAt":   int64(j.LastUpdatedAt) * millisPerSecond,
	}

	switch operation {
	case "completed":
		p["completedAt"] = int64(j.CompletedAt) * millisPerSecond

		d := b.jobProcessDetailsLocked(j.JobID)
		p["jobProcessDetails"] = map[string]int64{
			"numberOfCanceledThings":  d.NumberOfCanceledThings,
			"numberOfRejectedThings":  d.NumberOfRejectedThings,
			"numberOfFailedThings":    d.NumberOfFailedThings,
			"numberOfRemovedThings":   d.NumberOfRemovedThings,
			"numberOfSucceededThings": d.NumberOfSucceededThings,
		}
	case "deleted", "cancellation_in_progress", "deletion_in_progress":
		p["comment"] = j.Comment
	}

	b.queueEventLocked(eventCatJob, eventsTopicPrefix+"job/"+j.JobID+"/"+operation, p)
}

// queueJobExecutionEventLocked stages $aws/events/jobExecution/<jobId>/<operation>.
func (b *InMemoryBackend) queueJobExecutionEventLocked(operation string, e *JobExecution) {
	if !b.eventEnabledLocked(eventCatJobExecution) {
		return
	}

	details := map[string]string{}
	if e.StatusDetails != nil {
		maps.Copy(details, e.StatusDetails.DetailsMap)
	}

	p := map[string]any{
		keyEventType:    registryEventTypeJobExec,
		keyEventID:      uuid.NewString(),
		keyTimestamp:    time.Now().Unix(),
		keyOperation:    operation,
		keyJobID:        e.JobID,
		"thingArn":      b.ThingARN(e.ThingName),
		keyStatus:       strings.ToUpper(operation),
		"statusDetails": details,
	}

	b.queueEventLocked(eventCatJobExecution, eventsTopicPrefix+"jobExecution/"+e.JobID+"/"+operation, p)
}

// queueTerminalJobExecutionEventLocked stages the event for an execution a device just finished.
func (b *InMemoryBackend) queueTerminalJobExecutionEventLocked(e *JobExecution) {
	if isTerminalJobExecutionStatus(e.Status) {
		b.queueJobExecutionEventLocked(strings.ToLower(string(e.Status)), e)
	}
}
