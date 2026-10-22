package connect

import "context"

func allDayConfig() []HoursOfOperationConfig {
	cfg := make([]HoursOfOperationConfig, 0, len(weekDays()))
	for _, d := range weekDays() {
		cfg = append(
			cfg,
			HoursOfOperationConfig{
				Day:       d,
				StartTime: TimeSlice{},
				EndTime:   TimeSlice{Hours: maxHoursInDay, Minutes: maxMinutes},
			},
		)
	}

	return cfg
}

var defaultSecurityProfiles = []struct { //nolint:gochecknoglobals // immutable seed data
	name, description string
	permissions       []string
}{
	{
		"Admin",
		"Administrators have access to everything",
		[]string{
			permBasicAgent,
			permOutboundCalls,
			permMetrics,
			"AgentGrouping.Create",
			"AgentGrouping.Edit",
			"AgentGrouping.View",
			"ContactFlowModules.Create",
			"ContactFlowModules.Edit",
			"ContactFlowModules.View",
			"ContactFlows.Create",
			"ContactFlows.Edit",
			"ContactFlows.View",
			"HoursOfOperation.Create",
			"HoursOfOperation.Edit",
			"HoursOfOperation.View",
			"PhoneNumbers.Create",
			"PhoneNumbers.Edit",
			"PhoneNumbers.View",
			"Queues.Create",
			"Queues.Edit",
			"Queues.View",
			"QuickConnects.Create",
			"QuickConnects.Edit",
			"QuickConnects.View",
			"RoutingPolicies.Create",
			"RoutingPolicies.Edit",
			"RoutingPolicies.View",
			"SecurityProfiles.Create",
			"SecurityProfiles.Edit",
			"SecurityProfiles.View",
			"Users.Create",
			"Users.Edit",
			"Users.View",
		},
	},
	{"Agent", "Agents have access to the Contact Control Panel", []string{permBasicAgent, permOutboundCalls}},
	{
		"CallCenterManager",
		"Call center managers have access to metrics and users",
		[]string{
			permBasicAgent,
			permOutboundCalls,
			permMetrics,
			"AgentGrouping.View",
			"Queues.View",
			"RoutingPolicies.View",
			"Users.View",
		},
	},
	{
		"QualityAnalyst",
		"Quality analysts review recordings and evaluations",
		[]string{permMetrics, "ContactFlows.View"},
	},
}

// provisionDefaults seeds the resources Amazon Connect creates with every new instance.
func (b *Backend) provisionDefaults(ctx context.Context, r *instanceRec) {
	inst := r.Instance.ID

	hours := b.newHours(ctx, inst, "Basic Hours", "", "America/Los_Angeles", allDayConfig(), nil)
	b.hours.put(hours)

	q := &queueRec{InstanceID: inst, Queue: Queue{
		Stamp: b.stamp(ctx), Name: "BasicQueue", Description: "Default queue", Status: queueEnabled,
		HoursOfOperationID: hours.Hours.HoursOfOperationID,
	}}
	q.Queue.QueueID = newID()
	q.Queue.QueueArn = b.buildARN(ctx, "instance/"+inst+"/queue/"+q.Queue.QueueID)
	b.queues.put(q)

	rp := &routingProfileRec{InstanceID: inst, Profile: RoutingProfile{
		Stamp: b.stamp(
			ctx,
		),
		Description:            "Default routing profile",
		InstanceID:             inst,
		Name:                   "Basic Routing Profile",
		IsDefault:              true,
		DefaultOutboundQueueID: q.Queue.QueueID,
		AgentAvailabilityTimer: "TIME_SINCE_LAST_ACTIVITY",
		MediaConcurrencies: []MediaConcurrency{
			{Channel: channelVoice, Concurrency: 1},
			{Channel: channelChat, Concurrency: chatConcurrency},
			{Channel: channelTask, Concurrency: 1},
		},
	}}
	rp.Profile.RoutingProfileID = newID()
	rp.Profile.RoutingProfileArn = b.buildARN(ctx, "instance/"+inst+"/routing-profile/"+rp.Profile.RoutingProfileID)

	for _, ch := range []string{channelVoice, channelChat, channelTask} {
		rp.Queues = append(
			rp.Queues,
			RoutingProfileQueueConfig{
				QueueReference: QueueReference{QueueID: q.Queue.QueueID, Channel: ch},
				Priority:       1,
			},
		)
	}

	b.routingProfiles.put(rp)

	for _, sp := range defaultSecurityProfiles {
		b.securityProfiles.put(b.newSecurityProfile(ctx, inst, sp.name, sp.description, sp.permissions))
	}

	b.agentStatuses.put(b.newAgentStatus(ctx, inst, "Available", "", "ENABLED", "ROUTABLE", orderAvailable, nil))
	b.agentStatuses.put(b.newAgentStatus(ctx, inst, "Offline", "", "ENABLED", "OFFLINE", orderOffline, nil))
	b.provisionFlows(ctx, inst)
}
