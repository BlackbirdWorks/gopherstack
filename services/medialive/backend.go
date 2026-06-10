package medialive

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultMaxResults = 20

	stateIdle     = "IDLE"
	stateRunning  = "RUNNING"
	stateDeleted  = "DELETED"
	stateDeleting = "DELETING"

	stateDetached = "DETACHED"

	channelClassStandard     = "STANDARD"
	inputTypeUDPPush         = "UDP_PUSH"
	inputSecurityGroupActive = "IDLE"

	offeringTypeNoUpfront        = "NO_UPFRONT"
	offeringCurrencyUSD          = "USD"
	offeringDurationMonths       = "MONTHS"
	offeringVideoQualityStandard = "STANDARD"
	offeringUsagePrice           = 0.5
	offeringUsagePrice2          = 1.5
	offeringUsagePrice3          = 0.2
	offeringDuration             = 12
	batchErrNotFound             = "NOT_FOUND"

	resourceTypeChannel            = "channel"
	resourceTypeInput              = "input"
	resourceTypeInputSecurityGroup = "inputSecurityGroup"
	resourceTypeInputDevice        = "inputDevice"
	resourceTypeMultiplex          = "multiplex"
	resourceTypeCluster            = "cluster"
	resourceTypeNode               = "node"

	clusterStateActive   = "ACTIVE"
	clusterStateDeleting = "DELETING"
	clusterStateDeleted  = "DELETED"

	nodeStateActive    = "ACTIVE"
	nodeStateDeleted   = "DELETED"
	nodeRoleActive     = "ACTIVE"
	nodeConnectionConn = "CONNECTED"

	deviceConnectionConnected = "CONNECTED"
	deviceSettingsSynced      = "SYNCED"
	deviceUpdateUpToDate      = "UP_TO_DATE"
	deviceTypeHD              = "HD"
	transferTypeOutgoing      = "OUTGOING"
	transferTypeIncoming      = "INCOMING"
)

// ErrNotFound is returned when a resource does not exist.
var ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)

// ErrConflict is returned for state conflict operations.
var ErrConflict = awserr.New("ConflictException", awserr.ErrAlreadyExists)

// ErrInvalidParameter is returned for invalid input.
var ErrInvalidParameter = awserr.New("BadRequestException", awserr.ErrInvalidParameter)

type storedChannel struct {
	Tags         map[string]string `json:"tags"`
	ARN          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	ChannelClass string            `json:"channelClass"`
	RoleARN      string            `json:"roleArn"`
	State        string            `json:"state"`
}

func (c *storedChannel) toChannel() *Channel {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	return &Channel{
		ARN:          c.ARN,
		ID:           c.ID,
		Name:         c.Name,
		ChannelClass: c.ChannelClass,
		RoleARN:      c.RoleARN,
		State:        c.State,
		Tags:         tags,
	}
}

func (c *storedChannel) toSummary() *ChannelSummary {
	return &ChannelSummary{
		ARN:          c.ARN,
		ID:           c.ID,
		Name:         c.Name,
		ChannelClass: c.ChannelClass,
		State:        c.State,
	}
}

type storedInput struct {
	Tags      map[string]string `json:"tags"`
	ARN       string            `json:"arn"`
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	InputType string            `json:"inputType"`
	RoleARN   string            `json:"roleArn"`
	State     string            `json:"state"`
}

func (i *storedInput) toInput() *Input {
	tags := make(map[string]string, len(i.Tags))
	maps.Copy(tags, i.Tags)

	return &Input{
		ARN:       i.ARN,
		ID:        i.ID,
		Name:      i.Name,
		InputType: i.InputType,
		RoleARN:   i.RoleARN,
		State:     i.State,
		Tags:      tags,
	}
}

func (i *storedInput) toSummary() *InputSummary {
	return &InputSummary{
		ARN:       i.ARN,
		ID:        i.ID,
		Name:      i.Name,
		InputType: i.InputType,
		State:     i.State,
	}
}

type storedInputSecurityGroup struct {
	Tags           map[string]string `json:"tags"`
	ARN            string            `json:"arn"`
	ID             string            `json:"id"`
	State          string            `json:"state"`
	WhitelistRules []WhitelistRule   `json:"whitelistRules"`
}

func (g *storedInputSecurityGroup) toGroup() *InputSecurityGroup {
	tags := make(map[string]string, len(g.Tags))
	maps.Copy(tags, g.Tags)

	rules := make([]WhitelistRule, len(g.WhitelistRules))
	copy(rules, g.WhitelistRules)

	return &InputSecurityGroup{
		ARN:            g.ARN,
		ID:             g.ID,
		State:          g.State,
		WhitelistRules: rules,
		Tags:           tags,
	}
}

func (g *storedInputSecurityGroup) toSummary() *InputSecurityGroupSummary {
	return &InputSecurityGroupSummary{
		ARN:   g.ARN,
		ID:    g.ID,
		State: g.State,
	}
}

// Tags and pointer fields first for optimal field alignment.
type storedInputDevice struct {
	Tags            map[string]string          `json:"tags"`
	PendingTransfer *storedInputDeviceTransfer `json:"pendingTransfer,omitempty"`
	ARN             string                     `json:"arn"`
	ID              string                     `json:"id"`
	Name            string                     `json:"name"`
	SerialNumber    string                     `json:"serialNumber"`
	MacAddress      string                     `json:"macAddress"`
	DeviceType      string                     `json:"deviceType"`
	ConnectionState string                     `json:"connectionState"`
	// DeviceSettingsSyncState and DeviceUpdateStatus: SYNCED/SYNCING, UP_TO_DATE/etc.
	DeviceSettingsSyncState string `json:"deviceSettingsSyncState"`
	DeviceUpdateStatus      string `json:"deviceUpdateStatus"`
}

func (d *storedInputDevice) toDevice() *InputDevice {
	tags := make(map[string]string, len(d.Tags))
	maps.Copy(tags, d.Tags)

	return &InputDevice{
		Tags:                    tags,
		ARN:                     d.ARN,
		ID:                      d.ID,
		Name:                    d.Name,
		SerialNumber:            d.SerialNumber,
		MacAddress:              d.MacAddress,
		DeviceType:              d.DeviceType,
		ConnectionState:         d.ConnectionState,
		DeviceSettingsSyncState: d.DeviceSettingsSyncState,
		DeviceUpdateStatus:      d.DeviceUpdateStatus,
	}
}

func (d *storedInputDevice) toPendingTransfer(transferType string) *InputDeviceTransfer {
	if d.PendingTransfer == nil {
		return nil
	}

	return &InputDeviceTransfer{
		DeviceID:         d.ID,
		TargetCustomerID: d.PendingTransfer.TargetCustomerID,
		TransferType:     transferType,
		Message:          d.PendingTransfer.Message,
	}
}

type storedInputDeviceTransfer struct {
	TargetCustomerID string `json:"targetCustomerId"`
	TargetRegion     string `json:"targetRegion"`
	Message          string `json:"message"`
}

type storedMultiplexSettings struct {
	TransportStreamBitrate              int `json:"transportStreamBitrate"`
	TransportStreamID                   int `json:"transportStreamId"`
	TransportStreamReservedBitrate      int `json:"transportStreamReservedBitrate"`
	MaximumVideoBufferDelayMilliseconds int `json:"maximumVideoBufferDelayMilliseconds"`
}

// Tags and Programs (maps) first, then slice, then strings, then value struct: reduces GC pointer scan.
type storedMultiplex struct {
	Tags              map[string]string                  `json:"tags"`
	Programs          map[string]*storedMultiplexProgram `json:"programs"`
	ARN               string                             `json:"arn"`
	ID                string                             `json:"id"`
	Name              string                             `json:"name"`
	State             string                             `json:"state"`
	AvailabilityZones []string                           `json:"availabilityZones"`
	Settings          storedMultiplexSettings            `json:"settings"`
}

func (m *storedMultiplex) toMultiplex() *Multiplex {
	tags := make(map[string]string, len(m.Tags))
	maps.Copy(tags, m.Tags)

	zones := make([]string, len(m.AvailabilityZones))
	copy(zones, m.AvailabilityZones)

	return &Multiplex{
		Tags:              tags,
		AvailabilityZones: zones,
		ARN:               m.ARN,
		ID:                m.ID,
		Name:              m.Name,
		State:             m.State,
		Settings: MultiplexSettings{
			TransportStreamBitrate:              m.Settings.TransportStreamBitrate,
			TransportStreamID:                   m.Settings.TransportStreamID,
			TransportStreamReservedBitrate:      m.Settings.TransportStreamReservedBitrate,
			MaximumVideoBufferDelayMilliseconds: m.Settings.MaximumVideoBufferDelayMilliseconds,
		},
	}
}

func (m *storedMultiplex) toSummary() *MultiplexSummary {
	zones := make([]string, len(m.AvailabilityZones))
	copy(zones, m.AvailabilityZones)

	return &MultiplexSummary{
		ARN:               m.ARN,
		ID:                m.ID,
		Name:              m.Name,
		State:             m.State,
		AvailabilityZones: zones,
	}
}

type storedServiceDescriptor struct {
	ProviderName string `json:"providerName"`
	ServiceName  string `json:"serviceName"`
}

type storedMultiplexProgramSettings struct {
	ServiceDescriptor        storedServiceDescriptor `json:"serviceDescriptor"`
	PreferredChannelPipeline string                  `json:"preferredChannelPipeline"`
	ProgramNumber            int                     `json:"programNumber"`
}

// Strings first, value struct last: reduces GC pointer scan.
type storedMultiplexProgram struct {
	ProgramName string                         `json:"programName"`
	ChannelID   string                         `json:"channelId"`
	Settings    storedMultiplexProgramSettings `json:"settings"`
}

func (p *storedMultiplexProgram) toProgram() *MultiplexProgram {
	return &MultiplexProgram{
		ChannelID:   p.ChannelID,
		ProgramName: p.ProgramName,
		Settings: MultiplexProgramSettings{
			ProgramName:              p.ProgramName,
			ProgramNumber:            p.Settings.ProgramNumber,
			PreferredChannelPipeline: p.Settings.PreferredChannelPipeline,
			ServiceDescriptor: ServiceDescriptor{
				ProviderName: p.Settings.ServiceDescriptor.ProviderName,
				ServiceName:  p.Settings.ServiceDescriptor.ServiceName,
			},
		},
	}
}

func (p *storedMultiplexProgram) toSummary() *MultiplexProgramSummary {
	return &MultiplexProgramSummary{
		ProgramName: p.ProgramName,
		ChannelID:   p.ChannelID,
	}
}

// Tags and Nodes (maps) first, then strings: reduces GC pointer scan.
type storedCluster struct {
	Tags            map[string]string      `json:"tags"`
	Nodes           map[string]*storedNode `json:"nodes"`
	ARN             string                 `json:"arn"`
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	ClusterType     string                 `json:"clusterType"`
	InstanceRoleArn string                 `json:"instanceRoleArn"`
	State           string                 `json:"state"`
}

func (c *storedCluster) toCluster() *Cluster {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	return &Cluster{
		Tags:            tags,
		ARN:             c.ARN,
		ID:              c.ID,
		Name:            c.Name,
		ClusterType:     c.ClusterType,
		InstanceRoleArn: c.InstanceRoleArn,
		State:           c.State,
	}
}

func (c *storedCluster) toSummary() *ClusterSummary {
	return &ClusterSummary{
		ARN:             c.ARN,
		ID:              c.ID,
		Name:            c.Name,
		ClusterType:     c.ClusterType,
		InstanceRoleArn: c.InstanceRoleArn,
		State:           c.State,
	}
}

// Tags first, then strings: reduces GC pointer scan.
type storedNode struct {
	Tags            map[string]string `json:"tags"`
	ARN             string            `json:"arn"`
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	ClusterID       string            `json:"clusterId"`
	Role            string            `json:"role"`
	State           string            `json:"state"`
	ConnectionState string            `json:"connectionState"`
}

func (n *storedNode) toNode() *Node {
	tags := make(map[string]string, len(n.Tags))
	maps.Copy(tags, n.Tags)

	return &Node{
		Tags:            tags,
		ARN:             n.ARN,
		ID:              n.ID,
		Name:            n.Name,
		ClusterID:       n.ClusterID,
		Role:            n.Role,
		State:           n.State,
		ConnectionState: n.ConnectionState,
	}
}

func (n *storedNode) toSummary() *NodeSummary {
	return &NodeSummary{
		ARN:             n.ARN,
		ID:              n.ID,
		Name:            n.Name,
		ClusterID:       n.ClusterID,
		Role:            n.Role,
		State:           n.State,
		ConnectionState: n.ConnectionState,
	}
}

type storedSignalMap struct {
	Tags                            map[string]string `json:"tags"`
	Arn                             string            `json:"arn"`
	ID                              string            `json:"id"`
	Name                            string            `json:"name"`
	Description                     string            `json:"description"`
	DiscoveryEntryPointArn          string            `json:"discoveryEntryPointArn"`
	Status                          string            `json:"status"`
	MonitorDeploymentStatus         string            `json:"monitorDeploymentStatus"`
	CloudWatchAlarmTemplateGroupIDs []string          `json:"cloudWatchAlarmTemplateGroupIds"`
	EventBridgeRuleTemplateGroupIDs []string          `json:"eventBridgeRuleTemplateGroupIds"`
}

func (s *storedSignalMap) toSignalMap() *SignalMap {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)
	cwIDs := make([]string, len(s.CloudWatchAlarmTemplateGroupIDs))
	copy(cwIDs, s.CloudWatchAlarmTemplateGroupIDs)
	ebIDs := make([]string, len(s.EventBridgeRuleTemplateGroupIDs))
	copy(ebIDs, s.EventBridgeRuleTemplateGroupIDs)

	return &SignalMap{
		Tags:                            tags,
		CloudWatchAlarmTemplateGroupIDs: cwIDs,
		EventBridgeRuleTemplateGroupIDs: ebIDs,
		Arn:                             s.Arn,
		ID:                              s.ID,
		Name:                            s.Name,
		Description:                     s.Description,
		DiscoveryEntryPointArn:          s.DiscoveryEntryPointArn,
		Status:                          s.Status,
		MonitorDeploymentStatus:         s.MonitorDeploymentStatus,
	}
}

type storedCloudWatchAlarmTemplateGroup struct {
	Tags        map[string]string `json:"tags"`
	Arn         string            `json:"arn"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
}

func (g *storedCloudWatchAlarmTemplateGroup) toGroup() *CloudWatchAlarmTemplateGroup {
	tags := make(map[string]string, len(g.Tags))
	maps.Copy(tags, g.Tags)

	return &CloudWatchAlarmTemplateGroup{
		Tags:        tags,
		Arn:         g.Arn,
		ID:          g.ID,
		Name:        g.Name,
		Description: g.Description,
	}
}

type storedCloudWatchAlarmTemplate struct {
	Tags               map[string]string `json:"tags"`
	Arn                string            `json:"arn"`
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	GroupID            string            `json:"groupId"`
	GroupIdentifier    string            `json:"groupIdentifier"`
	MetricName         string            `json:"metricName"`
	Namespace          string            `json:"namespace"`
	Statistic          string            `json:"statistic"`
	ComparisonOperator string            `json:"comparisonOperator"`
	TargetResourceType string            `json:"targetResourceType"`
	TreatMissingData   string            `json:"treatMissingData"`
	Threshold          float64           `json:"threshold"`
	EvaluationPeriods  int32             `json:"evaluationPeriods"`
	DatapointsToAlarm  int32             `json:"datapointsToAlarm"`
	Period             int32             `json:"period"`
}

func (t *storedCloudWatchAlarmTemplate) toTemplate() *CloudWatchAlarmTemplate {
	tags := make(map[string]string, len(t.Tags))
	maps.Copy(tags, t.Tags)

	return &CloudWatchAlarmTemplate{
		Tags: tags, Arn: t.Arn, ID: t.ID, Name: t.Name, Description: t.Description,
		GroupID: t.GroupID, GroupIdentifier: t.GroupIdentifier,
		MetricName: t.MetricName, Namespace: t.Namespace, Statistic: t.Statistic,
		ComparisonOperator: t.ComparisonOperator, TargetResourceType: t.TargetResourceType,
		TreatMissingData: t.TreatMissingData, Threshold: t.Threshold,
		EvaluationPeriods: t.EvaluationPeriods, DatapointsToAlarm: t.DatapointsToAlarm, Period: t.Period,
	}
}

type storedEventBridgeRuleTemplateGroup struct {
	Tags        map[string]string `json:"tags"`
	Arn         string            `json:"arn"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
}

func (g *storedEventBridgeRuleTemplateGroup) toGroup() *EventBridgeRuleTemplateGroup {
	tags := make(map[string]string, len(g.Tags))
	maps.Copy(tags, g.Tags)

	return &EventBridgeRuleTemplateGroup{
		Tags:        tags,
		Arn:         g.Arn,
		ID:          g.ID,
		Name:        g.Name,
		Description: g.Description,
	}
}

type storedEventBridgeRuleTemplate struct {
	Tags            map[string]string               `json:"tags"`
	Arn             string                          `json:"arn"`
	ID              string                          `json:"id"`
	Name            string                          `json:"name"`
	Description     string                          `json:"description"`
	GroupID         string                          `json:"groupId"`
	GroupIdentifier string                          `json:"groupIdentifier"`
	EventType       string                          `json:"eventType"`
	EventTargets    []EventBridgeRuleTemplateTarget `json:"eventTargets"`
}

func (t *storedEventBridgeRuleTemplate) toTemplate() *EventBridgeRuleTemplate {
	tags := make(map[string]string, len(t.Tags))
	maps.Copy(tags, t.Tags)
	targets := make([]EventBridgeRuleTemplateTarget, len(t.EventTargets))
	copy(targets, t.EventTargets)

	return &EventBridgeRuleTemplate{
		Tags: tags, EventTargets: targets, Arn: t.Arn, ID: t.ID, Name: t.Name,
		Description: t.Description, GroupID: t.GroupID, GroupIdentifier: t.GroupIdentifier,
		EventType: t.EventType,
	}
}

type storedReservation struct {
	Tags                  map[string]string             `json:"tags"`
	ResourceSpecification OfferingResourceSpecification `json:"resourceSpecification"`
	Arn                   string                        `json:"arn"`
	ReservationID         string                        `json:"reservationId"`
	Name                  string                        `json:"name"`
	OfferingID            string                        `json:"offeringId"`
	OfferingDescription   string                        `json:"offeringDescription"`
	OfferingType          string                        `json:"offeringType"`
	CurrencyCode          string                        `json:"currencyCode"`
	Start                 string                        `json:"start"`
	End                   string                        `json:"end"`
	Region                string                        `json:"region"`
	State                 string                        `json:"state"`
	DurationUnits         string                        `json:"durationUnits"`
	FixedPrice            float64                       `json:"fixedPrice"`
	UsagePrice            float64                       `json:"usagePrice"`
	Duration              int32                         `json:"duration"`
	Count                 int32                         `json:"count"`
}

func (r *storedReservation) toReservation() *Reservation {
	tags := make(map[string]string, len(r.Tags))
	maps.Copy(tags, r.Tags)

	return &Reservation{
		Tags: tags, ResourceSpecification: r.ResourceSpecification,
		Arn: r.Arn, ReservationID: r.ReservationID, Name: r.Name,
		OfferingID: r.OfferingID, OfferingDescription: r.OfferingDescription,
		OfferingType: r.OfferingType, CurrencyCode: r.CurrencyCode,
		Start: r.Start, End: r.End, Region: r.Region, State: r.State,
		FixedPrice: r.FixedPrice, UsagePrice: r.UsagePrice,
		Duration: r.Duration, DurationUnits: r.DurationUnits, Count: r.Count,
	}
}

// storedScheduleAction persists one schedule action for a channel.
type storedScheduleAction struct {
	ActionName string `json:"actionName"`
	ActionType string `json:"actionType"`
}

type snapshot struct {
	Channels              map[string]*storedChannel                      `json:"channels"`
	Inputs                map[string]*storedInput                        `json:"inputs"`
	InputSecurityGroups   map[string]*storedInputSecurityGroup           `json:"inputSecurityGroups"`
	InputDevices          map[string]*storedInputDevice                  `json:"inputDevices"`
	Multiplexes           map[string]*storedMultiplex                    `json:"multiplexes"`
	Clusters              map[string]*storedCluster                      `json:"clusters"`
	Tags                  map[string]map[string]string                   `json:"tags"`
	SignalMaps            map[string]*storedSignalMap                    `json:"signalMaps"`
	CWAlarmTemplateGroups map[string]*storedCloudWatchAlarmTemplateGroup `json:"cwAlarmTemplateGroups"`
	CWAlarmTemplates      map[string]*storedCloudWatchAlarmTemplate      `json:"cwAlarmTemplates"`
	EBRuleTemplateGroups  map[string]*storedEventBridgeRuleTemplateGroup `json:"ebRuleTemplateGroups"`
	EBRuleTemplates       map[string]*storedEventBridgeRuleTemplate      `json:"ebRuleTemplates"`
	Reservations          map[string]*storedReservation                  `json:"reservations"`
	ScheduleActions       map[string][]*storedScheduleAction             `json:"scheduleActions"`
	AccountID             string                                         `json:"accountId"`
	Region                string                                         `json:"region"`
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu                    *lockmetrics.RWMutex
	channels              map[string]*storedChannel
	inputs                map[string]*storedInput
	inputSecurityGroups   map[string]*storedInputSecurityGroup
	inputDevices          map[string]*storedInputDevice
	multiplexes           map[string]*storedMultiplex
	clusters              map[string]*storedCluster
	tags                  map[string]map[string]string
	signalMaps            map[string]*storedSignalMap
	cwAlarmTemplateGroups map[string]*storedCloudWatchAlarmTemplateGroup
	cwAlarmTemplates      map[string]*storedCloudWatchAlarmTemplate
	ebRuleTemplateGroups  map[string]*storedEventBridgeRuleTemplateGroup
	ebRuleTemplates       map[string]*storedEventBridgeRuleTemplate
	reservations          map[string]*storedReservation
	scheduleActions       map[string][]*storedScheduleAction
	offerings             []*Offering
	accountID             string
	region                string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                    lockmetrics.New("medialive"),
		channels:              make(map[string]*storedChannel),
		inputs:                make(map[string]*storedInput),
		inputSecurityGroups:   make(map[string]*storedInputSecurityGroup),
		inputDevices:          make(map[string]*storedInputDevice),
		multiplexes:           make(map[string]*storedMultiplex),
		clusters:              make(map[string]*storedCluster),
		tags:                  make(map[string]map[string]string),
		signalMaps:            make(map[string]*storedSignalMap),
		cwAlarmTemplateGroups: make(map[string]*storedCloudWatchAlarmTemplateGroup),
		cwAlarmTemplates:      make(map[string]*storedCloudWatchAlarmTemplate),
		ebRuleTemplateGroups:  make(map[string]*storedEventBridgeRuleTemplateGroup),
		ebRuleTemplates:       make(map[string]*storedEventBridgeRuleTemplate),
		reservations:          make(map[string]*storedReservation),
		scheduleActions:       make(map[string][]*storedScheduleAction),
		offerings:             seedOfferings(region),
		accountID:             accountID,
		region:                region,
	}
}

// seedOfferings returns a small catalog of standard offerings.
func seedOfferings(region string) []*Offering {
	hd := OfferingResourceSpecification{
		ResourceType: "OUTPUT", VideoQuality: offeringVideoQualityStandard, Resolution: "HD",
		MaximumBitrate: "MAX_20_MBPS", MaximumFramerate: "MAX_30_FPS", Codec: "AVC",
	}
	uhd := OfferingResourceSpecification{
		ResourceType: "OUTPUT", VideoQuality: offeringVideoQualityStandard, Resolution: "UHD",
		MaximumBitrate: "MAX_50_MBPS", MaximumFramerate: "MAX_60_FPS", Codec: "HEVC",
	}
	input := OfferingResourceSpecification{
		ResourceType: "INPUT", VideoQuality: offeringVideoQualityStandard, Resolution: "HD",
		MaximumBitrate: "MAX_20_MBPS", MaximumFramerate: "MAX_30_FPS", Codec: "AVC",
	}

	return []*Offering{
		{
			OfferingID:            "87654321",
			Arn:                   "arn:aws:medialive:" + region + "::offering:87654321",
			OfferingDescription:   "HD AVC output at 10-20 Mbps, 30 fps, standard VQ in " + region,
			OfferingType:          offeringTypeNoUpfront,
			CurrencyCode:          offeringCurrencyUSD,
			FixedPrice:            0.0,
			UsagePrice:            offeringUsagePrice,
			Duration:              offeringDuration,
			DurationUnits:         offeringDurationMonths,
			ResourceSpecification: hd,
		},
		{
			OfferingID:            "12345678",
			Arn:                   "arn:aws:medialive:" + region + "::offering:12345678",
			OfferingDescription:   "UHD HEVC output at 20-50 Mbps, 60 fps, standard VQ in " + region,
			OfferingType:          offeringTypeNoUpfront,
			CurrencyCode:          offeringCurrencyUSD,
			FixedPrice:            0.0,
			UsagePrice:            offeringUsagePrice2,
			Duration:              offeringDuration,
			DurationUnits:         offeringDurationMonths,
			ResourceSpecification: uhd,
		},
		{
			OfferingID:            "11223344",
			Arn:                   "arn:aws:medialive:" + region + "::offering:11223344",
			OfferingDescription:   "HD AVC input at 10-20 Mbps, 30 fps, standard VQ in " + region,
			OfferingType:          offeringTypeNoUpfront,
			CurrencyCode:          offeringCurrencyUSD,
			FixedPrice:            0.0,
			UsagePrice:            offeringUsagePrice3,
			Duration:              offeringDuration,
			DurationUnits:         offeringDurationMonths,
			ResourceSpecification: input,
		},
	}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.channels = make(map[string]*storedChannel)
	b.inputs = make(map[string]*storedInput)
	b.inputSecurityGroups = make(map[string]*storedInputSecurityGroup)
	b.inputDevices = make(map[string]*storedInputDevice)
	b.multiplexes = make(map[string]*storedMultiplex)
	b.clusters = make(map[string]*storedCluster)
	b.tags = make(map[string]map[string]string)
	b.signalMaps = make(map[string]*storedSignalMap)
	b.cwAlarmTemplateGroups = make(map[string]*storedCloudWatchAlarmTemplateGroup)
	b.cwAlarmTemplates = make(map[string]*storedCloudWatchAlarmTemplate)
	b.ebRuleTemplateGroups = make(map[string]*storedEventBridgeRuleTemplateGroup)
	b.ebRuleTemplates = make(map[string]*storedEventBridgeRuleTemplate)
	b.reservations = make(map[string]*storedReservation)
	b.scheduleActions = make(map[string][]*storedScheduleAction)
}

// Snapshot serializes current state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	s := snapshot{
		Channels:              b.channels,
		Inputs:                b.inputs,
		InputSecurityGroups:   b.inputSecurityGroups,
		InputDevices:          b.inputDevices,
		Multiplexes:           b.multiplexes,
		Clusters:              b.clusters,
		Tags:                  b.tags,
		SignalMaps:            b.signalMaps,
		CWAlarmTemplateGroups: b.cwAlarmTemplateGroups,
		CWAlarmTemplates:      b.cwAlarmTemplates,
		EBRuleTemplateGroups:  b.ebRuleTemplateGroups,
		EBRuleTemplates:       b.ebRuleTemplates,
		Reservations:          b.reservations,
		ScheduleActions:       b.scheduleActions,
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, _ := json.Marshal(s)

	return data
}

// Restore deserializes state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.channels = s.Channels
	b.inputs = s.Inputs
	b.inputSecurityGroups = s.InputSecurityGroups
	if s.InputDevices != nil {
		b.inputDevices = s.InputDevices
	} else {
		b.inputDevices = make(map[string]*storedInputDevice)
	}
	b.multiplexes = s.Multiplexes
	if s.Clusters != nil {
		b.clusters = s.Clusters
	} else {
		b.clusters = make(map[string]*storedCluster)
	}
	b.tags = s.Tags
	if s.SignalMaps != nil {
		b.signalMaps = s.SignalMaps
	} else {
		b.signalMaps = make(map[string]*storedSignalMap)
	}
	if s.CWAlarmTemplateGroups != nil {
		b.cwAlarmTemplateGroups = s.CWAlarmTemplateGroups
	} else {
		b.cwAlarmTemplateGroups = make(map[string]*storedCloudWatchAlarmTemplateGroup)
	}
	if s.CWAlarmTemplates != nil {
		b.cwAlarmTemplates = s.CWAlarmTemplates
	} else {
		b.cwAlarmTemplates = make(map[string]*storedCloudWatchAlarmTemplate)
	}
	if s.EBRuleTemplateGroups != nil {
		b.ebRuleTemplateGroups = s.EBRuleTemplateGroups
	} else {
		b.ebRuleTemplateGroups = make(map[string]*storedEventBridgeRuleTemplateGroup)
	}
	if s.EBRuleTemplates != nil {
		b.ebRuleTemplates = s.EBRuleTemplates
	} else {
		b.ebRuleTemplates = make(map[string]*storedEventBridgeRuleTemplate)
	}
	if s.Reservations != nil {
		b.reservations = s.Reservations
	} else {
		b.reservations = make(map[string]*storedReservation)
	}
	if s.ScheduleActions != nil {
		b.scheduleActions = s.ScheduleActions
	} else {
		b.scheduleActions = make(map[string][]*storedScheduleAction)
	}
	b.accountID = s.AccountID
	b.region = s.Region

	return nil
}

func (b *InMemoryBackend) channelARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeChannel+":"+id)
}

func (b *InMemoryBackend) inputARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeInput+":"+id)
}

func (b *InMemoryBackend) inputSecurityGroupARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeInputSecurityGroup+":"+id)
}

func (b *InMemoryBackend) inputDeviceARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeInputDevice+":"+id)
}

func (b *InMemoryBackend) signalMapARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "signal-map:"+id)
}

func (b *InMemoryBackend) cwAlarmTemplateGroupARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "cloudwatch-alarm-template-group:"+id)
}

func (b *InMemoryBackend) cwAlarmTemplateARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "cloudwatch-alarm-template:"+id)
}

func (b *InMemoryBackend) ebRuleTemplateGroupARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "eventbridge-rule-template-group:"+id)
}

func (b *InMemoryBackend) ebRuleTemplateARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "eventbridge-rule-template:"+id)
}

func (b *InMemoryBackend) reservationARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, "reservation:"+id)
}

func newID() string {
	return uuid.New().String()[:8]
}

// --- Channel operations ---

// CreateChannel creates a new channel.
func (b *InMemoryBackend) CreateChannel(
	name, channelClass, roleArn string,
	tags map[string]string,
) (*Channel, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if channelClass == "" {
		channelClass = channelClassStandard
	}

	id := newID()
	ch := &storedChannel{
		ARN:          b.channelARN(id),
		ID:           id,
		Name:         name,
		ChannelClass: channelClass,
		RoleARN:      roleArn,
		State:        stateIdle,
		Tags:         copyTags(tags),
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	b.channels[id] = ch

	return ch.toChannel(), nil
}

// DescribeChannel returns a channel by ID.
func (b *InMemoryBackend) DescribeChannel(channelID string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return ch.toChannel(), nil
}

// UpdateChannel updates a channel's mutable fields.
func (b *InMemoryBackend) UpdateChannel(channelID, name, roleArn string) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if name != "" {
		ch.Name = name
	}

	if roleArn != "" {
		ch.RoleARN = roleArn
	}

	return ch.toChannel(), nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(channelID string) (*Channel, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State == stateRunning {
		return nil, fmt.Errorf("%w: channel must be idle before deleting", ErrConflict)
	}

	ch.State = stateDeleted
	delete(b.channels, channelID)

	return ch.toChannel(), nil
}

// ListChannels returns a paginated list of channels.
func (b *InMemoryBackend) ListChannels(
	maxResults int,
	nextToken string,
) ([]*ChannelSummary, string, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	all := make([]*storedChannel, 0, len(b.channels))
	for _, ch := range b.channels {
		all = append(all, ch)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ChannelSummary, 0, len(pg.Data))
	for _, ch := range pg.Data {
		summaries = append(summaries, ch.toSummary())
	}

	return summaries, pg.Next, nil
}

// StartChannel transitions a channel to RUNNING.
func (b *InMemoryBackend) StartChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StartChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateIdle {
		return nil, fmt.Errorf("%w: channel must be idle to start", ErrConflict)
	}

	ch.State = stateRunning

	return ch.toChannel(), nil
}

// StopChannel transitions a channel to IDLE.
func (b *InMemoryBackend) StopChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StopChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateRunning {
		return nil, fmt.Errorf("%w: channel must be running to stop", ErrConflict)
	}

	ch.State = stateIdle

	return ch.toChannel(), nil
}

// --- Input operations ---

// CreateInput creates a new input.
func (b *InMemoryBackend) CreateInput(
	name, inputType, roleArn string,
	tags map[string]string,
) (*Input, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if inputType == "" {
		inputType = inputTypeUDPPush
	}

	id := newID()
	inp := &storedInput{
		ARN:       b.inputARN(id),
		ID:        id,
		Name:      name,
		InputType: inputType,
		RoleARN:   roleArn,
		State:     stateDetached,
		Tags:      copyTags(tags),
	}

	b.mu.Lock("CreateInput")
	defer b.mu.Unlock()

	b.inputs[id] = inp

	return inp.toInput(), nil
}

// DescribeInput returns an input by ID.
func (b *InMemoryBackend) DescribeInput(inputID string) (*Input, error) {
	b.mu.RLock("DescribeInput")
	defer b.mu.RUnlock()

	inp, ok := b.inputs[inputID]
	if !ok {
		return nil, fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	return inp.toInput(), nil
}

// UpdateInput updates an input's mutable fields.
func (b *InMemoryBackend) UpdateInput(inputID, name, roleArn string) (*Input, error) {
	b.mu.Lock("UpdateInput")
	defer b.mu.Unlock()

	inp, ok := b.inputs[inputID]
	if !ok {
		return nil, fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	if name != "" {
		inp.Name = name
	}

	if roleArn != "" {
		inp.RoleARN = roleArn
	}

	return inp.toInput(), nil
}

// DeleteInput deletes an input.
func (b *InMemoryBackend) DeleteInput(inputID string) error {
	b.mu.Lock("DeleteInput")
	defer b.mu.Unlock()

	if _, ok := b.inputs[inputID]; !ok {
		return fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	delete(b.inputs, inputID)

	return nil
}

// ListInputs returns a paginated list of inputs.
func (b *InMemoryBackend) ListInputs(
	maxResults int,
	nextToken string,
) ([]*InputSummary, string, error) {
	b.mu.RLock("ListInputs")
	defer b.mu.RUnlock()

	all := make([]*storedInput, 0, len(b.inputs))
	for _, inp := range b.inputs {
		all = append(all, inp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSummary, 0, len(pg.Data))
	for _, inp := range pg.Data {
		summaries = append(summaries, inp.toSummary())
	}

	return summaries, pg.Next, nil
}

// --- InputSecurityGroup operations ---

// CreateInputSecurityGroup creates a new input security group.
func (b *InMemoryBackend) CreateInputSecurityGroup(
	whitelistRules []WhitelistRule,
	tags map[string]string,
) (*InputSecurityGroup, error) {
	id := newID()
	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g := &storedInputSecurityGroup{
		ARN:            b.inputSecurityGroupARN(id),
		ID:             id,
		State:          inputSecurityGroupActive,
		WhitelistRules: rules,
		Tags:           copyTags(tags),
	}

	b.mu.Lock("CreateInputSecurityGroup")
	defer b.mu.Unlock()

	b.inputSecurityGroups[id] = g

	return g.toGroup(), nil
}

// DescribeInputSecurityGroup returns an input security group by ID.
func (b *InMemoryBackend) DescribeInputSecurityGroup(groupID string) (*InputSecurityGroup, error) {
	b.mu.RLock("DescribeInputSecurityGroup")
	defer b.mu.RUnlock()

	g, ok := b.inputSecurityGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	return g.toGroup(), nil
}

// UpdateInputSecurityGroup updates an input security group's whitelist rules.
func (b *InMemoryBackend) UpdateInputSecurityGroup(
	groupID string,
	whitelistRules []WhitelistRule,
) (*InputSecurityGroup, error) {
	b.mu.Lock("UpdateInputSecurityGroup")
	defer b.mu.Unlock()

	g, ok := b.inputSecurityGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g.WhitelistRules = rules

	return g.toGroup(), nil
}

// DeleteInputSecurityGroup deletes an input security group.
func (b *InMemoryBackend) DeleteInputSecurityGroup(groupID string) error {
	b.mu.Lock("DeleteInputSecurityGroup")
	defer b.mu.Unlock()

	if _, ok := b.inputSecurityGroups[groupID]; !ok {
		return fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	delete(b.inputSecurityGroups, groupID)

	return nil
}

// ListInputSecurityGroups returns a paginated list of input security groups.
func (b *InMemoryBackend) ListInputSecurityGroups(
	maxResults int,
	nextToken string,
) ([]*InputSecurityGroupSummary, string, error) {
	b.mu.RLock("ListInputSecurityGroups")
	defer b.mu.RUnlock()

	all := make([]*storedInputSecurityGroup, 0, len(b.inputSecurityGroups))
	for _, g := range b.inputSecurityGroups {
		all = append(all, g)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSecurityGroupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		summaries = append(summaries, g.toSummary())
	}

	return summaries, pg.Next, nil
}

// --- Tag operations ---

// CreateTags adds tags to a resource.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// DeleteTags removes tag keys from a resource.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing := b.tags[resourceARN]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

func (b *InMemoryBackend) multiplexARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeMultiplex+":"+id)
}

// --- Multiplex operations ---

// CreateMultiplex creates a new Multiplex.
func (b *InMemoryBackend) CreateMultiplex(
	name string,
	availabilityZones []string,
	settings MultiplexSettings,
	tags map[string]string,
) (*Multiplex, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	zones := make([]string, len(availabilityZones))
	copy(zones, availabilityZones)

	id := newID()
	m := &storedMultiplex{
		ARN:               b.multiplexARN(id),
		ID:                id,
		Name:              name,
		State:             stateIdle,
		AvailabilityZones: zones,
		Settings:          storedMultiplexSettings(settings),
		Tags:              copyTags(tags),
		Programs:          make(map[string]*storedMultiplexProgram),
	}

	b.mu.Lock("CreateMultiplex")
	defer b.mu.Unlock()

	b.multiplexes[id] = m

	return m.toMultiplex(), nil
}

// DescribeMultiplex returns a Multiplex by ID.
func (b *InMemoryBackend) DescribeMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.RLock("DescribeMultiplex")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	return m.toMultiplex(), nil
}

// UpdateMultiplex updates a Multiplex's mutable fields.
func (b *InMemoryBackend) UpdateMultiplex(
	multiplexID, name string,
	settings MultiplexSettings,
) (*Multiplex, error) {
	b.mu.Lock("UpdateMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if name != "" {
		m.Name = name
	}

	m.Settings = storedMultiplexSettings(settings)

	return m.toMultiplex(), nil
}

// DeleteMultiplex deletes a Multiplex.
func (b *InMemoryBackend) DeleteMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("DeleteMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State == stateRunning {
		return nil, fmt.Errorf("%w: multiplex must be idle before deleting", ErrConflict)
	}

	m.State = stateDeleted
	delete(b.multiplexes, multiplexID)

	return m.toMultiplex(), nil
}

// ListMultiplexes returns a paginated list of multiplexes.
func (b *InMemoryBackend) ListMultiplexes(
	maxResults int,
	nextToken string,
) ([]*MultiplexSummary, string, error) {
	b.mu.RLock("ListMultiplexes")
	defer b.mu.RUnlock()

	all := make([]*storedMultiplex, 0, len(b.multiplexes))
	for _, m := range b.multiplexes {
		all = append(all, m)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*MultiplexSummary, 0, len(pg.Data))
	for _, m := range pg.Data {
		summaries = append(summaries, m.toSummary())
	}

	return summaries, pg.Next, nil
}

// StartMultiplex transitions a Multiplex to RUNNING.
func (b *InMemoryBackend) StartMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("StartMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State != stateIdle {
		return nil, fmt.Errorf("%w: multiplex must be idle to start", ErrConflict)
	}

	m.State = stateRunning

	return m.toMultiplex(), nil
}

// StopMultiplex transitions a Multiplex to IDLE.
func (b *InMemoryBackend) StopMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("StopMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State != stateRunning {
		return nil, fmt.Errorf("%w: multiplex must be running to stop", ErrConflict)
	}

	m.State = stateIdle

	return m.toMultiplex(), nil
}

// --- MultiplexProgram operations ---

// CreateMultiplexProgram creates a program within a Multiplex.
func (b *InMemoryBackend) CreateMultiplexProgram(
	multiplexID string,
	prog MultiplexProgramSettings,
) (*MultiplexProgram, error) {
	if prog.ProgramName == "" {
		return nil, fmt.Errorf("%w: programName required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if _, exists := m.Programs[prog.ProgramName]; exists {
		return nil, fmt.Errorf("%w: program %s already exists", ErrConflict, prog.ProgramName)
	}

	p := &storedMultiplexProgram{
		ProgramName: prog.ProgramName,
		Settings: storedMultiplexProgramSettings{
			ProgramNumber:            prog.ProgramNumber,
			PreferredChannelPipeline: prog.PreferredChannelPipeline,
			ServiceDescriptor: storedServiceDescriptor{
				ProviderName: prog.ServiceDescriptor.ProviderName,
				ServiceName:  prog.ServiceDescriptor.ServiceName,
			},
		},
	}

	m.Programs[prog.ProgramName] = p

	return p.toProgram(), nil
}

// DescribeMultiplexProgram returns a program by multiplex ID and program name.
func (b *InMemoryBackend) DescribeMultiplexProgram(
	multiplexID, programName string,
) (*MultiplexProgram, error) {
	b.mu.RLock("DescribeMultiplexProgram")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[programName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	return p.toProgram(), nil
}

// UpdateMultiplexProgram updates a program's settings.
func (b *InMemoryBackend) UpdateMultiplexProgram(
	multiplexID string,
	prog MultiplexProgramSettings,
) (*MultiplexProgram, error) {
	b.mu.Lock("UpdateMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[prog.ProgramName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, prog.ProgramName)
	}

	p.Settings = storedMultiplexProgramSettings{
		ProgramNumber:            prog.ProgramNumber,
		PreferredChannelPipeline: prog.PreferredChannelPipeline,
		ServiceDescriptor: storedServiceDescriptor{
			ProviderName: prog.ServiceDescriptor.ProviderName,
			ServiceName:  prog.ServiceDescriptor.ServiceName,
		},
	}

	return p.toProgram(), nil
}

// DeleteMultiplexProgram removes a program from a Multiplex.
func (b *InMemoryBackend) DeleteMultiplexProgram(
	multiplexID, programName string,
) (*MultiplexProgram, error) {
	b.mu.Lock("DeleteMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[programName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	delete(m.Programs, programName)

	return p.toProgram(), nil
}

// ListMultiplexPrograms returns a paginated list of programs for a Multiplex.
func (b *InMemoryBackend) ListMultiplexPrograms(
	multiplexID string,
	maxResults int,
	nextToken string,
) ([]*MultiplexProgramSummary, string, error) {
	b.mu.RLock("ListMultiplexPrograms")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return nil, "", fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	all := make([]*storedMultiplexProgram, 0, len(m.Programs))
	for _, p := range m.Programs {
		all = append(all, p)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ProgramName < all[j].ProgramName })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*MultiplexProgramSummary, 0, len(pg.Data))
	for _, p := range pg.Data {
		summaries = append(summaries, p.toSummary())
	}

	return summaries, pg.Next, nil
}

func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeCluster+":"+id)
}

func (b *InMemoryBackend) nodeARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeNode+":"+id)
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result
}

// --- InputDevice operations ---

// ClaimDevice registers a device (by ID) into this account.
func (b *InMemoryBackend) ClaimDevice(id string) (*InputDevice, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: id required", ErrInvalidParameter)
	}

	b.mu.Lock("ClaimDevice")
	defer b.mu.Unlock()

	if _, exists := b.inputDevices[id]; exists {
		return nil, fmt.Errorf("%w: device %s already claimed", ErrConflict, id)
	}

	d := &storedInputDevice{
		ARN:                     b.inputDeviceARN(id),
		ID:                      id,
		Name:                    id,
		SerialNumber:            id,
		MacAddress:              "00:00:00:00:00:00",
		DeviceType:              deviceTypeHD,
		ConnectionState:         deviceConnectionConnected,
		DeviceSettingsSyncState: deviceSettingsSynced,
		DeviceUpdateStatus:      deviceUpdateUpToDate,
		Tags:                    make(map[string]string),
	}
	b.inputDevices[id] = d

	return d.toDevice(), nil
}

// ListInputDevices returns a paginated list of input devices.
func (b *InMemoryBackend) ListInputDevices(
	maxResults int,
	nextToken string,
) ([]*InputDevice, string, error) {
	b.mu.RLock("ListInputDevices")
	defer b.mu.RUnlock()

	all := make([]*storedInputDevice, 0, len(b.inputDevices))
	for _, d := range b.inputDevices {
		all = append(all, d)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	devices := make([]*InputDevice, 0, len(pg.Data))
	for _, d := range pg.Data {
		devices = append(devices, d.toDevice())
	}

	return devices, pg.Next, nil
}

// DescribeInputDevice returns an input device by ID.
func (b *InMemoryBackend) DescribeInputDevice(deviceID string) (*InputDevice, error) {
	b.mu.RLock("DescribeInputDevice")
	defer b.mu.RUnlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return nil, fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return d.toDevice(), nil
}

// UpdateInputDevice updates the name of an input device.
func (b *InMemoryBackend) UpdateInputDevice(deviceID, name string) (*InputDevice, error) {
	b.mu.Lock("UpdateInputDevice")
	defer b.mu.Unlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return nil, fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if name != "" {
		d.Name = name
	}

	return d.toDevice(), nil
}

// RebootInputDevice initiates a reboot of the device (no-op in emulation).
func (b *InMemoryBackend) RebootInputDevice(deviceID string) error {
	b.mu.RLock("RebootInputDevice")
	defer b.mu.RUnlock()

	if _, ok := b.inputDevices[deviceID]; !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return nil
}

// TransferInputDevice initiates a transfer of the device to another account.
func (b *InMemoryBackend) TransferInputDevice(
	deviceID, targetCustomerID, targetRegion, message string,
) error {
	b.mu.Lock("TransferInputDevice")
	defer b.mu.Unlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer != nil {
		return fmt.Errorf("%w: device %s already has a pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = &storedInputDeviceTransfer{
		TargetCustomerID: targetCustomerID,
		TargetRegion:     targetRegion,
		Message:          message,
	}

	return nil
}

// AcceptInputDeviceTransfer accepts an incoming transfer and completes it.
func (b *InMemoryBackend) AcceptInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("AcceptInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil

	return nil
}

// CancelInputDeviceTransfer cancels an outgoing transfer.
func (b *InMemoryBackend) CancelInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("CancelInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil

	return nil
}

// RejectInputDeviceTransfer rejects an incoming transfer.
func (b *InMemoryBackend) RejectInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("RejectInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices[deviceID]
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil

	return nil
}

// ListInputDeviceTransfers lists devices with pending transfers.
// transferType must be "OUTGOING" or "INCOMING"; in this mock both resolve
// against the same pending-transfer store (we don't track the recipient side
// separately).
func (b *InMemoryBackend) ListInputDeviceTransfers(
	transferType string,
	maxResults int,
	nextToken string,
) ([]*InputDeviceTransfer, string, error) {
	if transferType != transferTypeOutgoing && transferType != transferTypeIncoming {
		return nil, "", fmt.Errorf(
			"%w: transferType must be OUTGOING or INCOMING",
			ErrInvalidParameter,
		)
	}

	b.mu.RLock("ListInputDeviceTransfers")
	defer b.mu.RUnlock()

	all := make([]*storedInputDevice, 0, len(b.inputDevices))
	for _, d := range b.inputDevices {
		if d.PendingTransfer != nil {
			all = append(all, d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	transfers := make([]*InputDeviceTransfer, 0, len(pg.Data))
	for _, d := range pg.Data {
		transfers = append(transfers, d.toPendingTransfer(transferType))
	}

	return transfers, pg.Next, nil
}

// --- Cluster operations ---

// CreateCluster creates a new Cluster.
func (b *InMemoryBackend) CreateCluster(
	name, clusterType, instanceRoleArn string,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if clusterType == "" {
		clusterType = "ON_PREMISES"
	}

	id := newID()
	c := &storedCluster{
		ARN:             b.clusterARN(id),
		ID:              id,
		Name:            name,
		ClusterType:     clusterType,
		InstanceRoleArn: instanceRoleArn,
		State:           clusterStateActive,
		Tags:            copyTags(tags),
		Nodes:           make(map[string]*storedNode),
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	b.clusters[id] = c

	return c.toCluster(), nil
}

// DescribeCluster returns a Cluster by ID.
func (b *InMemoryBackend) DescribeCluster(clusterID string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return c.toCluster(), nil
}

// UpdateCluster updates a Cluster's mutable fields.
func (b *InMemoryBackend) UpdateCluster(clusterID, name string) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if name != "" {
		c.Name = name
	}

	return c.toCluster(), nil
}

// DeleteCluster deletes a Cluster.
func (b *InMemoryBackend) DeleteCluster(clusterID string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	c.State = clusterStateDeleted
	delete(b.clusters, clusterID)

	return c.toCluster(), nil
}

// ListClusters returns a paginated list of Clusters.
func (b *InMemoryBackend) ListClusters(
	maxResults int,
	nextToken string,
) ([]*ClusterSummary, string, error) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	all := make([]*storedCluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		all = append(all, c)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ClusterSummary, 0, len(pg.Data))
	for _, c := range pg.Data {
		summaries = append(summaries, c.toSummary())
	}

	return summaries, pg.Next, nil
}

// --- Node operations ---

// CreateNode creates a Node within a Cluster.
func (b *InMemoryBackend) CreateNode(
	clusterID, name, role string,
	tags map[string]string,
) (*Node, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: clusterId required", ErrInvalidParameter)
	}

	if role == "" {
		role = nodeRoleActive
	}

	b.mu.Lock("CreateNode")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	id := newID()
	if name == "" {
		name = id
	}

	n := &storedNode{
		ARN:             b.nodeARN(id),
		ID:              id,
		Name:            name,
		ClusterID:       clusterID,
		Role:            role,
		State:           nodeStateActive,
		ConnectionState: nodeConnectionConn,
		Tags:            copyTags(tags),
	}

	c.Nodes[id] = n

	return n.toNode(), nil
}

// DescribeNode returns a Node by cluster ID and node ID.
func (b *InMemoryBackend) DescribeNode(clusterID, nodeID string) (*Node, error) {
	b.mu.RLock("DescribeNode")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	return n.toNode(), nil
}

// UpdateNode updates a Node's mutable fields.
func (b *InMemoryBackend) UpdateNode(clusterID, nodeID, name, role string) (*Node, error) {
	b.mu.Lock("UpdateNode")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	if name != "" {
		n.Name = name
	}

	if role != "" {
		n.Role = role
	}

	return n.toNode(), nil
}

// UpdateNodeState updates the state of a Node.
func (b *InMemoryBackend) UpdateNodeState(clusterID, nodeID, state string) (*Node, error) {
	b.mu.Lock("UpdateNodeState")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	if state != "" {
		n.State = state
	}

	return n.toNode(), nil
}

// DeleteNode removes a Node from a Cluster.
func (b *InMemoryBackend) DeleteNode(clusterID, nodeID string) (*Node, error) {
	b.mu.Lock("DeleteNode")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: node %s not found", ErrNotFound, nodeID)
	}

	delete(c.Nodes, nodeID)

	return n.toNode(), nil
}

// paginateNodes returns a sorted, paginated node-summary slice from a cluster.
func paginateNodes(c *storedCluster, maxResults int, nextToken string) ([]*NodeSummary, string) {
	nodes := make([]*storedNode, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		nodes = append(nodes, n)
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	pg := page.New(nodes, nextToken, maxResults, defaultMaxResults)

	out := make([]*NodeSummary, 0, len(pg.Data))
	for _, n := range pg.Data {
		out = append(out, n.toSummary())
	}

	return out, pg.Next
}

// ListNodes returns a paginated list of Nodes in a Cluster.
func (b *InMemoryBackend) ListNodes(
	clusterID string,
	maxResults int,
	nextToken string,
) ([]*NodeSummary, string, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	summaries, next := paginateNodes(c, maxResults, nextToken)

	return summaries, next, nil
}

// CreateNodeRegistrationScript returns a registration script for a Cluster Node.
func (b *InMemoryBackend) CreateNodeRegistrationScript(clusterID string) (string, error) {
	b.mu.RLock("CreateNodeRegistrationScript")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterID]; !ok {
		return "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return "#!/bin/bash\n# Node registration script for cluster " + clusterID + "\n", nil
}

// ListClusterAlerts returns alerts for a Cluster (always empty in emulation).
func (b *InMemoryBackend) ListClusterAlerts(
	clusterID string,
	_ int,
	_ string,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListClusterAlerts")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterID]; !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	return []map[string]any{}, "", nil
}

// --- Signal Map operations ---

// findSignalMap locates a signal map by ID or ARN or name.
func (b *InMemoryBackend) findSignalMap(identifier string) (*storedSignalMap, bool) {
	for _, sm := range b.signalMaps {
		if sm.ID == identifier || sm.Arn == identifier || sm.Name == identifier {
			return sm, true
		}
	}

	return nil, false
}

// CreateSignalMap creates a new signal map.
func (b *InMemoryBackend) CreateSignalMap(
	name, description, discoveryEntryPointArn string,
	cwGroupIDs, ebGroupIDs []string,
	tags map[string]string,
) (*SignalMap, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	id := newID()
	sm := &storedSignalMap{
		Tags:                            copyTags(tags),
		CloudWatchAlarmTemplateGroupIDs: append([]string{}, cwGroupIDs...),
		EventBridgeRuleTemplateGroupIDs: append([]string{}, ebGroupIDs...),
		Arn:                             b.signalMapARN(id),
		ID:                              id,
		Name:                            name,
		Description:                     description,
		DiscoveryEntryPointArn:          discoveryEntryPointArn,
		Status:                          "SUCCEEDED",
		MonitorDeploymentStatus:         "NOT_DEPLOYED",
	}

	b.mu.Lock("CreateSignalMap")
	defer b.mu.Unlock()
	b.signalMaps[id] = sm

	return sm.toSignalMap(), nil
}

// GetSignalMap returns a signal map by identifier.
func (b *InMemoryBackend) GetSignalMap(identifier string) (*SignalMap, error) {
	b.mu.RLock("GetSignalMap")
	defer b.mu.RUnlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}

	return sm.toSignalMap(), nil
}

// ListSignalMaps returns all signal maps.
func (b *InMemoryBackend) ListSignalMaps(
	maxResults int,
	nextToken string,
) ([]*SignalMap, string, error) {
	b.mu.RLock("ListSignalMaps")
	defer b.mu.RUnlock()
	all := make([]*storedSignalMap, 0, len(b.signalMaps))
	for _, sm := range b.signalMaps {
		all = append(all, sm)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*SignalMap, 0, len(pg.Data))
	for _, sm := range pg.Data {
		result = append(result, sm.toSignalMap())
	}

	return result, pg.Next, nil
}

// DeleteSignalMap deletes a signal map.
func (b *InMemoryBackend) DeleteSignalMap(identifier string) error {
	b.mu.Lock("DeleteSignalMap")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	delete(b.signalMaps, sm.ID)

	return nil
}

// StartUpdateSignalMap updates a signal map's configuration.
func (b *InMemoryBackend) StartUpdateSignalMap(
	identifier, name, description string,
	cwGroupIDs, ebGroupIDs []string,
) (*SignalMap, error) {
	b.mu.Lock("StartUpdateSignalMap")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	if name != "" {
		sm.Name = name
	}
	if description != "" {
		sm.Description = description
	}
	if cwGroupIDs != nil {
		sm.CloudWatchAlarmTemplateGroupIDs = append([]string{}, cwGroupIDs...)
	}
	if ebGroupIDs != nil {
		sm.EventBridgeRuleTemplateGroupIDs = append([]string{}, ebGroupIDs...)
	}
	sm.Status = "SUCCEEDED"

	return sm.toSignalMap(), nil
}

// StartMonitorDeployment deploys monitoring for a signal map.
func (b *InMemoryBackend) StartMonitorDeployment(identifier string) (*SignalMap, error) {
	b.mu.Lock("StartMonitorDeployment")
	defer b.mu.Unlock()
	sm, ok := b.findSignalMap(identifier)
	if !ok {
		return nil, fmt.Errorf("%w: signal map %s not found", ErrNotFound, identifier)
	}
	sm.MonitorDeploymentStatus = "DEPLOYED"

	return sm.toSignalMap(), nil
}

// --- CloudWatch Alarm Template Group operations ---

func (b *InMemoryBackend) findCWAlarmTemplateGroup(
	identifier string,
) (*storedCloudWatchAlarmTemplateGroup, bool) {
	for _, g := range b.cwAlarmTemplateGroups {
		if g.ID == identifier || g.Arn == identifier || g.Name == identifier {
			return g, true
		}
	}

	return nil, false
}

// CreateCloudWatchAlarmTemplateGroup creates a new CW alarm template group.
func (b *InMemoryBackend) CreateCloudWatchAlarmTemplateGroup(
	name, description string, tags map[string]string,
) (*CloudWatchAlarmTemplateGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	id := newID()
	g := &storedCloudWatchAlarmTemplateGroup{
		Tags:        copyTags(tags),
		Arn:         b.cwAlarmTemplateGroupARN(id),
		ID:          id,
		Name:        name,
		Description: description,
	}
	b.mu.Lock("CreateCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	b.cwAlarmTemplateGroups[id] = g

	return g.toGroup(), nil
}

// GetCloudWatchAlarmTemplateGroup returns a CW alarm template group by identifier.
func (b *InMemoryBackend) GetCloudWatchAlarmTemplateGroup(
	identifier string,
) (*CloudWatchAlarmTemplateGroup, error) {
	b.mu.RLock("GetCloudWatchAlarmTemplateGroup")
	defer b.mu.RUnlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return g.toGroup(), nil
}

// ListCloudWatchAlarmTemplateGroups returns all CW alarm template groups.
func (b *InMemoryBackend) ListCloudWatchAlarmTemplateGroups(
	maxResults int,
	nextToken string,
) ([]*CloudWatchAlarmTemplateGroup, string, error) {
	b.mu.RLock("ListCloudWatchAlarmTemplateGroups")
	defer b.mu.RUnlock()
	all := make([]*storedCloudWatchAlarmTemplateGroup, 0, len(b.cwAlarmTemplateGroups))
	for _, g := range b.cwAlarmTemplateGroups {
		all = append(all, g)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*CloudWatchAlarmTemplateGroup, 0, len(pg.Data))
	for _, g := range pg.Data {
		result = append(result, g.toGroup())
	}

	return result, pg.Next, nil
}

// UpdateCloudWatchAlarmTemplateGroup updates a CW alarm template group.
func (b *InMemoryBackend) UpdateCloudWatchAlarmTemplateGroup(
	identifier, name, description string,
) (*CloudWatchAlarmTemplateGroup, error) {
	b.mu.Lock("UpdateCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		g.Name = name
	}
	if description != "" {
		g.Description = description
	}

	return g.toGroup(), nil
}

// DeleteCloudWatchAlarmTemplateGroup deletes a CW alarm template group.
func (b *InMemoryBackend) DeleteCloudWatchAlarmTemplateGroup(identifier string) error {
	b.mu.Lock("DeleteCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	delete(b.cwAlarmTemplateGroups, g.ID)

	return nil
}

// --- CloudWatch Alarm Template operations ---

func (b *InMemoryBackend) findCWAlarmTemplate(
	identifier string,
) (*storedCloudWatchAlarmTemplate, bool) {
	for _, t := range b.cwAlarmTemplates {
		if t.ID == identifier || t.Arn == identifier || t.Name == identifier {
			return t, true
		}
	}

	return nil, false
}

// CreateCloudWatchAlarmTemplate creates a new CW alarm template.
func (b *InMemoryBackend) CreateCloudWatchAlarmTemplate(
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
	tags map[string]string,
) (*CloudWatchAlarmTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	groupID := groupIdentifier
	b.mu.Lock("CreateCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	if g, ok := b.findCWAlarmTemplateGroup(groupIdentifier); ok {
		groupID = g.ID
	}
	id := newID()
	t := &storedCloudWatchAlarmTemplate{
		Tags: copyTags(
			tags,
		), Arn: b.cwAlarmTemplateARN(id), ID: id, Name: name, Description: description,
		GroupID: groupID, GroupIdentifier: groupIdentifier, MetricName: metricName, Namespace: namespace,
		Statistic: statistic, ComparisonOperator: comparisonOperator, TargetResourceType: targetResourceType,
		TreatMissingData: treatMissingData, Threshold: threshold,
		EvaluationPeriods: evaluationPeriods, DatapointsToAlarm: datapointsToAlarm, Period: period,
	}
	b.cwAlarmTemplates[id] = t

	return t.toTemplate(), nil
}

// GetCloudWatchAlarmTemplate returns a CW alarm template by identifier.
func (b *InMemoryBackend) GetCloudWatchAlarmTemplate(
	identifier string,
) (*CloudWatchAlarmTemplate, error) {
	b.mu.RLock("GetCloudWatchAlarmTemplate")
	defer b.mu.RUnlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return t.toTemplate(), nil
}

// ListCloudWatchAlarmTemplates returns all CW alarm templates.
func (b *InMemoryBackend) ListCloudWatchAlarmTemplates(
	maxResults int,
	nextToken string,
) ([]*CloudWatchAlarmTemplate, string, error) {
	b.mu.RLock("ListCloudWatchAlarmTemplates")
	defer b.mu.RUnlock()
	all := make([]*storedCloudWatchAlarmTemplate, 0, len(b.cwAlarmTemplates))
	for _, t := range b.cwAlarmTemplates {
		all = append(all, t)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*CloudWatchAlarmTemplate, 0, len(pg.Data))
	for _, t := range pg.Data {
		result = append(result, t.toTemplate())
	}

	return result, pg.Next, nil
}

func (b *InMemoryBackend) updateCWTemplateFields(
	t *storedCloudWatchAlarmTemplate,
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
) {
	if name != "" {
		t.Name = name
	}
	if description != "" {
		t.Description = description
	}
	if groupIdentifier != "" {
		t.GroupIdentifier = groupIdentifier
		if g, ok := b.findCWAlarmTemplateGroup(groupIdentifier); ok {
			t.GroupID = g.ID
		} else {
			t.GroupID = groupIdentifier
		}
	}
	if metricName != "" {
		t.MetricName = metricName
	}
	if namespace != "" {
		t.Namespace = namespace
	}
	if statistic != "" {
		t.Statistic = statistic
	}
	if comparisonOperator != "" {
		t.ComparisonOperator = comparisonOperator
	}
	if targetResourceType != "" {
		t.TargetResourceType = targetResourceType
	}
	if treatMissingData != "" {
		t.TreatMissingData = treatMissingData
	}
	if threshold != 0 {
		t.Threshold = threshold
	}
	if evaluationPeriods != 0 {
		t.EvaluationPeriods = evaluationPeriods
	}
	if datapointsToAlarm != 0 {
		t.DatapointsToAlarm = datapointsToAlarm
	}
	if period != 0 {
		t.Period = period
	}
}

// UpdateCloudWatchAlarmTemplate updates a CW alarm template.
func (b *InMemoryBackend) UpdateCloudWatchAlarmTemplate(
	identifier string,
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
) (*CloudWatchAlarmTemplate, error) {
	b.mu.Lock("UpdateCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template %s not found",
			ErrNotFound,
			identifier,
		)
	}
	b.updateCWTemplateFields(
		t,
		name,
		description,
		groupIdentifier,
		metricName,
		namespace,
		statistic,
		comparisonOperator,
		targetResourceType,
		treatMissingData,
		threshold,
		evaluationPeriods,
		datapointsToAlarm,
		period,
	)

	return t.toTemplate(), nil
}

// DeleteCloudWatchAlarmTemplate deletes a CW alarm template.
func (b *InMemoryBackend) DeleteCloudWatchAlarmTemplate(identifier string) error {
	b.mu.Lock("DeleteCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return fmt.Errorf("%w: cloudwatch alarm template %s not found", ErrNotFound, identifier)
	}
	delete(b.cwAlarmTemplates, t.ID)

	return nil
}

// --- EventBridge Rule Template Group operations ---

func (b *InMemoryBackend) findEBRuleTemplateGroup(
	identifier string,
) (*storedEventBridgeRuleTemplateGroup, bool) {
	for _, g := range b.ebRuleTemplateGroups {
		if g.ID == identifier || g.Arn == identifier || g.Name == identifier {
			return g, true
		}
	}

	return nil, false
}

// CreateEventBridgeRuleTemplateGroup creates a new EB rule template group.
func (b *InMemoryBackend) CreateEventBridgeRuleTemplateGroup(
	name, description string, tags map[string]string,
) (*EventBridgeRuleTemplateGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	id := newID()
	g := &storedEventBridgeRuleTemplateGroup{
		Tags: copyTags(
			tags,
		), Arn: b.ebRuleTemplateGroupARN(id), ID: id, Name: name, Description: description,
	}
	b.mu.Lock("CreateEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	b.ebRuleTemplateGroups[id] = g

	return g.toGroup(), nil
}

// GetEventBridgeRuleTemplateGroup returns an EB rule template group.
func (b *InMemoryBackend) GetEventBridgeRuleTemplateGroup(
	identifier string,
) (*EventBridgeRuleTemplateGroup, error) {
	b.mu.RLock("GetEventBridgeRuleTemplateGroup")
	defer b.mu.RUnlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return g.toGroup(), nil
}

// ListEventBridgeRuleTemplateGroups returns all EB rule template groups.
func (b *InMemoryBackend) ListEventBridgeRuleTemplateGroups(
	maxResults int,
	nextToken string,
) ([]*EventBridgeRuleTemplateGroup, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplateGroups")
	defer b.mu.RUnlock()
	all := make([]*storedEventBridgeRuleTemplateGroup, 0, len(b.ebRuleTemplateGroups))
	for _, g := range b.ebRuleTemplateGroups {
		all = append(all, g)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*EventBridgeRuleTemplateGroup, 0, len(pg.Data))
	for _, g := range pg.Data {
		result = append(result, g.toGroup())
	}

	return result, pg.Next, nil
}

// UpdateEventBridgeRuleTemplateGroup updates an EB rule template group.
func (b *InMemoryBackend) UpdateEventBridgeRuleTemplateGroup(
	identifier, name, description string,
) (*EventBridgeRuleTemplateGroup, error) {
	b.mu.Lock("UpdateEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		g.Name = name
	}
	if description != "" {
		g.Description = description
	}

	return g.toGroup(), nil
}

// DeleteEventBridgeRuleTemplateGroup deletes an EB rule template group.
func (b *InMemoryBackend) DeleteEventBridgeRuleTemplateGroup(identifier string) error {
	b.mu.Lock("DeleteEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	delete(b.ebRuleTemplateGroups, g.ID)

	return nil
}

// --- EventBridge Rule Template operations ---

func (b *InMemoryBackend) findEBRuleTemplate(
	identifier string,
) (*storedEventBridgeRuleTemplate, bool) {
	for _, t := range b.ebRuleTemplates {
		if t.ID == identifier || t.Arn == identifier || t.Name == identifier {
			return t, true
		}
	}

	return nil, false
}

// CreateEventBridgeRuleTemplate creates a new EB rule template.
func (b *InMemoryBackend) CreateEventBridgeRuleTemplate(
	name, description, groupIdentifier, eventType string,
	eventTargets []EventBridgeRuleTemplateTarget,
	tags map[string]string,
) (*EventBridgeRuleTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	groupID := groupIdentifier
	b.mu.Lock("CreateEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	if g, ok := b.findEBRuleTemplateGroup(groupIdentifier); ok {
		groupID = g.ID
	}
	targets := make([]EventBridgeRuleTemplateTarget, len(eventTargets))
	copy(targets, eventTargets)
	id := newID()
	t := &storedEventBridgeRuleTemplate{
		Tags: copyTags(
			tags,
		), EventTargets: targets, Arn: b.ebRuleTemplateARN(id), ID: id, Name: name,
		Description: description, GroupID: groupID, GroupIdentifier: groupIdentifier, EventType: eventType,
	}
	b.ebRuleTemplates[id] = t

	return t.toTemplate(), nil
}

// GetEventBridgeRuleTemplate returns an EB rule template.
func (b *InMemoryBackend) GetEventBridgeRuleTemplate(
	identifier string,
) (*EventBridgeRuleTemplate, error) {
	b.mu.RLock("GetEventBridgeRuleTemplate")
	defer b.mu.RUnlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return t.toTemplate(), nil
}

// ListEventBridgeRuleTemplates returns all EB rule templates.
func (b *InMemoryBackend) ListEventBridgeRuleTemplates(
	maxResults int,
	nextToken string,
) ([]*EventBridgeRuleTemplate, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplates")
	defer b.mu.RUnlock()
	all := make([]*storedEventBridgeRuleTemplate, 0, len(b.ebRuleTemplates))
	for _, t := range b.ebRuleTemplates {
		all = append(all, t)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*EventBridgeRuleTemplate, 0, len(pg.Data))
	for _, t := range pg.Data {
		result = append(result, t.toTemplate())
	}

	return result, pg.Next, nil
}

// UpdateEventBridgeRuleTemplate updates an EB rule template.
func (b *InMemoryBackend) UpdateEventBridgeRuleTemplate(
	identifier, name, description, groupIdentifier, eventType string,
	eventTargets []EventBridgeRuleTemplateTarget,
) (*EventBridgeRuleTemplate, error) {
	b.mu.Lock("UpdateEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		t.Name = name
	}
	if description != "" {
		t.Description = description
	}
	if groupIdentifier != "" {
		t.GroupIdentifier = groupIdentifier
		g, found := b.findEBRuleTemplateGroup(groupIdentifier)
		if found {
			t.GroupID = g.ID
		} else {
			t.GroupID = groupIdentifier
		}
	}
	if eventType != "" {
		t.EventType = eventType
	}
	if eventTargets != nil {
		t.EventTargets = make([]EventBridgeRuleTemplateTarget, len(eventTargets))
		copy(t.EventTargets, eventTargets)
	}

	return t.toTemplate(), nil
}

// DeleteEventBridgeRuleTemplate deletes an EB rule template.
func (b *InMemoryBackend) DeleteEventBridgeRuleTemplate(identifier string) error {
	b.mu.Lock("DeleteEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return fmt.Errorf("%w: eventbridge rule template %s not found", ErrNotFound, identifier)
	}
	delete(b.ebRuleTemplates, t.ID)

	return nil
}

// --- Offering operations ---

// ListOfferings returns the seeded offering catalog.
func (b *InMemoryBackend) ListOfferings(
	maxResults int,
	nextToken string,
) ([]*Offering, string, error) {
	b.mu.RLock("ListOfferings")
	defer b.mu.RUnlock()
	pg := page.New(b.offerings, nextToken, maxResults, defaultMaxResults)
	result := make([]*Offering, len(pg.Data))
	copy(result, pg.Data)

	return result, pg.Next, nil
}

// DescribeOffering returns a single offering by ID.
func (b *InMemoryBackend) DescribeOffering(offeringID string) (*Offering, error) {
	b.mu.RLock("DescribeOffering")
	defer b.mu.RUnlock()
	for _, o := range b.offerings {
		if o.OfferingID == offeringID {
			cp := *o

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: offering %s not found", ErrNotFound, offeringID)
}

// --- Reservation operations ---

// PurchaseOffering creates a Reservation from an Offering.
func (b *InMemoryBackend) PurchaseOffering(
	offeringID, name string,
	count int32,
	tags map[string]string,
) (*Reservation, error) {
	b.mu.Lock("PurchaseOffering")
	defer b.mu.Unlock()
	var off *Offering
	for _, o := range b.offerings {
		if o.OfferingID == offeringID {
			cp := *o
			off = &cp

			break
		}
	}
	if off == nil {
		return nil, fmt.Errorf("%w: offering %s not found", ErrNotFound, offeringID)
	}
	if count <= 0 {
		count = 1
	}
	id := newID()
	r := &storedReservation{
		Tags:                  copyTags(tags),
		ResourceSpecification: off.ResourceSpecification,
		Arn:                   b.reservationARN(id),
		ReservationID:         id,
		Name:                  name,
		OfferingID:            off.OfferingID,
		OfferingDescription:   off.OfferingDescription,
		OfferingType:          off.OfferingType,
		CurrencyCode:          off.CurrencyCode,
		FixedPrice:            off.FixedPrice,
		UsagePrice:            off.UsagePrice,
		Duration:              off.Duration,
		DurationUnits:         off.DurationUnits,
		Start:                 "2024-01-01T00:00:00Z",
		End:                   "2025-01-01T00:00:00Z",
		Region:                b.region,
		State:                 "ACTIVE",
		Count:                 count,
	}
	b.reservations[id] = r

	return r.toReservation(), nil
}

// ListReservations returns all reservations.
func (b *InMemoryBackend) ListReservations(
	maxResults int,
	nextToken string,
) ([]*Reservation, string, error) {
	b.mu.RLock("ListReservations")
	defer b.mu.RUnlock()
	all := make([]*storedReservation, 0, len(b.reservations))
	for _, r := range b.reservations {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ReservationID < all[j].ReservationID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*Reservation, 0, len(pg.Data))
	for _, r := range pg.Data {
		result = append(result, r.toReservation())
	}

	return result, pg.Next, nil
}

// DescribeReservation returns a single reservation.
func (b *InMemoryBackend) DescribeReservation(reservationID string) (*Reservation, error) {
	b.mu.RLock("DescribeReservation")
	defer b.mu.RUnlock()
	r, ok := b.reservations[reservationID]
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}

	return r.toReservation(), nil
}

// DeleteReservation cancels a reservation.
func (b *InMemoryBackend) DeleteReservation(reservationID string) (*Reservation, error) {
	b.mu.Lock("DeleteReservation")
	defer b.mu.Unlock()
	r, ok := b.reservations[reservationID]
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}
	r.State = "CANCELED"
	out := r.toReservation()
	delete(b.reservations, reservationID)

	return out, nil
}

// UpdateReservation updates a reservation's name.
func (b *InMemoryBackend) UpdateReservation(reservationID, name string) (*Reservation, error) {
	b.mu.Lock("UpdateReservation")
	defer b.mu.Unlock()
	r, ok := b.reservations[reservationID]
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}
	if name != "" {
		r.Name = name
	}

	return r.toReservation(), nil
}

// --- Batch operations ---

func (b *InMemoryBackend) batchSetState(
	channelIDs, multiplexIDs []string,
	state string,
) *BatchResult {
	var result BatchResult
	for _, id := range channelIDs {
		ch, ok := b.channels[id]
		if !ok {
			result.Failed = append(result.Failed, BatchFailedResult{ID: id, Code: batchErrNotFound})

			continue
		}
		ch.State = state
		result.Successful = append(
			result.Successful,
			BatchSuccessfulResult{ID: id, Arn: ch.ARN, State: ch.State},
		)
	}
	for _, id := range multiplexIDs {
		mx, ok := b.multiplexes[id]
		if !ok {
			result.Failed = append(result.Failed, BatchFailedResult{ID: id, Code: batchErrNotFound})

			continue
		}
		mx.State = state
		result.Successful = append(
			result.Successful,
			BatchSuccessfulResult{ID: id, Arn: mx.ARN, State: mx.State},
		)
	}

	return &result
}

// BatchStart starts channels/inputs/multiplexes in bulk.
func (b *InMemoryBackend) BatchStart(
	channelIDs, _, multiplexIDs []string,
) (*BatchResult, error) {
	b.mu.Lock("BatchStart")
	defer b.mu.Unlock()

	return b.batchSetState(channelIDs, multiplexIDs, stateRunning), nil
}

// BatchStop stops channels/inputs/multiplexes in bulk.
func (b *InMemoryBackend) BatchStop(
	channelIDs, _, multiplexIDs []string,
) (*BatchResult, error) {
	b.mu.Lock("BatchStop")
	defer b.mu.Unlock()

	return b.batchSetState(channelIDs, multiplexIDs, stateIdle), nil
}

// BatchDelete deletes channels/inputs/multiplexes in bulk.
func (b *InMemoryBackend) BatchDelete(
	channelIDs, inputIDs, multiplexIDs []string,
) (*BatchResult, error) {
	b.mu.Lock("BatchDelete")
	defer b.mu.Unlock()
	var result BatchResult
	for _, id := range channelIDs {
		ch, ok := b.channels[id]
		if !ok {
			result.Failed = append(result.Failed, BatchFailedResult{ID: id, Code: batchErrNotFound})

			continue
		}
		if ch.State == stateRunning {
			result.Failed = append(
				result.Failed,
				BatchFailedResult{ID: id, Arn: ch.ARN, Code: "CONFLICT"},
			)

			continue
		}
		delete(b.channels, id)
		result.Successful = append(
			result.Successful,
			BatchSuccessfulResult{ID: id, Arn: ch.ARN, State: stateDeleted},
		)
	}
	for _, id := range inputIDs {
		inp, ok := b.inputs[id]
		if !ok {
			result.Failed = append(result.Failed, BatchFailedResult{ID: id, Code: batchErrNotFound})

			continue
		}
		delete(b.inputs, id)
		result.Successful = append(
			result.Successful,
			BatchSuccessfulResult{ID: id, Arn: inp.ARN, State: stateDeleted},
		)
	}
	for _, id := range multiplexIDs {
		mx, ok := b.multiplexes[id]
		if !ok {
			result.Failed = append(result.Failed, BatchFailedResult{ID: id, Code: batchErrNotFound})

			continue
		}
		if mx.State == stateRunning {
			result.Failed = append(
				result.Failed,
				BatchFailedResult{ID: id, Arn: mx.ARN, Code: "CONFLICT"},
			)

			continue
		}
		delete(b.multiplexes, id)
		result.Successful = append(
			result.Successful,
			BatchSuccessfulResult{ID: id, Arn: mx.ARN, State: stateDeleted},
		)
	}

	return &result, nil
}

// BatchUpdateSchedule adds/removes schedule actions for a channel.
func (b *InMemoryBackend) BatchUpdateSchedule(
	channelID string,
	creates []ScheduleAction,
	deleteActionNames []string,
) (*BatchUpdateScheduleResult, error) {
	b.mu.Lock("BatchUpdateSchedule")
	defer b.mu.Unlock()
	if _, ok := b.channels[channelID]; !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}
	actions := b.scheduleActions[channelID]
	// Remove deleted actions.
	toDelete := make(map[string]bool, len(deleteActionNames))
	for _, n := range deleteActionNames {
		toDelete[n] = true
	}
	filtered := actions[:0]
	for _, a := range actions {
		if !toDelete[a.ActionName] {
			filtered = append(filtered, a)
		}
	}
	// Add new actions.
	var created []ScheduleAction
	for _, c := range creates {
		filtered = append(
			filtered,
			&storedScheduleAction{ActionName: c.ActionName, ActionType: c.ActionType},
		)
		created = append(created, c)
	}
	b.scheduleActions[channelID] = filtered
	// Build deleted list from intersection of requested deletes and what actually existed.
	var deleted []ScheduleAction
	for _, n := range deleteActionNames {
		deleted = append(deleted, ScheduleAction{ActionName: n})
	}

	return &BatchUpdateScheduleResult{Creates: created, Deletes: deleted}, nil
}
