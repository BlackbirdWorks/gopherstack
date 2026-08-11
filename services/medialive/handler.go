package medialive

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// formatISO8601 renders t in the wire format the real medialive restjson1
// deserializer expects for __timestampIso8601 shapes (SignalMap/
// CloudWatchAlarmTemplate(Group)/EventBridgeRuleTemplate(Group)
// createdAt/modifiedAt) -- confirmed against
// aws-sdk-go-v2/service/medialive@v1.101.4's deserializers.go, which parses
// these fields with smithytime.ParseDateTime (an ISO8601/RFC3339 string),
// NOT smithytime.ParseEpochSeconds. A zero time.Time renders as "" so a
// resource that hasn't recorded a timestamp yet doesn't emit a bogus
// 0001-01-01 date.
func formatISO8601(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(time.RFC3339)
}

const (
	matchPriority = service.PriorityPathVersioned

	pathPrefix                = "/prod/"
	pathChannels              = "/prod/channels"
	pathInputs                = "/prod/inputs"
	pathInputSecurityGroups   = "/prod/inputSecurityGroups"
	pathInputDevices          = "/prod/inputDevices"
	pathInputDeviceTransfers  = "/prod/inputDeviceTransfers"
	pathClaimDevice           = "/prod/claimDevice"
	pathMultiplexes           = "/prod/multiplexes"
	pathClusters              = "/prod/clusters"
	pathTags                  = "/prod/tags/"
	pathSignalMaps            = "/prod/signal-maps"
	pathCWAlarmTemplateGroups = "/prod/cloudwatch-alarm-template-groups"
	pathCWAlarmTemplates      = "/prod/cloudwatch-alarm-templates"
	pathEBRuleTemplateGroups  = "/prod/eventbridge-rule-template-groups"
	pathEBRuleTemplates       = "/prod/eventbridge-rule-templates"
	pathReservations          = "/prod/reservations"
	pathOfferings             = "/prod/offerings"
	pathBatch                 = "/prod/batch"
	pathNetworks              = "/prod/networks"
	pathSdiSources            = "/prod/sdiSources"
	pathAccountConfiguration  = "/prod/accountConfiguration"
	pathVersions              = "/prod/versions"

	subPrograms                = "programs"
	subStart                   = "start"
	subStop                    = "stop"
	subNodes                   = "nodes"
	subAlerts                  = "alerts"
	subNodeRegistrationScript  = "nodeRegistrationScript"
	subState                   = "state"
	subSchedule                = "schedule"
	subMonitorDeployment       = "monitor-deployment"
	subPurchase                = "purchase"
	subChannelPlacementGroups  = "channelplacementgroups"
	subThumbnails              = "thumbnails"
	subThumbnailData           = "thumbnailData"
	subChannelClass            = "channelClass"
	subRestartChannelPipelines = "restartChannelPipelines"
	subMaintenanceWindow       = "startInputDeviceMaintenanceWindow"
	subPartners                = "partners"

	pathSegmentsID      = 1
	pathSegmentsSub     = 2
	pathSegmentsNamed   = 3
	pathSegmentsDeepSub = 4

	keyMessage         = "Message"
	keyArn             = "arn"
	keyID              = "id"
	keyName            = "name"
	keyState           = "state"
	keyTags            = "tags"
	keyDescription     = "description"
	keyChannel         = "channel"
	keyInput           = "input"
	keyAlerts          = "alerts"
	keyActionName      = "actionName"
	keyScheduleActions = "scheduleActions"
	keyLowerMessage    = "message"
	keyCreatedAt       = "createdAt"
	keyModifiedAt      = "modifiedAt"
	keySdiSource       = "sdiSource"
	keyGroupID         = "groupId"
	opUnknown          = "Unknown"

	opCreateChannel   = "CreateChannel"
	opDescribeChannel = "DescribeChannel"
	opUpdateChannel   = "UpdateChannel"
	opDeleteChannel   = "DeleteChannel"
	opListChannels    = "ListChannels"
	opStartChannel    = "StartChannel"
	opStopChannel     = "StopChannel"

	opCreateInput   = "CreateInput"
	opDescribeInput = "DescribeInput"
	opUpdateInput   = "UpdateInput"
	opDeleteInput   = "DeleteInput"
	opListInputs    = "ListInputs"

	opCreateInputSecurityGroup   = "CreateInputSecurityGroup"
	opDescribeInputSecurityGroup = "DescribeInputSecurityGroup"
	opUpdateInputSecurityGroup   = "UpdateInputSecurityGroup"
	opDeleteInputSecurityGroup   = "DeleteInputSecurityGroup"
	opListInputSecurityGroups    = "ListInputSecurityGroups"

	opClaimDevice               = "ClaimDevice"
	opListInputDevices          = "ListInputDevices"
	opDescribeInputDevice       = "DescribeInputDevice"
	opUpdateInputDevice         = "UpdateInputDevice"
	opRebootInputDevice         = "RebootInputDevice"
	opTransferInputDevice       = "TransferInputDevice"
	opAcceptInputDeviceTransfer = "AcceptInputDeviceTransfer"
	opCancelInputDeviceTransfer = "CancelInputDeviceTransfer"
	opRejectInputDeviceTransfer = "RejectInputDeviceTransfer"
	opListInputDeviceTransfers  = "ListInputDeviceTransfers"

	opCreateMultiplex   = "CreateMultiplex"
	opDescribeMultiplex = "DescribeMultiplex"
	opUpdateMultiplex   = "UpdateMultiplex"
	opDeleteMultiplex   = "DeleteMultiplex"
	opListMultiplexes   = "ListMultiplexes"
	opStartMultiplex    = "StartMultiplex"
	opStopMultiplex     = "StopMultiplex"

	opCreateMultiplexProgram   = "CreateMultiplexProgram"
	opDescribeMultiplexProgram = "DescribeMultiplexProgram"
	opUpdateMultiplexProgram   = "UpdateMultiplexProgram"
	opDeleteMultiplexProgram   = "DeleteMultiplexProgram"
	opListMultiplexPrograms    = "ListMultiplexPrograms"

	opCreateTags          = "CreateTags"
	opDeleteTags          = "DeleteTags"
	opListTagsForResource = "ListTagsForResource"

	opCreateCluster                = "CreateCluster"
	opDescribeCluster              = "DescribeCluster"
	opUpdateCluster                = "UpdateCluster"
	opDeleteCluster                = "DeleteCluster"
	opListClusters                 = "ListClusters"
	opListClusterAlerts            = "ListClusterAlerts"
	opCreateNodeRegistrationScript = "CreateNodeRegistrationScript"

	opCreateNode      = "CreateNode"
	opDescribeNode    = "DescribeNode"
	opUpdateNode      = "UpdateNode"
	opUpdateNodeState = "UpdateNodeState"
	opDeleteNode      = "DeleteNode"
	opListNodes       = "ListNodes"

	opCreateSignalMap        = "CreateSignalMap"
	opGetSignalMap           = "GetSignalMap"
	opListSignalMaps         = "ListSignalMaps"
	opDeleteSignalMap        = "DeleteSignalMap"
	opStartUpdateSignalMap   = "StartUpdateSignalMap"
	opStartMonitorDeployment = "StartMonitorDeployment"

	opCreateCWAlarmTemplateGroup = "CreateCloudWatchAlarmTemplateGroup"
	opGetCWAlarmTemplateGroup    = "GetCloudWatchAlarmTemplateGroup"
	opListCWAlarmTemplateGroups  = "ListCloudWatchAlarmTemplateGroups"
	opUpdateCWAlarmTemplateGroup = "UpdateCloudWatchAlarmTemplateGroup"
	opDeleteCWAlarmTemplateGroup = "DeleteCloudWatchAlarmTemplateGroup"

	opCreateCWAlarmTemplate = "CreateCloudWatchAlarmTemplate"
	opGetCWAlarmTemplate    = "GetCloudWatchAlarmTemplate"
	opListCWAlarmTemplates  = "ListCloudWatchAlarmTemplates"
	opUpdateCWAlarmTemplate = "UpdateCloudWatchAlarmTemplate"
	opDeleteCWAlarmTemplate = "DeleteCloudWatchAlarmTemplate"

	opCreateEBRuleTemplateGroup = "CreateEventBridgeRuleTemplateGroup"
	opGetEBRuleTemplateGroup    = "GetEventBridgeRuleTemplateGroup"
	opListEBRuleTemplateGroups  = "ListEventBridgeRuleTemplateGroups"
	opUpdateEBRuleTemplateGroup = "UpdateEventBridgeRuleTemplateGroup"
	opDeleteEBRuleTemplateGroup = "DeleteEventBridgeRuleTemplateGroup"

	opCreateEBRuleTemplate = "CreateEventBridgeRuleTemplate"
	opGetEBRuleTemplate    = "GetEventBridgeRuleTemplate"
	opListEBRuleTemplates  = "ListEventBridgeRuleTemplates"
	opUpdateEBRuleTemplate = "UpdateEventBridgeRuleTemplate"
	opDeleteEBRuleTemplate = "DeleteEventBridgeRuleTemplate"

	opListOfferings    = "ListOfferings"
	opDescribeOffering = "DescribeOffering"
	opPurchaseOffering = "PurchaseOffering"

	opListReservations    = "ListReservations"
	opDescribeReservation = "DescribeReservation"
	opDeleteReservation   = "DeleteReservation"
	opUpdateReservation   = "UpdateReservation"

	opBatchDelete         = "BatchDelete"
	opBatchStart          = "BatchStart"
	opBatchStop           = "BatchStop"
	opBatchUpdateSchedule = "BatchUpdateSchedule"

	opCreateNetwork   = "CreateNetwork"
	opDescribeNetwork = "DescribeNetwork"
	opUpdateNetwork   = "UpdateNetwork"
	opDeleteNetwork   = "DeleteNetwork"
	opListNetworks    = "ListNetworks"

	opCreateSdiSource   = "CreateSdiSource"
	opDescribeSdiSource = "DescribeSdiSource"
	opUpdateSdiSource   = "UpdateSdiSource"
	opDeleteSdiSource   = "DeleteSdiSource"
	opListSdiSources    = "ListSdiSources"

	opCreateChannelPlacementGroup   = "CreateChannelPlacementGroup"
	opDescribeChannelPlacementGroup = "DescribeChannelPlacementGroup"
	opUpdateChannelPlacementGroup   = "UpdateChannelPlacementGroup"
	opDeleteChannelPlacementGroup   = "DeleteChannelPlacementGroup"
	opListChannelPlacementGroups    = "ListChannelPlacementGroups"

	opDescribeAccountConfiguration = "DescribeAccountConfiguration"
	opUpdateAccountConfiguration   = "UpdateAccountConfiguration"

	opDescribeSchedule = "DescribeSchedule"
	opDeleteSchedule   = "DeleteSchedule"

	opListAlerts          = "ListAlerts"
	opListMultiplexAlerts = "ListMultiplexAlerts"
	opListVersions        = "ListVersions"

	opUpdateChannelClass      = "UpdateChannelClass"
	opRestartChannelPipelines = "RestartChannelPipelines"
	opDescribeThumbnails      = "DescribeThumbnails"

	opStartInputDevice                  = "StartInputDevice"
	opStopInputDevice                   = "StopInputDevice"
	opStartInputDeviceMaintenanceWindow = "StartInputDeviceMaintenanceWindow"
	opDescribeInputDeviceThumbnail      = "DescribeInputDeviceThumbnail"

	opStartDeleteMonitorDeployment = "StartDeleteMonitorDeployment"
	opCreatePartnerInput           = "CreatePartnerInput"
)

// Handler handles MediaLive HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaLive" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return append(coreOperations(), parityOperations()...)
}

// coreOperations returns the originally supported operation set.
func coreOperations() []string {
	return []string{
		opCreateChannel,
		opDescribeChannel,
		opUpdateChannel,
		opDeleteChannel,
		opListChannels,
		opStartChannel,
		opStopChannel,
		opCreateInput,
		opDescribeInput,
		opUpdateInput,
		opDeleteInput,
		opListInputs,
		opCreateInputSecurityGroup,
		opDescribeInputSecurityGroup,
		opUpdateInputSecurityGroup,
		opDeleteInputSecurityGroup,
		opListInputSecurityGroups,
		opClaimDevice,
		opListInputDevices,
		opDescribeInputDevice,
		opUpdateInputDevice,
		opRebootInputDevice,
		opTransferInputDevice,
		opAcceptInputDeviceTransfer,
		opCancelInputDeviceTransfer,
		opRejectInputDeviceTransfer,
		opListInputDeviceTransfers,
		opCreateMultiplex,
		opDescribeMultiplex,
		opUpdateMultiplex,
		opDeleteMultiplex,
		opListMultiplexes,
		opStartMultiplex,
		opStopMultiplex,
		opCreateMultiplexProgram,
		opDescribeMultiplexProgram,
		opUpdateMultiplexProgram,
		opDeleteMultiplexProgram,
		opListMultiplexPrograms,
		opCreateTags,
		opDeleteTags,
		opListTagsForResource,
		opCreateCluster,
		opDescribeCluster,
		opUpdateCluster,
		opDeleteCluster,
		opListClusters,
		opListClusterAlerts,
		opCreateNodeRegistrationScript,
		opCreateNode,
		opDescribeNode,
		opUpdateNode,
		opUpdateNodeState,
		opDeleteNode,
		opListNodes,
		opCreateSignalMap,
		opGetSignalMap,
		opListSignalMaps,
		opDeleteSignalMap,
		opStartUpdateSignalMap,
		opStartMonitorDeployment,
		opCreateCWAlarmTemplateGroup,
		opGetCWAlarmTemplateGroup,
		opListCWAlarmTemplateGroups,
		opUpdateCWAlarmTemplateGroup,
		opDeleteCWAlarmTemplateGroup,
		opCreateCWAlarmTemplate,
		opGetCWAlarmTemplate,
		opListCWAlarmTemplates,
		opUpdateCWAlarmTemplate,
		opDeleteCWAlarmTemplate,
		opCreateEBRuleTemplateGroup,
		opGetEBRuleTemplateGroup,
		opListEBRuleTemplateGroups,
		opUpdateEBRuleTemplateGroup,
		opDeleteEBRuleTemplateGroup,
		opCreateEBRuleTemplate,
		opGetEBRuleTemplate,
		opListEBRuleTemplates,
		opUpdateEBRuleTemplate,
		opDeleteEBRuleTemplate,
		opListOfferings,
		opDescribeOffering,
		opPurchaseOffering,
		opListReservations,
		opDescribeReservation,
		opDeleteReservation,
		opUpdateReservation,
		opBatchDelete,
		opBatchStart,
		opBatchStop,
		opBatchUpdateSchedule,
	}
}

// parityOperations returns the operations added to reach full SDK parity.
func parityOperations() []string {
	return []string{
		opCreateNetwork,
		opDescribeNetwork,
		opUpdateNetwork,
		opDeleteNetwork,
		opListNetworks,
		opCreateSdiSource,
		opDescribeSdiSource,
		opUpdateSdiSource,
		opDeleteSdiSource,
		opListSdiSources,
		opCreateChannelPlacementGroup,
		opDescribeChannelPlacementGroup,
		opUpdateChannelPlacementGroup,
		opDeleteChannelPlacementGroup,
		opListChannelPlacementGroups,
		opDescribeAccountConfiguration,
		opUpdateAccountConfiguration,
		opDescribeSchedule,
		opDeleteSchedule,
		opListAlerts,
		opListMultiplexAlerts,
		opListVersions,
		opUpdateChannelClass,
		opRestartChannelPipelines,
		opDescribeThumbnails,
		opStartInputDevice,
		opStopInputDevice,
		opStartInputDeviceMaintenanceWindow,
		opDescribeInputDeviceThumbnail,
		opStartDeleteMonitorDeployment,
		opCreatePartnerInput,
	}
}

// RouteMatcher returns a function that matches MediaLive requests by path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	op, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
			err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "invalid JSON body"})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	if fn, ok := h.coreHandlers(c, resource, body)[op]; ok {
		return fn()
	}

	if fn, ok := h.parityHandlers(c, resource, body)[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
}

// coreHandlers returns the dispatch map for the core operations.
func (h *Handler) coreHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateChannel:                func() error { return h.handleCreateChannel(c, body) },
		opDescribeChannel:              func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:                func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:                func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:                 func() error { return h.handleListChannels(c) },
		opStartChannel:                 func() error { return h.handleStartChannel(c, resource) },
		opStopChannel:                  func() error { return h.handleStopChannel(c, resource) },
		opCreateInput:                  func() error { return h.handleCreateInput(c, body) },
		opDescribeInput:                func() error { return h.handleDescribeInput(c, resource) },
		opUpdateInput:                  func() error { return h.handleUpdateInput(c, resource, body) },
		opDeleteInput:                  func() error { return h.handleDeleteInput(c, resource) },
		opListInputs:                   func() error { return h.handleListInputs(c) },
		opCreateInputSecurityGroup:     func() error { return h.handleCreateInputSecurityGroup(c, body) },
		opDescribeInputSecurityGroup:   func() error { return h.handleDescribeInputSecurityGroup(c, resource) },
		opUpdateInputSecurityGroup:     func() error { return h.handleUpdateInputSecurityGroup(c, resource, body) },
		opDeleteInputSecurityGroup:     func() error { return h.handleDeleteInputSecurityGroup(c, resource) },
		opListInputSecurityGroups:      func() error { return h.handleListInputSecurityGroups(c) },
		opClaimDevice:                  func() error { return h.handleClaimDevice(c, body) },
		opListInputDevices:             func() error { return h.handleListInputDevices(c) },
		opDescribeInputDevice:          func() error { return h.handleDescribeInputDevice(c, resource) },
		opUpdateInputDevice:            func() error { return h.handleUpdateInputDevice(c, resource, body) },
		opRebootInputDevice:            func() error { return h.handleRebootInputDevice(c, resource) },
		opTransferInputDevice:          func() error { return h.handleTransferInputDevice(c, resource, body) },
		opAcceptInputDeviceTransfer:    func() error { return h.handleAcceptInputDeviceTransfer(c, resource) },
		opCancelInputDeviceTransfer:    func() error { return h.handleCancelInputDeviceTransfer(c, resource) },
		opRejectInputDeviceTransfer:    func() error { return h.handleRejectInputDeviceTransfer(c, resource) },
		opListInputDeviceTransfers:     func() error { return h.handleListInputDeviceTransfers(c) },
		opCreateMultiplex:              func() error { return h.handleCreateMultiplex(c, body) },
		opDescribeMultiplex:            func() error { return h.handleDescribeMultiplex(c, resource) },
		opUpdateMultiplex:              func() error { return h.handleUpdateMultiplex(c, resource, body) },
		opDeleteMultiplex:              func() error { return h.handleDeleteMultiplex(c, resource) },
		opListMultiplexes:              func() error { return h.handleListMultiplexes(c) },
		opStartMultiplex:               func() error { return h.handleStartMultiplex(c, resource) },
		opStopMultiplex:                func() error { return h.handleStopMultiplex(c, resource) },
		opCreateMultiplexProgram:       func() error { return h.handleCreateMultiplexProgram(c, resource, body) },
		opDescribeMultiplexProgram:     func() error { return h.handleDescribeMultiplexProgram(c, resource) },
		opUpdateMultiplexProgram:       func() error { return h.handleUpdateMultiplexProgram(c, resource, body) },
		opDeleteMultiplexProgram:       func() error { return h.handleDeleteMultiplexProgram(c, resource) },
		opListMultiplexPrograms:        func() error { return h.handleListMultiplexPrograms(c, resource) },
		opCreateTags:                   func() error { return h.handleCreateTags(c, resource, body) },
		opDeleteTags:                   func() error { return h.handleDeleteTags(c, resource) },
		opListTagsForResource:          func() error { return h.handleListTagsForResource(c, resource) },
		opCreateCluster:                func() error { return h.handleCreateCluster(c, body) },
		opDescribeCluster:              func() error { return h.handleDescribeCluster(c, resource) },
		opUpdateCluster:                func() error { return h.handleUpdateCluster(c, resource, body) },
		opDeleteCluster:                func() error { return h.handleDeleteCluster(c, resource) },
		opListClusters:                 func() error { return h.handleListClusters(c) },
		opListClusterAlerts:            func() error { return h.handleListClusterAlerts(c, resource) },
		opCreateNodeRegistrationScript: func() error { return h.handleCreateNodeRegistrationScript(c, resource) },
		opCreateNode:                   func() error { return h.handleCreateNode(c, resource, body) },
		opDescribeNode:                 func() error { return h.handleDescribeNode(c, resource) },
		opUpdateNode:                   func() error { return h.handleUpdateNode(c, resource, body) },
		opUpdateNodeState:              func() error { return h.handleUpdateNodeState(c, resource, body) },
		opDeleteNode:                   func() error { return h.handleDeleteNode(c, resource) },
		opListNodes:                    func() error { return h.handleListNodes(c, resource) },
		opCreateSignalMap:              func() error { return h.handleCreateSignalMap(c, body) },
		opGetSignalMap:                 func() error { return h.handleGetSignalMap(c, resource) },
		opListSignalMaps:               func() error { return h.handleListSignalMaps(c) },
		opDeleteSignalMap:              func() error { return h.handleDeleteSignalMap(c, resource) },
		opStartUpdateSignalMap:         func() error { return h.handleStartUpdateSignalMap(c, resource, body) },
		opStartMonitorDeployment:       func() error { return h.handleStartMonitorDeployment(c, resource) },
		opCreateCWAlarmTemplateGroup:   func() error { return h.handleCreateCWAlarmTemplateGroup(c, body) },
		opGetCWAlarmTemplateGroup:      func() error { return h.handleGetCWAlarmTemplateGroup(c, resource) },
		opListCWAlarmTemplateGroups:    func() error { return h.handleListCWAlarmTemplateGroups(c) },
		opUpdateCWAlarmTemplateGroup:   func() error { return h.handleUpdateCWAlarmTemplateGroup(c, resource, body) },
		opDeleteCWAlarmTemplateGroup:   func() error { return h.handleDeleteCWAlarmTemplateGroup(c, resource) },
		opCreateCWAlarmTemplate:        func() error { return h.handleCreateCWAlarmTemplate(c, body) },
		opGetCWAlarmTemplate:           func() error { return h.handleGetCWAlarmTemplate(c, resource) },
		opListCWAlarmTemplates:         func() error { return h.handleListCWAlarmTemplates(c) },
		opUpdateCWAlarmTemplate:        func() error { return h.handleUpdateCWAlarmTemplate(c, resource, body) },
		opDeleteCWAlarmTemplate:        func() error { return h.handleDeleteCWAlarmTemplate(c, resource) },
		opCreateEBRuleTemplateGroup:    func() error { return h.handleCreateEBRuleTemplateGroup(c, body) },
		opGetEBRuleTemplateGroup:       func() error { return h.handleGetEBRuleTemplateGroup(c, resource) },
		opListEBRuleTemplateGroups:     func() error { return h.handleListEBRuleTemplateGroups(c) },
		opUpdateEBRuleTemplateGroup:    func() error { return h.handleUpdateEBRuleTemplateGroup(c, resource, body) },
		opDeleteEBRuleTemplateGroup:    func() error { return h.handleDeleteEBRuleTemplateGroup(c, resource) },
		opCreateEBRuleTemplate:         func() error { return h.handleCreateEBRuleTemplate(c, body) },
		opGetEBRuleTemplate:            func() error { return h.handleGetEBRuleTemplate(c, resource) },
		opListEBRuleTemplates:          func() error { return h.handleListEBRuleTemplates(c) },
		opUpdateEBRuleTemplate:         func() error { return h.handleUpdateEBRuleTemplate(c, resource, body) },
		opDeleteEBRuleTemplate:         func() error { return h.handleDeleteEBRuleTemplate(c, resource) },
		opListOfferings:                func() error { return h.handleListOfferings(c) },
		opDescribeOffering:             func() error { return h.handleDescribeOffering(c, resource) },
		opPurchaseOffering:             func() error { return h.handlePurchaseOffering(c, resource, body) },
		opListReservations:             func() error { return h.handleListReservations(c) },
		opDescribeReservation:          func() error { return h.handleDescribeReservation(c, resource) },
		opDeleteReservation:            func() error { return h.handleDeleteReservation(c, resource) },
		opUpdateReservation:            func() error { return h.handleUpdateReservation(c, resource, body) },
		opBatchDelete:                  func() error { return h.handleBatchDelete(c, body) },
		opBatchStart:                   func() error { return h.handleBatchStart(c, body) },
		opBatchStop:                    func() error { return h.handleBatchStop(c, body) },
		opBatchUpdateSchedule:          func() error { return h.handleBatchUpdateSchedule(c, resource, body) },
	}
}

// parityHandlers returns the dispatch map for the parity operations.
func (h *Handler) parityHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateNetwork:     func() error { return h.handleCreateNetwork(c, body) },
		opDescribeNetwork:   func() error { return h.handleDescribeNetwork(c, resource) },
		opUpdateNetwork:     func() error { return h.handleUpdateNetwork(c, resource, body) },
		opDeleteNetwork:     func() error { return h.handleDeleteNetwork(c, resource) },
		opListNetworks:      func() error { return h.handleListNetworks(c) },
		opCreateSdiSource:   func() error { return h.handleCreateSdiSource(c, body) },
		opDescribeSdiSource: func() error { return h.handleDescribeSdiSource(c, resource) },
		opUpdateSdiSource:   func() error { return h.handleUpdateSdiSource(c, resource, body) },
		opDeleteSdiSource:   func() error { return h.handleDeleteSdiSource(c, resource) },
		opListSdiSources:    func() error { return h.handleListSdiSources(c) },
		opCreateChannelPlacementGroup: func() error {
			return h.handleCreateChannelPlacementGroup(c, resource, body)
		},
		opDescribeChannelPlacementGroup: func() error {
			return h.handleDescribeChannelPlacementGroup(c, resource)
		},
		opUpdateChannelPlacementGroup: func() error {
			return h.handleUpdateChannelPlacementGroup(c, resource, body)
		},
		opDeleteChannelPlacementGroup: func() error {
			return h.handleDeleteChannelPlacementGroup(c, resource)
		},
		opListChannelPlacementGroups: func() error {
			return h.handleListChannelPlacementGroups(c, resource)
		},
		opDescribeAccountConfiguration: func() error {
			return h.handleDescribeAccountConfiguration(c)
		},
		opUpdateAccountConfiguration: func() error { return h.handleUpdateAccountConfiguration(c, body) },
		opDescribeSchedule:           func() error { return h.handleDescribeSchedule(c, resource) },
		opDeleteSchedule:             func() error { return h.handleDeleteSchedule(c, resource) },
		opListAlerts:                 func() error { return h.handleListAlerts(c, resource) },
		opListMultiplexAlerts:        func() error { return h.handleListMultiplexAlerts(c, resource) },
		opListVersions:               func() error { return h.handleListVersions(c) },
		opUpdateChannelClass:         func() error { return h.handleUpdateChannelClass(c, resource, body) },
		opRestartChannelPipelines:    func() error { return h.handleRestartChannelPipelines(c, resource) },
		opDescribeThumbnails:         func() error { return h.handleDescribeThumbnails(c, resource) },
		opStartInputDevice:           func() error { return h.handleStartInputDevice(c, resource) },
		opStopInputDevice:            func() error { return h.handleStopInputDevice(c, resource) },
		opStartInputDeviceMaintenanceWindow: func() error {
			return h.handleStartInputDeviceMaintenanceWindow(c, resource)
		},
		opDescribeInputDeviceThumbnail: func() error {
			return h.handleDescribeInputDeviceThumbnail(c, resource)
		},
		opStartDeleteMonitorDeployment: func() error {
			return h.handleStartDeleteMonitorDeployment(c, resource)
		},
		opCreatePartnerInput: func() error { return h.handleCreatePartnerInput(c, resource, body) },
	}
}

func errStatus(err error) int {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, awserr.ErrAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, awserr.ErrInvalidParameter):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// amznErrorTypeHeader carries the modeled exception type for the restjson1
// protocol. aws-sdk-go-v2's restjson.GetErrorInfo (aws/protocol/restjson/decoder_util.go)
// reads this header before falling back to a body "code"/"__type" field; without it every
// error here deserialized client-side as a generic UnknownError -- the exact bug fixed for
// the sibling mediatailor service in f41d5b42f.
const amznErrorTypeHeader = "X-Amzn-Errortype"

// errType maps err to the wire exception type name, verified against this
// service's own deserializer error lists (medialive@v1.101.4 deserializers.go:
// CreateChannel/DescribeChannel/DeleteChannel/UpdateChannel/DeleteInput model
// exactly NotFoundException, ConflictException, BadRequestException,
// InternalServerErrorException, ForbiddenException, TooManyRequestsException,
// BadGatewayException, GatewayTimeoutException, UnprocessableEntityException).
func errType(err error) string {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return "NotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists):
		return "ConflictException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		return "BadRequestException"
	default:
		return "InternalServerErrorException"
	}
}

func respondErr(c *echo.Context, err error) error {
	c.Response().Header().Set(amznErrorTypeHeader, errType(err))

	return c.JSON(errStatus(err), map[string]any{keyMessage: err.Error()})
}

func intFromAny(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}

	return 0
}

// int32FromAny reads a JSON-decoded numeric value and truncates it to
// int32. Every gopherstack-jb9i EncoderSettings/InputAttachment field this
// feeds (SyncThreshold, BlackFrameMsec, ErrorClearTimeMsec,
// AudioSilenceThresholdMsec, ...) is documented by the real SDK as a small
// bounded millisecond/frame count, so a wider-than-int32 input is malformed
// input, not a security-relevant overflow (same rationale as
// services/mediatailor's int32Field).
//
//nolint:gosec // G115: bounded millisecond/frame-count fields, see comment above
func int32FromAny(v any) int32 {
	return int32(intFromAny(v))
}

func float64FromAny(v any) float64 {
	f, _ := v.(float64)

	return f
}

func extractTags(body map[string]any) map[string]string {
	raw, hasTags := body["tags"].(map[string]any)
	if !hasTags {
		raw, _ = body["Tags"].(map[string]any)
	}
	if len(raw) == 0 {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}

	return tags
}

func extractTagKeys(c *echo.Context) []string {
	return c.Request().URL.Query()["tagKeys"]
}

func extractStringSlice(body map[string]any, key string) []string {
	raw, _ := body[key].([]any)
	result := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}

	return result
}

// --- Account configuration handlers ---

func (h *Handler) handleDescribeAccountConfiguration(c *echo.Context) error {
	cfg, err := h.Backend.DescribeAccountConfiguration()
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accountConfiguration": map[string]any{"kmsKeyId": cfg.KmsKeyID},
	})
}

func (h *Handler) handleUpdateAccountConfiguration(c *echo.Context, body map[string]any) error {
	kmsKeyID := ""
	if cfg, ok := body["accountConfiguration"].(map[string]any); ok {
		kmsKeyID, _ = cfg["kmsKeyId"].(string)
	}

	cfg, err := h.Backend.UpdateAccountConfiguration(kmsKeyID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accountConfiguration": map[string]any{"kmsKeyId": cfg.KmsKeyID},
	})
}
