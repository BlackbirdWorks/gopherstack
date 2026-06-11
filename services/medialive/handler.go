package medialive

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

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
	keyArn             = "Arn"
	keyID              = "Id"
	keyName            = "Name"
	keyState           = "State"
	keyTags            = "Tags"
	keyDescription     = "Description"
	keyChannel         = "Channel"
	keyInput           = "Input"
	keyAlerts          = "Alerts"
	keyActionName      = "ActionName"
	keyScheduleActions = "ScheduleActions"
	keySdiSource       = "SdiSource"
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

// classifyPath maps (method, path) → (operation, resource).
// For MultiplexProgram ops, resource is "multiplexID/programName".
func classifyPath(method, path string) (string, string) {
	if op, res, ok := classifyChannelPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyInputPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyInputSecurityGroupPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyInputDevicePath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyMultiplexPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyClusterPath(method, path); ok {
		return op, res
	}

	if strings.HasPrefix(path, pathTags) {
		return classifyTagPath(method, path)
	}

	if op, res, ok := classifySignalMapPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyAnyTemplatePath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyOfferingPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyReservationPath(method, path); ok {
		return op, res
	}

	if op, ok := classifyBatchPath(method, path); ok {
		return op, ""
	}

	if op, res, ok := classifyParityPath(method, path); ok {
		return op, res
	}

	return opUnknown, ""
}

// classifyAnyTemplatePath classifies all four CRUD-only template resources.
func classifyAnyTemplatePath(method, path string) (string, string, bool) {
	if op, res, ok := classifyTemplatePath(method, path, pathCWAlarmTemplateGroups,
		opCreateCWAlarmTemplateGroup, opGetCWAlarmTemplateGroup,
		opListCWAlarmTemplateGroups, opUpdateCWAlarmTemplateGroup,
		opDeleteCWAlarmTemplateGroup); ok {
		return op, res, true
	}

	if op, res, ok := classifyTemplatePath(method, path, pathCWAlarmTemplates,
		opCreateCWAlarmTemplate, opGetCWAlarmTemplate,
		opListCWAlarmTemplates, opUpdateCWAlarmTemplate,
		opDeleteCWAlarmTemplate); ok {
		return op, res, true
	}

	if op, res, ok := classifyTemplatePath(method, path, pathEBRuleTemplateGroups,
		opCreateEBRuleTemplateGroup, opGetEBRuleTemplateGroup,
		opListEBRuleTemplateGroups, opUpdateEBRuleTemplateGroup,
		opDeleteEBRuleTemplateGroup); ok {
		return op, res, true
	}

	if op, res, ok := classifyTemplatePath(method, path, pathEBRuleTemplates,
		opCreateEBRuleTemplate, opGetEBRuleTemplate,
		opListEBRuleTemplates, opUpdateEBRuleTemplate,
		opDeleteEBRuleTemplate); ok {
		return op, res, true
	}

	return "", "", false
}

// classifyParityPath classifies the standalone parity resources:
// networks, SDI sources, account configuration and versions.
func classifyParityPath(method, path string) (string, string, bool) {
	if op, res, ok := classifyNetworkPath(method, path); ok {
		return op, res, true
	}

	if op, res, ok := classifySdiSourcePath(method, path); ok {
		return op, res, true
	}

	if op, ok := classifyAccountConfigurationPath(method, path); ok {
		return op, "", true
	}

	if op, ok := classifyVersionsPath(method, path); ok {
		return op, "", true
	}

	return "", "", false
}

// classifyNetworkPath classifies /prod/networks paths.
func classifyNetworkPath(method, path string) (string, string, bool) {
	const prefix = pathNetworks + "/"

	switch {
	case path == pathNetworks && method == http.MethodGet:
		return opListNetworks, "", true
	case path == pathNetworks && method == http.MethodPost:
		return opCreateNetwork, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeNetwork, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateNetwork, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteNetwork, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

// classifySdiSourcePath classifies /prod/sdiSources paths.
func classifySdiSourcePath(method, path string) (string, string, bool) {
	const prefix = pathSdiSources + "/"

	switch {
	case path == pathSdiSources && method == http.MethodGet:
		return opListSdiSources, "", true
	case path == pathSdiSources && method == http.MethodPost:
		return opCreateSdiSource, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeSdiSource, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateSdiSource, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteSdiSource, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

// classifyAccountConfigurationPath classifies /prod/accountConfiguration paths.
func classifyAccountConfigurationPath(method, path string) (string, bool) {
	if path != pathAccountConfiguration {
		return "", false
	}

	switch method {
	case http.MethodGet:
		return opDescribeAccountConfiguration, true
	case http.MethodPut:
		return opUpdateAccountConfiguration, true
	}

	return "", false
}

// classifyVersionsPath classifies /prod/versions paths.
func classifyVersionsPath(method, path string) (string, bool) {
	if path == pathVersions && method == http.MethodGet {
		return opListVersions, true
	}

	return "", false
}

func classifyMultiplexPath(method, path string) (string, string, bool) {
	if path == pathMultiplexes {
		return classifyMultiplexRoot(method)
	}

	after, ok := strings.CutPrefix(path, pathMultiplexes+"/")
	if !ok {
		return "", "", false
	}

	parts := strings.SplitN(after, "/", pathSegmentsNamed)
	id := parts[0]

	if id == "" {
		return "", "", false
	}

	switch len(parts) {
	case pathSegmentsID:
		return classifyMultiplexIDOnly(method, id)
	case pathSegmentsSub:
		return classifyMultiplexSubpath(method, id, parts[1])
	case pathSegmentsNamed:
		return classifyMultiplexProgramPath(method, id, parts[1], parts[2])
	}

	return "", "", false
}

func classifyMultiplexRoot(method string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opListMultiplexes, "", true
	case http.MethodPost:
		return opCreateMultiplex, "", true
	}

	return "", "", false
}

func classifyMultiplexIDOnly(method, id string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opDescribeMultiplex, id, true
	case http.MethodPut:
		return opUpdateMultiplex, id, true
	case http.MethodDelete:
		return opDeleteMultiplex, id, true
	}

	return "", "", false
}

func classifyMultiplexSubpath(method, id, sub string) (string, string, bool) {
	switch {
	case sub == subStart && method == http.MethodPost:
		return opStartMultiplex, id, true
	case sub == subStop && method == http.MethodPost:
		return opStopMultiplex, id, true
	case sub == subPrograms && method == http.MethodGet:
		return opListMultiplexPrograms, id, true
	case sub == subPrograms && method == http.MethodPost:
		return opCreateMultiplexProgram, id, true
	case sub == subAlerts && method == http.MethodGet:
		return opListMultiplexAlerts, id, true
	}

	return "", "", false
}

func classifyMultiplexProgramPath(method, id, sub, name string) (string, string, bool) {
	if sub != subPrograms || name == "" {
		return "", "", false
	}

	compound := id + "/" + name

	switch method {
	case http.MethodGet:
		return opDescribeMultiplexProgram, compound, true
	case http.MethodPut:
		return opUpdateMultiplexProgram, compound, true
	case http.MethodDelete:
		return opDeleteMultiplexProgram, compound, true
	}

	return "", "", false
}

// splitMultiplexProgram splits the compound resource "multiplexID/programName".
func splitMultiplexProgram(resource string) (string, string) {
	before, after, _ := strings.Cut(resource, "/")

	return before, after
}

func classifyChannelPath(method, path string) (string, string, bool) {
	const prefix = pathChannels + "/"
	if path == pathChannels {
		switch method {
		case http.MethodGet:
			return opListChannels, "", true
		case http.MethodPost:
			return opCreateChannel, "", true
		}

		return "", "", false
	}

	return classifyChannelSubPath(method, path, prefix)
}

// channelSubAction maps a path suffix + HTTP method to an operation.
type channelSubAction struct {
	suffix string
	method string
	op     string
}

func classifyChannelSubPath(method, path, prefix string) (string, string, bool) {
	subActions := []channelSubAction{
		{"/start", http.MethodPost, opStartChannel},
		{"/stop", http.MethodPost, opStopChannel},
		{"/" + subSchedule, http.MethodPut, opBatchUpdateSchedule},
		{"/" + subSchedule, http.MethodGet, opDescribeSchedule},
		{"/" + subSchedule, http.MethodDelete, opDeleteSchedule},
		{"/" + subChannelClass, http.MethodPut, opUpdateChannelClass},
		{"/" + subRestartChannelPipelines, http.MethodPost, opRestartChannelPipelines},
		{"/" + subThumbnails, http.MethodGet, opDescribeThumbnails},
		{"/" + subAlerts, http.MethodGet, opListAlerts},
	}

	for _, a := range subActions {
		if a.method == method && matchSegment(path, prefix, a.suffix) {
			return a.op, extractSegment(path, prefix, a.suffix), true
		}
	}

	return classifyChannelIDOnly(method, path, prefix)
}

func classifyChannelIDOnly(method, path, prefix string) (string, string, bool) {
	if !matchSegment(path, prefix, "") {
		return "", "", false
	}

	switch method {
	case http.MethodGet:
		return opDescribeChannel, extractSegment(path, prefix, ""), true
	case http.MethodPut:
		return opUpdateChannel, extractSegment(path, prefix, ""), true
	case http.MethodDelete:
		return opDeleteChannel, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyInputPath(method, path string) (string, string, bool) {
	const prefix = pathInputs + "/"

	switch {
	case path == pathInputs && method == http.MethodGet:
		return opListInputs, "", true
	case path == pathInputs && method == http.MethodPost:
		return opCreateInput, "", true
	case matchSegment(path, prefix, "/"+subPartners) && method == http.MethodPost:
		return opCreatePartnerInput, extractSegment(path, prefix, "/"+subPartners), true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeInput, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateInput, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteInput, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyInputSecurityGroupPath(method, path string) (string, string, bool) {
	const prefix = pathInputSecurityGroups + "/"

	switch {
	case path == pathInputSecurityGroups && method == http.MethodGet:
		return opListInputSecurityGroups, "", true
	case path == pathInputSecurityGroups && method == http.MethodPost:
		return opCreateInputSecurityGroup, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeInputSecurityGroup, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateInputSecurityGroup, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteInputSecurityGroup, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyInputDevicePath(method, path string) (string, string, bool) {
	const prefix = pathInputDevices + "/"

	switch {
	case path == pathClaimDevice && method == http.MethodPost:
		return opClaimDevice, "", true
	case path == pathInputDevices && method == http.MethodGet:
		return opListInputDevices, "", true
	case path == pathInputDeviceTransfers && method == http.MethodGet:
		return opListInputDeviceTransfers, "", true
	case strings.HasPrefix(path, prefix):
		return classifyInputDeviceSubPath(method, path, prefix)
	}

	return "", "", false
}

// classifyInputDeviceSubPath handles paths of the form /prod/inputDevices/{id}[/action].
func classifyInputDeviceSubPath(method, path, prefix string) (string, string, bool) {
	// POST sub-actions: /prod/inputDevices/{id}/accept|cancel|reboot|reject|transfer
	postActions := map[string]string{
		"/accept":                  opAcceptInputDeviceTransfer,
		"/cancel":                  opCancelInputDeviceTransfer,
		"/reboot":                  opRebootInputDevice,
		"/reject":                  opRejectInputDeviceTransfer,
		"/transfer":                opTransferInputDevice,
		"/" + subStart:             opStartInputDevice,
		"/" + subStop:              opStopInputDevice,
		"/" + subMaintenanceWindow: opStartInputDeviceMaintenanceWindow,
	}

	if method == http.MethodPost {
		for suffix, op := range postActions {
			if matchSegment(path, prefix, suffix) {
				return op, extractSegment(path, prefix, suffix), true
			}
		}
	}

	if matchSegment(path, prefix, "/"+subThumbnailData) && method == http.MethodGet {
		return opDescribeInputDeviceThumbnail, extractSegment(path, prefix, "/"+subThumbnailData), true
	}

	if matchSegment(path, prefix, "") && method == http.MethodGet {
		return opDescribeInputDevice, extractSegment(path, prefix, ""), true
	}

	if matchSegment(path, prefix, "") && method == http.MethodPut {
		return opUpdateInputDevice, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyTagPath(method, path string) (string, string) {
	resource := strings.TrimPrefix(path, pathTags)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resource
	case http.MethodPost:
		return opCreateTags, resource
	case http.MethodDelete:
		return opDeleteTags, resource
	}

	return opUnknown, ""
}

// matchSegment returns true when path has the form prefix+<id>+suffix.
func matchSegment(path, prefix, suffix string) bool {
	after, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return false
	}

	if suffix == "" {
		return !strings.Contains(after, "/")
	}

	id, hasSuffix := strings.CutSuffix(after, suffix)

	return hasSuffix && !strings.Contains(id, "/")
}

// extractSegment extracts the <id> from prefix+<id>+suffix.
func extractSegment(path, prefix, suffix string) string {
	after, _ := strings.CutPrefix(path, prefix)
	if suffix == "" {
		return after
	}

	id, _ := strings.CutSuffix(after, suffix)

	return id
}

// classifySignalMapPath classifies /prod/signal-maps paths.
func classifySignalMapPath(method, path string) (string, string, bool) {
	const prefix = pathSignalMaps + "/"

	switch {
	case path == pathSignalMaps && method == http.MethodGet:
		return opListSignalMaps, "", true
	case path == pathSignalMaps && method == http.MethodPost:
		return opCreateSignalMap, "", true
	case matchSegment(path, prefix, "/"+subMonitorDeployment) && method == http.MethodPost:
		return opStartMonitorDeployment, extractSegment(
			path,
			prefix,
			"/"+subMonitorDeployment,
		), true
	case matchSegment(path, prefix, "/"+subMonitorDeployment) && method == http.MethodDelete:
		return opStartDeleteMonitorDeployment, extractSegment(
			path,
			prefix,
			"/"+subMonitorDeployment,
		), true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opGetSignalMap, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteSignalMap, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPatch:
		return opStartUpdateSignalMap, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

// classifyTemplatePath is a generic classifier for CRUD-only template resources.
func classifyTemplatePath(
	method, path, prefix string,
	createOp, getOp, listOp, updateOp, deleteOp string,
) (string, string, bool) {
	pre := prefix + "/"

	switch {
	case path == prefix && method == http.MethodGet:
		return listOp, "", true
	case path == prefix && method == http.MethodPost:
		return createOp, "", true
	case matchSegment(path, pre, "") && method == http.MethodGet:
		return getOp, extractSegment(path, pre, ""), true
	case matchSegment(path, pre, "") && method == http.MethodDelete:
		return deleteOp, extractSegment(path, pre, ""), true
	case matchSegment(path, pre, "") && method == http.MethodPatch:
		return updateOp, extractSegment(path, pre, ""), true
	}

	return "", "", false
}

// classifyOfferingPath classifies /prod/offerings paths.
func classifyOfferingPath(method, path string) (string, string, bool) {
	const prefix = pathOfferings + "/"

	switch {
	case path == pathOfferings && method == http.MethodGet:
		return opListOfferings, "", true
	case matchSegment(path, prefix, "/"+subPurchase) && method == http.MethodPost:
		return opPurchaseOffering, extractSegment(path, prefix, "/"+subPurchase), true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeOffering, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

// classifyReservationPath classifies /prod/reservations paths.
func classifyReservationPath(method, path string) (string, string, bool) {
	const prefix = pathReservations + "/"

	switch {
	case path == pathReservations && method == http.MethodGet:
		return opListReservations, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeReservation, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteReservation, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateReservation, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

// classifyBatchPath classifies /prod/batch/* paths.
func classifyBatchPath(method, path string) (string, bool) {
	switch {
	case path == pathBatch+"/delete" && method == http.MethodPost:
		return opBatchDelete, true
	case path == pathBatch+"/start" && method == http.MethodPost:
		return opBatchStart, true
	case path == pathBatch+"/stop" && method == http.MethodPost:
		return opBatchStop, true
	}

	return "", false
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

func respondErr(c *echo.Context, err error) error {
	return c.JSON(errStatus(err), map[string]any{keyMessage: err.Error()})
}

// --- Channel handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type channelOutput struct {
	Tags         map[string]string `json:"Tags"`
	Arn          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	ChannelClass string            `json:"ChannelClass"`
	RoleArn      string            `json:"RoleArn"`
	State        string            `json:"State"`
}

func toChannelOutput(ch *Channel) channelOutput {
	tags := ch.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return channelOutput{
		Tags:         tags,
		Arn:          ch.ARN,
		ID:           ch.ID,
		Name:         ch.Name,
		ChannelClass: ch.ChannelClass,
		RoleArn:      ch.RoleARN,
		State:        ch.State,
	}
}

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	channelClass, _ := body["ChannelClass"].(string)
	roleArn, _ := body["RoleArn"].(string)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(name, channelClass, roleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DescribeChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	name, _ := body["Name"].(string)
	roleArn, _ := body["RoleArn"].(string)

	ch, err := h.Backend.UpdateChannel(channelID, name, roleArn)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDeleteChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DeleteChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:         s.ARN,
			keyID:          s.ID,
			"Name":         s.Name,
			"ChannelClass": s.ChannelClass,
			keyState:       s.State,
		})
	}

	resp := map[string]any{"Channels": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StartChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleStopChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StopChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// --- Input handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type inputOutput struct {
	Tags    map[string]string `json:"Tags"`
	Arn     string            `json:"Arn"`
	ID      string            `json:"Id"`
	Name    string            `json:"Name"`
	Type    string            `json:"Type"`
	RoleArn string            `json:"RoleArn"`
	State   string            `json:"State"`
}

func toInputOutput(inp *Input) inputOutput {
	tags := inp.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return inputOutput{
		Tags:    tags,
		Arn:     inp.ARN,
		ID:      inp.ID,
		Name:    inp.Name,
		Type:    inp.InputType,
		RoleArn: inp.RoleARN,
		State:   inp.State,
	}
}

func (h *Handler) handleCreateInput(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	inputType, _ := body["Type"].(string)
	roleArn, _ := body["RoleArn"].(string)
	tags := extractTags(body)

	inp, err := h.Backend.CreateInput(name, inputType, roleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyInput: toInputOutput(inp)})
}

func (h *Handler) handleDescribeInput(c *echo.Context, inputID string) error {
	inp, err := h.Backend.DescribeInput(inputID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputOutput(inp))
}

func (h *Handler) handleUpdateInput(c *echo.Context, inputID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	roleArn, _ := body["RoleArn"].(string)

	inp, err := h.Backend.UpdateInput(inputID, name, roleArn)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyInput: toInputOutput(inp)})
}

func (h *Handler) handleDeleteInput(c *echo.Context, inputID string) error {
	if err := h.Backend.DeleteInput(inputID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputs(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListInputs(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:   s.ARN,
			keyID:    s.ID,
			"Name":   s.Name,
			"Type":   s.InputType,
			keyState: s.State,
		})
	}

	resp := map[string]any{"Inputs": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- InputSecurityGroup handlers ---

// Tags first, then strings, then slice: reduces GC pointer scan from 80 to 64 bytes.
type inputSecurityGroupOutput struct {
	Tags           map[string]string `json:"tags"`
	Arn            string            `json:"arn"`
	ID             string            `json:"id"`
	State          string            `json:"state"`
	WhitelistRules []map[string]any  `json:"whitelistRules"`
}

func toGroupOutput(g *InputSecurityGroup) inputSecurityGroupOutput {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	rules := make([]map[string]any, 0, len(g.WhitelistRules))
	for _, r := range g.WhitelistRules {
		rules = append(rules, map[string]any{"cidr": r.Cidr})
	}

	return inputSecurityGroupOutput{
		Tags:           tags,
		Arn:            g.ARN,
		ID:             g.ID,
		State:          g.State,
		WhitelistRules: rules,
	}
}

func extractWhitelistRules(body map[string]any) []WhitelistRule {
	raw, ok := body["whitelistRules"].([]any)
	if !ok {
		raw, _ = body["WhitelistRules"].([]any)
	}
	rules := make([]WhitelistRule, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		cidr, hasCidr := m["cidr"].(string)
		if !hasCidr {
			cidr, _ = m["Cidr"].(string)
		}
		if cidr != "" {
			rules = append(rules, WhitelistRule{Cidr: cidr})
		}
	}

	return rules
}

func (h *Handler) handleCreateInputSecurityGroup(c *echo.Context, body map[string]any) error {
	rules := extractWhitelistRules(body)
	tags := extractTags(body)

	g, err := h.Backend.CreateInputSecurityGroup(rules, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"securityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDescribeInputSecurityGroup(c *echo.Context, groupID string) error {
	g, err := h.Backend.DescribeInputSecurityGroup(groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toGroupOutput(g))
}

func (h *Handler) handleUpdateInputSecurityGroup(
	c *echo.Context,
	groupID string,
	body map[string]any,
) error {
	rules := extractWhitelistRules(body)

	g, err := h.Backend.UpdateInputSecurityGroup(groupID, rules)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"securityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDeleteInputSecurityGroup(c *echo.Context, groupID string) error {
	if err := h.Backend.DeleteInputSecurityGroup(groupID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputSecurityGroups(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListInputSecurityGroups(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		rules := make([]map[string]any, 0, len(s.WhitelistRules))
		for _, r := range s.WhitelistRules {
			rules = append(rules, map[string]any{"cidr": r.Cidr})
		}
		out = append(out, map[string]any{
			"arn":            s.ARN,
			"id":             s.ID,
			"state":          s.State,
			"whitelistRules": rules,
		})
	}

	resp := map[string]any{"inputSecurityGroups": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Tag handlers ---

func (h *Handler) handleCreateTags(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.CreateTags(resourceARN, tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDeleteTags(c *echo.Context, resourceARN string) error {
	keys := extractTagKeys(c)

	if err := h.Backend.DeleteTags(resourceARN, keys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return respondErr(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{keyTags: tags})
}

// --- Multiplex handlers ---

type multiplexSettingsOutput struct {
	TransportStreamBitrate              int `json:"TransportStreamBitrate"`
	TransportStreamID                   int `json:"TransportStreamId"`
	TransportStreamReservedBitrate      int `json:"TransportStreamReservedBitrate"`
	MaximumVideoBufferDelayMilliseconds int `json:"MaximumVideoBufferDelayMilliseconds"`
}

// Tags and AvailabilityZones first: reduces GC pointer scan.
type multiplexOutput struct {
	Tags              map[string]string       `json:"Tags"`
	Arn               string                  `json:"Arn"`
	ID                string                  `json:"Id"`
	Name              string                  `json:"Name"`
	State             string                  `json:"State"`
	AvailabilityZones []string                `json:"AvailabilityZones"`
	MultiplexSettings multiplexSettingsOutput `json:"MultiplexSettings"`
}

func toMultiplexOutput(m *Multiplex) multiplexOutput {
	tags := m.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	zones := m.AvailabilityZones
	if zones == nil {
		zones = []string{}
	}

	return multiplexOutput{
		Tags:              tags,
		AvailabilityZones: zones,
		Arn:               m.ARN,
		ID:                m.ID,
		Name:              m.Name,
		State:             m.State,
		MultiplexSettings: multiplexSettingsOutput{
			TransportStreamBitrate:              m.Settings.TransportStreamBitrate,
			TransportStreamID:                   m.Settings.TransportStreamID,
			TransportStreamReservedBitrate:      m.Settings.TransportStreamReservedBitrate,
			MaximumVideoBufferDelayMilliseconds: m.Settings.MaximumVideoBufferDelayMilliseconds,
		},
	}
}

func extractMultiplexSettings(body map[string]any) MultiplexSettings {
	raw, _ := body["MultiplexSettings"].(map[string]any)
	if raw == nil {
		return MultiplexSettings{}
	}

	return MultiplexSettings{
		TransportStreamBitrate:              intFromAny(raw["TransportStreamBitrate"]),
		TransportStreamID:                   intFromAny(raw["TransportStreamId"]),
		TransportStreamReservedBitrate:      intFromAny(raw["TransportStreamReservedBitrate"]),
		MaximumVideoBufferDelayMilliseconds: intFromAny(raw["MaximumVideoBufferDelayMilliseconds"]),
	}
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

func (h *Handler) handleCreateMultiplex(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	settings := extractMultiplexSettings(body)
	tags := extractTags(body)

	var zones []string
	if raw, ok := body["AvailabilityZones"].([]any); ok {
		for _, z := range raw {
			if s, isStr := z.(string); isStr {
				zones = append(zones, s)
			}
		}
	}

	m, err := h.Backend.CreateMultiplex(name, zones, settings, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"Multiplex": toMultiplexOutput(m)})
}

func (h *Handler) handleDescribeMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.DescribeMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleUpdateMultiplex(
	c *echo.Context,
	multiplexID string,
	body map[string]any,
) error {
	name, _ := body["Name"].(string)
	settings := extractMultiplexSettings(body)

	m, err := h.Backend.UpdateMultiplex(multiplexID, name, settings)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Multiplex": toMultiplexOutput(m)})
}

func (h *Handler) handleDeleteMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.DeleteMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleListMultiplexes(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListMultiplexes(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		zones := s.AvailabilityZones
		if zones == nil {
			zones = []string{}
		}

		out = append(out, map[string]any{
			keyArn:              s.ARN,
			keyID:               s.ID,
			keyName:             s.Name,
			keyState:            s.State,
			"AvailabilityZones": zones,
		})
	}

	resp := map[string]any{"Multiplexes": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.StartMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleStopMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.StopMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

// --- MultiplexProgram handlers ---

type serviceDescriptorOutput struct {
	ProviderName string `json:"ProviderName"`
	ServiceName  string `json:"ServiceName"`
}

type multiplexProgramSettingsOutput struct {
	ServiceDescriptor        serviceDescriptorOutput `json:"ServiceDescriptor"`
	PreferredChannelPipeline string                  `json:"PreferredChannelPipeline"`
	ProgramNumber            int                     `json:"ProgramNumber"`
}

// ProgramName and ChannelID first: reduces GC pointer scan.
type multiplexProgramOutput struct {
	ProgramName              string                         `json:"ProgramName"`
	ChannelID                string                         `json:"ChannelId"`
	MultiplexProgramSettings multiplexProgramSettingsOutput `json:"MultiplexProgramSettings"`
}

func toMultiplexProgramOutput(p *MultiplexProgram) multiplexProgramOutput {
	return multiplexProgramOutput{
		ProgramName: p.ProgramName,
		ChannelID:   p.ChannelID,
		MultiplexProgramSettings: multiplexProgramSettingsOutput{
			ProgramNumber:            p.Settings.ProgramNumber,
			PreferredChannelPipeline: p.Settings.PreferredChannelPipeline,
			ServiceDescriptor: serviceDescriptorOutput{
				ProviderName: p.Settings.ServiceDescriptor.ProviderName,
				ServiceName:  p.Settings.ServiceDescriptor.ServiceName,
			},
		},
	}
}

func extractMultiplexProgramSettings(body map[string]any) MultiplexProgramSettings {
	programName, _ := body["ProgramName"].(string)

	raw, _ := body["MultiplexProgramSettings"].(map[string]any)
	if raw == nil {
		return MultiplexProgramSettings{ProgramName: programName}
	}

	var sd ServiceDescriptor
	if sdRaw, ok := raw["ServiceDescriptor"].(map[string]any); ok {
		sd.ProviderName, _ = sdRaw["ProviderName"].(string)
		sd.ServiceName, _ = sdRaw["ServiceName"].(string)
	}

	preferred, _ := raw["PreferredChannelPipeline"].(string)

	return MultiplexProgramSettings{
		ProgramName:              programName,
		ProgramNumber:            intFromAny(raw["ProgramNumber"]),
		PreferredChannelPipeline: preferred,
		ServiceDescriptor:        sd,
	}
}

func (h *Handler) handleCreateMultiplexProgram(
	c *echo.Context,
	multiplexID string,
	body map[string]any,
) error {
	prog := extractMultiplexProgramSettings(body)

	p, err := h.Backend.CreateMultiplexProgram(multiplexID, prog)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(
		http.StatusCreated,
		map[string]any{"MultiplexProgram": toMultiplexProgramOutput(p)},
	)
}

func (h *Handler) handleDescribeMultiplexProgram(c *echo.Context, resource string) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	p, err := h.Backend.DescribeMultiplexProgram(multiplexID, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexProgramOutput(p))
}

func (h *Handler) handleUpdateMultiplexProgram(
	c *echo.Context,
	resource string,
	body map[string]any,
) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	prog := extractMultiplexProgramSettings(body)
	prog.ProgramName = programName

	p, err := h.Backend.UpdateMultiplexProgram(multiplexID, prog)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"MultiplexProgram": toMultiplexProgramOutput(p)})
}

func (h *Handler) handleDeleteMultiplexProgram(c *echo.Context, resource string) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	p, err := h.Backend.DeleteMultiplexProgram(multiplexID, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexProgramOutput(p))
}

func (h *Handler) handleListMultiplexPrograms(c *echo.Context, multiplexID string) error {
	summaries, nextToken, err := h.Backend.ListMultiplexPrograms(multiplexID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			"ProgramName": s.ProgramName,
			"ChannelId":   s.ChannelID,
		})
	}

	resp := map[string]any{"MultiplexPrograms": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
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

// --- InputDevice handlers ---

type inputDeviceOutput struct {
	Tags                    map[string]string `json:"Tags"`
	Arn                     string            `json:"Arn"`
	ID                      string            `json:"Id"`
	Name                    string            `json:"Name"`
	SerialNumber            string            `json:"SerialNumber"`
	MacAddress              string            `json:"MacAddress"`
	DeviceType              string            `json:"Type"`
	ConnectionState         string            `json:"ConnectionState"`
	DeviceSettingsSyncState string            `json:"DeviceSettingsSyncState"`
	DeviceUpdateStatus      string            `json:"DeviceUpdateStatus"`
}

func toInputDeviceOutput(d *InputDevice) inputDeviceOutput {
	tags := d.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return inputDeviceOutput{
		Tags:                    tags,
		Arn:                     d.ARN,
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

func (h *Handler) handleClaimDevice(c *echo.Context, body map[string]any) error {
	id, _ := body["Id"].(string)

	if _, err := h.Backend.ClaimDevice(id); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputDevices(c *echo.Context) error {
	devices, nextToken, err := h.Backend.ListInputDevices(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]inputDeviceOutput, 0, len(devices))
	for _, d := range devices {
		out = append(out, toInputDeviceOutput(d))
	}

	resp := map[string]any{"InputDevices": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeInputDevice(c *echo.Context, deviceID string) error {
	d, err := h.Backend.DescribeInputDevice(deviceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputDeviceOutput(d))
}

func (h *Handler) handleUpdateInputDevice(
	c *echo.Context,
	deviceID string,
	body map[string]any,
) error {
	name, _ := body["Name"].(string)

	d, err := h.Backend.UpdateInputDevice(deviceID, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputDeviceOutput(d))
}

func (h *Handler) handleRebootInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.RebootInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleTransferInputDevice(
	c *echo.Context,
	deviceID string,
	body map[string]any,
) error {
	targetCustomerID, _ := body["TargetCustomerId"].(string)
	targetRegion, _ := body["TargetRegion"].(string)
	message, _ := body["TransferMessage"].(string)

	if err := h.Backend.TransferInputDevice(deviceID, targetCustomerID, targetRegion, message); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleAcceptInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.AcceptInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCancelInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.CancelInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRejectInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.RejectInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputDeviceTransfers(c *echo.Context) error {
	transferType := c.QueryParam("transferType")

	transfers, nextToken, err := h.Backend.ListInputDeviceTransfers(transferType, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(transfers))
	for _, t := range transfers {
		out = append(out, map[string]any{
			keyID:              t.DeviceID,
			"TargetCustomerId": t.TargetCustomerID,
			"TransferType":     t.TransferType,
			"Message":          t.Message,
		})
	}

	resp := map[string]any{"InputDeviceTransfers": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Cluster path classification ---

// classifyClusterPath classifies paths under /prod/clusters.
// resource is one of:
//   - clusterID (single cluster ops)
//   - clusterID (for node-list/create ops, nodeID is embedded in resource)
//   - "clusterID/nodeID" (compound, for node CRUD)
func classifyClusterPath(method, path string) (string, string, bool) {
	if path == pathClusters {
		return classifyClusterRoot(method)
	}

	after, ok := strings.CutPrefix(path, pathClusters+"/")
	if !ok {
		return "", "", false
	}

	// Split into at most 4 parts: clusterId/subpath/nodeId/state
	parts := strings.SplitN(after, "/", pathSegmentsDeepSub)

	clusterID := parts[0]
	if clusterID == "" {
		return "", "", false
	}

	switch len(parts) {
	case pathSegmentsID:
		return classifyClusterIDOnly(method, clusterID)
	case pathSegmentsSub:
		return classifyClusterSubpath(method, clusterID, parts[1])
	case pathSegmentsNamed:
		return classifyClusterNodePath(method, clusterID, parts[1], parts[2])
	case pathSegmentsDeepSub:
		return classifyClusterNodeStatePath(method, clusterID, parts[1], parts[2], parts[3])
	}

	return "", "", false
}

func classifyClusterRoot(method string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opListClusters, "", true
	case http.MethodPost:
		return opCreateCluster, "", true
	}

	return "", "", false
}

func classifyClusterIDOnly(method, clusterID string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opDescribeCluster, clusterID, true
	case http.MethodPut:
		return opUpdateCluster, clusterID, true
	case http.MethodDelete:
		return opDeleteCluster, clusterID, true
	}

	return "", "", false
}

func classifyClusterSubpath(method, clusterID, sub string) (string, string, bool) {
	switch {
	case sub == subAlerts && method == http.MethodGet:
		return opListClusterAlerts, clusterID, true
	case sub == subNodeRegistrationScript && method == http.MethodPost:
		return opCreateNodeRegistrationScript, clusterID, true
	case sub == subNodes && method == http.MethodGet:
		return opListNodes, clusterID, true
	case sub == subNodes && method == http.MethodPost:
		return opCreateNode, clusterID, true
	case sub == subChannelPlacementGroups && method == http.MethodGet:
		return opListChannelPlacementGroups, clusterID, true
	case sub == subChannelPlacementGroups && method == http.MethodPost:
		return opCreateChannelPlacementGroup, clusterID, true
	}

	return "", "", false
}

// classifyClusterNodePath handles /prod/clusters/{id}/nodes/{nodeId}.
// resource is compound "clusterID/nodeID".
func classifyClusterNodePath(method, clusterID, sub, nodeID string) (string, string, bool) {
	if nodeID == "" {
		return "", "", false
	}

	compound := clusterID + "/" + nodeID

	if sub == subChannelPlacementGroups {
		switch method {
		case http.MethodGet:
			return opDescribeChannelPlacementGroup, compound, true
		case http.MethodPut:
			return opUpdateChannelPlacementGroup, compound, true
		case http.MethodDelete:
			return opDeleteChannelPlacementGroup, compound, true
		}

		return "", "", false
	}

	if sub != subNodes {
		return "", "", false
	}

	switch method {
	case http.MethodGet:
		return opDescribeNode, compound, true
	case http.MethodPut:
		return opUpdateNode, compound, true
	case http.MethodDelete:
		return opDeleteNode, compound, true
	}

	return "", "", false
}

// classifyClusterNodeStatePath handles /prod/clusters/{id}/nodes/{nodeId}/state.
func classifyClusterNodeStatePath(method, clusterID, sub, nodeID, extra string) (string, string, bool) {
	if sub != subNodes || nodeID == "" || extra != subState {
		return "", "", false
	}

	if method != http.MethodPut {
		return "", "", false
	}

	compound := clusterID + "/" + nodeID

	return opUpdateNodeState, compound, true
}

// splitClusterNode splits the compound resource "clusterID/nodeID".
func splitClusterNode(resource string) (string, string) {
	before, after, _ := strings.Cut(resource, "/")

	return before, after
}

// --- Cluster handlers ---

type clusterOutput struct {
	Tags            map[string]string `json:"Tags"`
	Arn             string            `json:"Arn"`
	ID              string            `json:"Id"`
	Name            string            `json:"Name"`
	ClusterType     string            `json:"ClusterType"`
	InstanceRoleArn string            `json:"InstanceRoleArn"`
	State           string            `json:"State"`
}

func toClusterOutput(c *Cluster) clusterOutput {
	tags := c.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return clusterOutput{
		Tags:            tags,
		Arn:             c.ARN,
		ID:              c.ID,
		Name:            c.Name,
		ClusterType:     c.ClusterType,
		InstanceRoleArn: c.InstanceRoleArn,
		State:           c.State,
	}
}

func (h *Handler) handleCreateCluster(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	clusterType, _ := body["ClusterType"].(string)
	instanceRoleArn, _ := body["InstanceRoleArn"].(string)
	tags := extractTags(body)

	cl, err := h.Backend.CreateCluster(name, clusterType, instanceRoleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toClusterOutput(cl))
}

func (h *Handler) handleDescribeCluster(c *echo.Context, clusterID string) error {
	cl, err := h.Backend.DescribeCluster(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleUpdateCluster(c *echo.Context, clusterID string, body map[string]any) error {
	name, _ := body["Name"].(string)

	cl, err := h.Backend.UpdateCluster(clusterID, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleDeleteCluster(c *echo.Context, clusterID string) error {
	cl, err := h.Backend.DeleteCluster(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListClusters(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:            s.ARN,
			keyID:             s.ID,
			keyName:           s.Name,
			keyState:          s.State,
			"ClusterType":     s.ClusterType,
			"InstanceRoleArn": s.InstanceRoleArn,
		})
	}

	resp := map[string]any{"Clusters": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListClusterAlerts(c *echo.Context, clusterID string) error {
	alerts, nextToken, err := h.Backend.ListClusterAlerts(clusterID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{keyAlerts: alerts}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateNodeRegistrationScript(c *echo.Context, clusterID string) error {
	script, err := h.Backend.CreateNodeRegistrationScript(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"NodeRegistrationScript": script})
}

// --- Node handlers ---

type nodeOutput struct {
	Tags            map[string]string `json:"Tags"`
	Arn             string            `json:"Arn"`
	ID              string            `json:"Id"`
	Name            string            `json:"Name"`
	ClusterID       string            `json:"ClusterId"`
	Role            string            `json:"Role"`
	State           string            `json:"State"`
	ConnectionState string            `json:"ConnectionState"`
}

func toNodeOutput(n *Node) nodeOutput {
	tags := n.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return nodeOutput{
		Tags:            tags,
		Arn:             n.ARN,
		ID:              n.ID,
		Name:            n.Name,
		ClusterID:       n.ClusterID,
		Role:            n.Role,
		State:           n.State,
		ConnectionState: n.ConnectionState,
	}
}

func (h *Handler) handleCreateNode(c *echo.Context, clusterID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	role, _ := body["Role"].(string)
	tags := extractTags(body)

	n, err := h.Backend.CreateNode(clusterID, name, role, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toNodeOutput(n))
}

func (h *Handler) handleDescribeNode(c *echo.Context, resource string) error {
	clusterID, nodeID := splitClusterNode(resource)

	n, err := h.Backend.DescribeNode(clusterID, nodeID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleUpdateNode(c *echo.Context, resource string, body map[string]any) error {
	clusterID, nodeID := splitClusterNode(resource)

	name, _ := body["Name"].(string)
	role, _ := body["Role"].(string)

	n, err := h.Backend.UpdateNode(clusterID, nodeID, name, role)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleUpdateNodeState(c *echo.Context, resource string, body map[string]any) error {
	clusterID, nodeID := splitClusterNode(resource)

	state, _ := body["State"].(string)

	n, err := h.Backend.UpdateNodeState(clusterID, nodeID, state)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleDeleteNode(c *echo.Context, resource string) error {
	clusterID, nodeID := splitClusterNode(resource)

	n, err := h.Backend.DeleteNode(clusterID, nodeID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleListNodes(c *echo.Context, clusterID string) error {
	summaries, nextToken, err := h.Backend.ListNodes(clusterID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:            s.ARN,
			keyID:             s.ID,
			keyName:           s.Name,
			keyState:          s.State,
			"ClusterId":       s.ClusterID,
			"Role":            s.Role,
			"ConnectionState": s.ConnectionState,
		})
	}

	resp := map[string]any{"Nodes": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Signal Map handlers ---

func toSignalMapOutput(sm *SignalMap) map[string]any {
	tags := sm.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	cwIDs := sm.CloudWatchAlarmTemplateGroupIDs
	if cwIDs == nil {
		cwIDs = []string{}
	}
	ebIDs := sm.EventBridgeRuleTemplateGroupIDs
	if ebIDs == nil {
		ebIDs = []string{}
	}

	return map[string]any{
		keyArn: sm.Arn, keyID: sm.ID, keyName: sm.Name,
		keyDescription: sm.Description, "DiscoveryEntryPointArn": sm.DiscoveryEntryPointArn,
		"Status": sm.Status, "MonitorDeploymentStatus": sm.MonitorDeploymentStatus,
		"CloudWatchAlarmTemplateGroupIds": cwIDs, "EventBridgeRuleTemplateGroupIds": ebIDs,
		keyTags: tags,
	}
}

func (h *Handler) handleCreateSignalMap(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	discoveryArn, _ := body["DiscoveryEntryPointArn"].(string)
	cwGroupIDs := extractStringSlice(body, "CloudWatchAlarmTemplateGroupIdentifiers")
	ebGroupIDs := extractStringSlice(body, "EventBridgeRuleTemplateGroupIdentifiers")
	tags := extractTags(body)
	sm, err := h.Backend.CreateSignalMap(
		name,
		description,
		discoveryArn,
		cwGroupIDs,
		ebGroupIDs,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toSignalMapOutput(sm))
}

func (h *Handler) handleGetSignalMap(c *echo.Context, identifier string) error {
	sm, err := h.Backend.GetSignalMap(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSignalMapOutput(sm))
}

func (h *Handler) handleListSignalMaps(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListSignalMaps(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, sm := range items {
		out = append(out, toSignalMapOutput(sm))
	}
	resp := map[string]any{"SignalMaps": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteSignalMap(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteSignalMap(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{})
}

func (h *Handler) handleStartUpdateSignalMap(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	cwGroupIDs := extractStringSlice(body, "CloudWatchAlarmTemplateGroupIdentifiers")
	ebGroupIDs := extractStringSlice(body, "EventBridgeRuleTemplateGroupIdentifiers")
	sm, err := h.Backend.StartUpdateSignalMap(identifier, name, description, cwGroupIDs, ebGroupIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}

func (h *Handler) handleStartMonitorDeployment(c *echo.Context, identifier string) error {
	sm, err := h.Backend.StartMonitorDeployment(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}

// --- CloudWatch Alarm Template Group handlers ---

func toCWAlarmTemplateGroupOutput(g *CloudWatchAlarmTemplateGroup) map[string]any {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: g.Arn, keyID: g.ID, keyName: g.Name, keyDescription: g.Description, keyTags: tags,
	}
}

func (h *Handler) handleCreateCWAlarmTemplateGroup(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	g, err := h.Backend.CreateCloudWatchAlarmTemplateGroup(name, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleGetCWAlarmTemplateGroup(c *echo.Context, identifier string) error {
	g, err := h.Backend.GetCloudWatchAlarmTemplateGroup(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleListCWAlarmTemplateGroups(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListCloudWatchAlarmTemplateGroups(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, g := range items {
		out = append(out, toCWAlarmTemplateGroupOutput(g))
	}
	resp := map[string]any{"CloudWatchAlarmTemplateGroups": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateCWAlarmTemplateGroup(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	g, err := h.Backend.UpdateCloudWatchAlarmTemplateGroup(identifier, name, description)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleDeleteCWAlarmTemplateGroup(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteCloudWatchAlarmTemplateGroup(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- CloudWatch Alarm Template handlers ---

func toCWAlarmTemplateOutput(t *CloudWatchAlarmTemplate) map[string]any {
	tags := t.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: t.Arn, keyID: t.ID, keyName: t.Name, keyDescription: t.Description,
		"GroupId": t.GroupID, "GroupIdentifier": t.GroupIdentifier,
		"MetricName": t.MetricName, "Namespace": t.Namespace,
		"Statistic": t.Statistic, "ComparisonOperator": t.ComparisonOperator,
		"TargetResourceType": t.TargetResourceType, "TreatMissingData": t.TreatMissingData,
		"Threshold": t.Threshold, "EvaluationPeriods": t.EvaluationPeriods,
		"DatapointsToAlarm": t.DatapointsToAlarm, "Period": t.Period,
		keyTags: tags,
	}
}

func extractCWAlarmTemplateFields(
	body map[string]any,
) (string, string, string, string, string, string, string, float64, int32, int32, int32) {
	groupIdentifier, _ := body["GroupIdentifier"].(string)
	metricName, _ := body["MetricName"].(string)
	namespace, _ := body["Namespace"].(string)
	statistic, _ := body["Statistic"].(string)
	comparisonOperator, _ := body["ComparisonOperator"].(string)
	targetResourceType, _ := body["TargetResourceType"].(string)
	treatMissingData, _ := body["TreatMissingData"].(string)
	var threshold float64
	if v, ok := body["Threshold"].(float64); ok {
		threshold = v
	}
	var evalPeriods int32
	if v, ok := body["EvaluationPeriods"].(float64); ok {
		evalPeriods = int32(v)
	}
	var datapointsToAlarm int32
	if v, ok := body["DatapointsToAlarm"].(float64); ok {
		datapointsToAlarm = int32(v)
	}
	var period int32
	if v, ok := body["Period"].(float64); ok {
		period = int32(v)
	}

	return groupIdentifier, metricName, namespace, statistic, comparisonOperator,
		targetResourceType, treatMissingData, threshold, evalPeriods, datapointsToAlarm, period
}

func (h *Handler) handleCreateCWAlarmTemplate(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	groupID, metricName, namespace, statistic, compOp,
		targetType, treatMissing, threshold,
		evalPeriods, datapointsToAlarm, period :=
		extractCWAlarmTemplateFields(body)
	t, err := h.Backend.CreateCloudWatchAlarmTemplate(
		name,
		description,
		groupID,
		metricName,
		namespace,
		statistic,
		compOp,
		targetType,
		treatMissing,
		threshold,
		evalPeriods,
		datapointsToAlarm,
		period,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleGetCWAlarmTemplate(c *echo.Context, identifier string) error {
	t, err := h.Backend.GetCloudWatchAlarmTemplate(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleListCWAlarmTemplates(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListCloudWatchAlarmTemplates(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, toCWAlarmTemplateOutput(t))
	}
	resp := map[string]any{"CloudWatchAlarmTemplates": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateCWAlarmTemplate(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	groupID, metricName, namespace, statistic, compOp,
		targetType, treatMissing, threshold,
		evalPeriods, datapointsToAlarm, period :=
		extractCWAlarmTemplateFields(body)
	t, err := h.Backend.UpdateCloudWatchAlarmTemplate(
		identifier,
		name,
		description,
		groupID,
		metricName,
		namespace,
		statistic,
		compOp,
		targetType,
		treatMissing,
		threshold,
		evalPeriods,
		datapointsToAlarm,
		period,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleDeleteCWAlarmTemplate(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteCloudWatchAlarmTemplate(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- EventBridge Rule Template Group handlers ---

func toEBRuleTemplateGroupOutput(g *EventBridgeRuleTemplateGroup) map[string]any {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: g.Arn, keyID: g.ID, keyName: g.Name, keyDescription: g.Description, keyTags: tags,
	}
}

func (h *Handler) handleCreateEBRuleTemplateGroup(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	g, err := h.Backend.CreateEventBridgeRuleTemplateGroup(name, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleGetEBRuleTemplateGroup(c *echo.Context, identifier string) error {
	g, err := h.Backend.GetEventBridgeRuleTemplateGroup(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleListEBRuleTemplateGroups(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListEventBridgeRuleTemplateGroups(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, g := range items {
		out = append(out, toEBRuleTemplateGroupOutput(g))
	}
	resp := map[string]any{"EventBridgeRuleTemplateGroups": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEBRuleTemplateGroup(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	g, err := h.Backend.UpdateEventBridgeRuleTemplateGroup(identifier, name, description)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleDeleteEBRuleTemplateGroup(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteEventBridgeRuleTemplateGroup(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- EventBridge Rule Template handlers ---

func toEBRuleTemplateOutput(t *EventBridgeRuleTemplate) map[string]any {
	tags := t.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	targets := make([]map[string]any, 0, len(t.EventTargets))
	for _, tgt := range t.EventTargets {
		targets = append(targets, map[string]any{keyArn: tgt.Arn})
	}

	return map[string]any{
		keyArn: t.Arn, keyID: t.ID, keyName: t.Name, keyDescription: t.Description,
		"GroupId": t.GroupID, "GroupIdentifier": t.GroupIdentifier,
		"EventType": t.EventType, "EventTargets": targets, keyTags: tags,
	}
}

func extractEBTargets(body map[string]any) []EventBridgeRuleTemplateTarget {
	raw, _ := body["EventTargets"].([]any)
	targets := make([]EventBridgeRuleTemplateTarget, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		arnVal, _ := m[keyArn].(string)
		if arnVal != "" {
			targets = append(targets, EventBridgeRuleTemplateTarget{Arn: arnVal})
		}
	}

	return targets
}

func (h *Handler) handleCreateEBRuleTemplate(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	groupIdentifier, _ := body["GroupIdentifier"].(string)
	eventType, _ := body["EventType"].(string)
	targets := extractEBTargets(body)
	tags := extractTags(body)
	t, err := h.Backend.CreateEventBridgeRuleTemplate(
		name,
		description,
		groupIdentifier,
		eventType,
		targets,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleGetEBRuleTemplate(c *echo.Context, identifier string) error {
	t, err := h.Backend.GetEventBridgeRuleTemplate(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleListEBRuleTemplates(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListEventBridgeRuleTemplates(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, toEBRuleTemplateOutput(t))
	}
	resp := map[string]any{"EventBridgeRuleTemplates": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEBRuleTemplate(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)
	groupIdentifier, _ := body["GroupIdentifier"].(string)
	eventType, _ := body["EventType"].(string)
	targets := extractEBTargets(body)
	t, err := h.Backend.UpdateEventBridgeRuleTemplate(
		identifier,
		name,
		description,
		groupIdentifier,
		eventType,
		targets,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleDeleteEBRuleTemplate(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteEventBridgeRuleTemplate(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Offering handlers ---

func toOfferingOutput(o *Offering) map[string]any {
	return map[string]any{
		keyArn: o.Arn, "OfferingId": o.OfferingID,
		"OfferingDescription": o.OfferingDescription, "OfferingType": o.OfferingType,
		"CurrencyCode": o.CurrencyCode, "FixedPrice": o.FixedPrice, "UsagePrice": o.UsagePrice,
		"Duration": o.Duration, "DurationUnits": o.DurationUnits,
		"ResourceSpecification": map[string]any{
			"ResourceType":     o.ResourceSpecification.ResourceType,
			"VideoQuality":     o.ResourceSpecification.VideoQuality,
			"Resolution":       o.ResourceSpecification.Resolution,
			"MaximumBitrate":   o.ResourceSpecification.MaximumBitrate,
			"MaximumFramerate": o.ResourceSpecification.MaximumFramerate,
			"Codec":            o.ResourceSpecification.Codec,
		},
	}
}

func (h *Handler) handleListOfferings(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListOfferings(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, o := range items {
		out = append(out, toOfferingOutput(o))
	}
	resp := map[string]any{"Offerings": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeOffering(c *echo.Context, offeringID string) error {
	o, err := h.Backend.DescribeOffering(offeringID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toOfferingOutput(o))
}

func (h *Handler) handlePurchaseOffering(
	c *echo.Context,
	offeringID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	var count int32 = 1
	if v, ok := body["Count"].(float64); ok {
		count = int32(v)
	}
	tags := extractTags(body)
	r, err := h.Backend.PurchaseOffering(offeringID, name, count, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"Reservation": toReservationOutput(r)})
}

// --- Reservation handlers ---

func toReservationOutput(r *Reservation) map[string]any {
	tags := r.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: r.Arn, "ReservationId": r.ReservationID, keyName: r.Name,
		"OfferingId": r.OfferingID, "OfferingDescription": r.OfferingDescription,
		"OfferingType": r.OfferingType, "CurrencyCode": r.CurrencyCode,
		"FixedPrice": r.FixedPrice, "UsagePrice": r.UsagePrice,
		"Duration": r.Duration, "DurationUnits": r.DurationUnits,
		"Start": r.Start, "End": r.End, "Region": r.Region, keyState: r.State,
		"Count": r.Count,
		"ResourceSpecification": map[string]any{
			"ResourceType":     r.ResourceSpecification.ResourceType,
			"VideoQuality":     r.ResourceSpecification.VideoQuality,
			"Resolution":       r.ResourceSpecification.Resolution,
			"MaximumBitrate":   r.ResourceSpecification.MaximumBitrate,
			"MaximumFramerate": r.ResourceSpecification.MaximumFramerate,
			"Codec":            r.ResourceSpecification.Codec,
		},
		keyTags: tags,
	}
}

func (h *Handler) handleListReservations(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListReservations(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, r := range items {
		out = append(out, toReservationOutput(r))
	}
	resp := map[string]any{"Reservations": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeReservation(c *echo.Context, reservationID string) error {
	r, err := h.Backend.DescribeReservation(reservationID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toReservationOutput(r))
}

func (h *Handler) handleDeleteReservation(c *echo.Context, reservationID string) error {
	r, err := h.Backend.DeleteReservation(reservationID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toReservationOutput(r))
}

func (h *Handler) handleUpdateReservation(
	c *echo.Context,
	reservationID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	r, err := h.Backend.UpdateReservation(reservationID, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toReservationOutput(r))
}

// --- Batch handlers ---

func toBatchResultOutput(result *BatchResult) map[string]any {
	successful := make([]map[string]any, 0, len(result.Successful))
	for _, s := range result.Successful {
		successful = append(
			successful,
			map[string]any{keyArn: s.Arn, keyID: s.ID, keyState: s.State},
		)
	}
	failed := make([]map[string]any, 0, len(result.Failed))
	for _, f := range result.Failed {
		failed = append(failed, map[string]any{keyArn: f.Arn, keyID: f.ID, "Code": f.Code})
	}

	return map[string]any{"Successful": successful, "Failed": failed}
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

func (h *Handler) handleBatchStart(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "ChannelIds")
	inputIDs := extractStringSlice(body, "InputIds")
	multiplexIDs := extractStringSlice(body, "MultiplexIds")
	result, err := h.Backend.BatchStart(channelIDs, inputIDs, multiplexIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}

func (h *Handler) handleBatchStop(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "ChannelIds")
	inputIDs := extractStringSlice(body, "InputIds")
	multiplexIDs := extractStringSlice(body, "MultiplexIds")
	result, err := h.Backend.BatchStop(channelIDs, inputIDs, multiplexIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}

func (h *Handler) handleBatchDelete(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "ChannelIds")
	inputIDs := extractStringSlice(body, "InputIds")
	multiplexIDs := extractStringSlice(body, "MultiplexIds")
	result, err := h.Backend.BatchDelete(channelIDs, inputIDs, multiplexIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}

func (h *Handler) handleBatchUpdateSchedule(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	var creates []ScheduleAction
	if rawCreates, ok := body["Creates"].(map[string]any); ok {
		rawActions, hasActions := rawCreates["ScheduleActions"].([]any)
		if hasActions {
			for _, item := range rawActions {
				m, isMapped := item.(map[string]any)
				if !isMapped {
					continue
				}
				actionName, _ := m["ActionName"].(string)
				creates = append(creates, ScheduleAction{ActionName: actionName})
			}
		}
	}
	var deleteNames []string
	if rawDeletes, ok := body["Deletes"].(map[string]any); ok {
		deleteNames = extractStringSlice(rawDeletes, "ActionNames")
	}
	result, err := h.Backend.BatchUpdateSchedule(channelID, creates, deleteNames)
	if err != nil {
		return respondErr(c, err)
	}
	createsOut := make([]map[string]any, 0, len(result.Creates))
	for _, a := range result.Creates {
		createsOut = append(createsOut, map[string]any{keyActionName: a.ActionName})
	}
	deletesOut := make([]map[string]any, 0, len(result.Deletes))
	for _, a := range result.Deletes {
		deletesOut = append(deletesOut, map[string]any{keyActionName: a.ActionName})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Creates": map[string]any{keyScheduleActions: createsOut},
		"Deletes": map[string]any{keyScheduleActions: deletesOut},
	})
}

// --- Network handlers ---

func toNetworkOutput(n *Network) map[string]any {
	tags := n.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	clusters := n.AssociatedClusterIDs
	if clusters == nil {
		clusters = []string{}
	}
	pools := n.IPPools
	if pools == nil {
		pools = []IPPool{}
	}
	routes := n.Routes
	if routes == nil {
		routes = []Route{}
	}

	return map[string]any{
		keyArn: n.ARN, keyID: n.ID, keyName: n.Name, keyState: n.State,
		"AssociatedClusterIds": clusters, "IpPools": pools, "Routes": routes,
		keyTags: tags,
	}
}

func extractIPPools(body map[string]any) []IPPool {
	raw, _ := body["IpPools"].([]any)
	if raw == nil {
		return nil
	}

	pools := make([]IPPool, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cidr, _ := m["Cidr"].(string)
		pools = append(pools, IPPool{Cidr: cidr})
	}

	return pools
}

func extractRoutes(body map[string]any) []Route {
	raw, _ := body["Routes"].([]any)
	if raw == nil {
		return nil
	}

	routes := make([]Route, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cidr, _ := m["Cidr"].(string)
		gateway, _ := m["Gateway"].(string)
		routes = append(routes, Route{Cidr: cidr, Gateway: gateway})
	}

	return routes
}

func (h *Handler) handleCreateNetwork(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	tags := extractTags(body)

	n, err := h.Backend.CreateNetwork(name, extractIPPools(body), extractRoutes(body), tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toNetworkOutput(n))
}

func (h *Handler) handleDescribeNetwork(c *echo.Context, networkID string) error {
	n, err := h.Backend.DescribeNetwork(networkID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleUpdateNetwork(
	c *echo.Context,
	networkID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)

	n, err := h.Backend.UpdateNetwork(networkID, name, extractIPPools(body), extractRoutes(body))
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleDeleteNetwork(c *echo.Context, networkID string) error {
	n, err := h.Backend.DeleteNetwork(networkID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleListNetworks(c *echo.Context) error {
	nets, nextToken, err := h.Backend.ListNetworks(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(nets))
	for _, n := range nets {
		out = append(out, toNetworkOutput(n))
	}

	resp := map[string]any{"Networks": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- SdiSource handlers ---

func toSdiSourceOutput(s *SdiSource) map[string]any {
	inputs := s.Inputs
	if inputs == nil {
		inputs = []string{}
	}

	return map[string]any{
		keyArn: s.ARN, keyID: s.ID, keyName: s.Name,
		"Type": s.Type, "Mode": s.Mode, keyState: s.State, "Inputs": inputs,
	}
}

func (h *Handler) handleCreateSdiSource(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	sdiType, _ := body["Type"].(string)
	mode, _ := body["Mode"].(string)
	tags := extractTags(body)

	s, err := h.Backend.CreateSdiSource(name, sdiType, mode, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleDescribeSdiSource(c *echo.Context, sdiSourceID string) error {
	s, err := h.Backend.DescribeSdiSource(sdiSourceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleUpdateSdiSource(
	c *echo.Context,
	sdiSourceID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	sdiType, _ := body["Type"].(string)
	mode, _ := body["Mode"].(string)

	s, err := h.Backend.UpdateSdiSource(sdiSourceID, name, sdiType, mode)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleDeleteSdiSource(c *echo.Context, sdiSourceID string) error {
	s, err := h.Backend.DeleteSdiSource(sdiSourceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleListSdiSources(c *echo.Context) error {
	sources, nextToken, err := h.Backend.ListSdiSources(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(sources))
	for _, s := range sources {
		out = append(out, toSdiSourceOutput(s))
	}

	resp := map[string]any{"SdiSources": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- ChannelPlacementGroup handlers ---

func toChannelPlacementGroupOutput(g *ChannelPlacementGroup) map[string]any {
	channels := g.Channels
	if channels == nil {
		channels = []string{}
	}
	nodes := g.Nodes
	if nodes == nil {
		nodes = []string{}
	}

	return map[string]any{
		keyArn: g.ARN, keyID: g.ID, keyName: g.Name, "ClusterId": g.ClusterID,
		keyState: g.State, "Channels": channels, "Nodes": nodes,
	}
}

func (h *Handler) handleCreateChannelPlacementGroup(
	c *echo.Context,
	clusterID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	nodes := extractStringSlice(body, "Nodes")
	tags := extractTags(body)

	g, err := h.Backend.CreateChannelPlacementGroup(clusterID, name, nodes, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleDescribeChannelPlacementGroup(c *echo.Context, resource string) error {
	clusterID, groupID := splitClusterNode(resource)

	g, err := h.Backend.DescribeChannelPlacementGroup(clusterID, groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleUpdateChannelPlacementGroup(
	c *echo.Context,
	resource string,
	body map[string]any,
) error {
	clusterID, groupID := splitClusterNode(resource)
	name, _ := body[keyName].(string)
	nodes := extractStringSlice(body, "Nodes")

	g, err := h.Backend.UpdateChannelPlacementGroup(clusterID, groupID, name, nodes)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleDeleteChannelPlacementGroup(c *echo.Context, resource string) error {
	clusterID, groupID := splitClusterNode(resource)

	g, err := h.Backend.DeleteChannelPlacementGroup(clusterID, groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleListChannelPlacementGroups(c *echo.Context, clusterID string) error {
	groups, nextToken, err := h.Backend.ListChannelPlacementGroups(clusterID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, toChannelPlacementGroupOutput(g))
	}

	resp := map[string]any{"ChannelPlacementGroups": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Account configuration handlers ---

func (h *Handler) handleDescribeAccountConfiguration(c *echo.Context) error {
	cfg, err := h.Backend.DescribeAccountConfiguration()
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AccountConfiguration": map[string]any{"KmsKeyId": cfg.KmsKeyID},
	})
}

func (h *Handler) handleUpdateAccountConfiguration(c *echo.Context, body map[string]any) error {
	kmsKeyID := ""
	if cfg, ok := body["AccountConfiguration"].(map[string]any); ok {
		kmsKeyID, _ = cfg["KmsKeyId"].(string)
	}

	cfg, err := h.Backend.UpdateAccountConfiguration(kmsKeyID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AccountConfiguration": map[string]any{"KmsKeyId": cfg.KmsKeyID},
	})
}

// --- Schedule handlers ---

func (h *Handler) handleDescribeSchedule(c *echo.Context, channelID string) error {
	actions, err := h.Backend.DescribeSchedule(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(actions))
	for _, a := range actions {
		out = append(out, map[string]any{keyActionName: a.ActionName})
	}

	return c.JSON(http.StatusOK, map[string]any{keyScheduleActions: out})
}

func (h *Handler) handleDeleteSchedule(c *echo.Context, channelID string) error {
	if err := h.Backend.DeleteSchedule(channelID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Alert and version handlers ---

func (h *Handler) handleListAlerts(c *echo.Context, channelID string) error {
	alerts, err := h.Backend.ListAlerts(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAlerts: alerts})
}

func (h *Handler) handleListMultiplexAlerts(c *echo.Context, multiplexID string) error {
	alerts, err := h.Backend.ListMultiplexAlerts(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAlerts: alerts})
}

func (h *Handler) handleListVersions(c *echo.Context) error {
	versions := h.Backend.ListVersions()

	out := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		out = append(out, map[string]any{"Version": v.Version, "ExpirationDate": v.ExpirationDate})
	}

	return c.JSON(http.StatusOK, map[string]any{"Versions": out})
}

// --- Channel lifecycle extra handlers ---

func (h *Handler) handleUpdateChannelClass(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	channelClass, _ := body["ChannelClass"].(string)

	ch, err := h.Backend.UpdateChannelClass(channelID, channelClass)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleRestartChannelPipelines(c *echo.Context, channelID string) error {
	pipelineIDs := []string{}

	ch, err := h.Backend.RestartChannelPipelines(channelID, pipelineIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDescribeThumbnails(c *echo.Context, channelID string) error {
	if _, err := h.Backend.DescribeThumbnails(channelID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ThumbnailDetails": []map[string]any{}})
}

// --- InputDevice lifecycle extra handlers ---

func (h *Handler) handleStartInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.StartInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.StopInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartInputDeviceMaintenanceWindow(c *echo.Context, deviceID string) error {
	if err := h.Backend.StartInputDeviceMaintenanceWindow(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeInputDeviceThumbnail(c *echo.Context, deviceID string) error {
	if _, err := h.Backend.DescribeInputDeviceThumbnail(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ContentType": "image/jpeg", "ContentLength": 0})
}

// --- SignalMap monitor deployment teardown handler ---

func (h *Handler) handleStartDeleteMonitorDeployment(c *echo.Context, identifier string) error {
	sm, err := h.Backend.StartDeleteMonitorDeployment(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}

// --- Partner input handler ---

func (h *Handler) handleCreatePartnerInput(
	c *echo.Context,
	inputID string,
	body map[string]any,
) error {
	tags := extractTags(body)

	inp, err := h.Backend.CreatePartnerInput(inputID, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyInput: toInputOutput(inp)})
}
