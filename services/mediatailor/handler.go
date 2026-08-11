package mediatailor

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority      = service.PriorityPathVersioned
	mediaTailorService = "mediatailor"

	pathChannels         = "/channels"
	pathChannel          = "/channel/"
	pathPlaybackConfig   = "/playbackConfiguration"
	pathPlaybackConfigs  = "/playbackConfigurations"
	pathSourceLocations  = "/sourceLocations"
	pathSourceLocation   = "/sourceLocation/"
	pathTags             = "/tags/"
	pathPrefetchSchedule = "/prefetchSchedule/"
	pathFunction         = "/function/"
	pathFunctions        = "/functions"
	pathAlerts           = "/alerts"
	pathConfigureLogs    = "/configureLogs/"

	// sigV4Service is the SigV4 signing name MediaTailor SDK clients use. The
	// bare "/channels" path is shared with MediaPackage and IoT Analytics, so we
	// disambiguate it by the request's SigV4 service name.
	sigV4Service = "mediatailor"

	keyMessage = "Message"
	// amznErrorTypeHeader carries the modeled exception type name for the
	// restjson1 protocol -- see errType's doc comment.
	amznErrorTypeHeader = "X-Amzn-Errortype"
	// keyTags is the wire name for the Tags map on every MediaTailor operation.
	// Unlike every other field (which is PascalCase), the real MediaTailor
	// restjson1 model gives this member a "tags" (lowercase) locationName —
	// confirmed against aws-sdk-go-v2/service/mediatailor's (de)serializers
	// and botocore's service-2.json. Sending/expecting "Tags" here silently
	// drops tags to/from a real SDK client.
	keyTags               = "tags"
	keyItems              = "Items"
	keyArn                = "Arn"
	keySourceLocationName = "SourceLocationName"
	keyName               = "Name"
	keyChannelName        = "ChannelName"
	keySourceGroup        = "SourceGroup"
	keyVodSourceName      = "VodSourceName"
	keyBaseURL            = "BaseUrl"
	keyLiveSourceName     = "LiveSourceName"

	splitTwo   = 2
	splitThree = 3

	opUnknown = "Unknown"

	opPutPlaybackConfiguration    = "PutPlaybackConfiguration"
	opGetPlaybackConfiguration    = "GetPlaybackConfiguration"
	opDeletePlaybackConfiguration = "DeletePlaybackConfiguration"
	opListPlaybackConfigurations  = "ListPlaybackConfigurations"

	opCreateChannel   = "CreateChannel"
	opDescribeChannel = "DescribeChannel"
	opUpdateChannel   = "UpdateChannel"
	opDeleteChannel   = "DeleteChannel"
	opListChannels    = "ListChannels"
	opStartChannel    = "StartChannel"
	opStopChannel     = "StopChannel"

	opCreateSourceLocation   = "CreateSourceLocation"
	opDescribeSourceLocation = "DescribeSourceLocation"
	opUpdateSourceLocation   = "UpdateSourceLocation"
	opDeleteSourceLocation   = "DeleteSourceLocation"
	opListSourceLocations    = "ListSourceLocations"

	opCreateVodSource   = "CreateVodSource"
	opDescribeVodSource = "DescribeVodSource"
	opUpdateVodSource   = "UpdateVodSource"
	opDeleteVodSource   = "DeleteVodSource"
	opListVodSources    = "ListVodSources"

	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"

	opCreateLiveSource   = "CreateLiveSource"
	opDescribeLiveSource = "DescribeLiveSource"
	opUpdateLiveSource   = "UpdateLiveSource"
	opDeleteLiveSource   = "DeleteLiveSource"
	opListLiveSources    = "ListLiveSources"

	opCreatePrefetchSchedule = "CreatePrefetchSchedule"
	opGetPrefetchSchedule    = "GetPrefetchSchedule"
	opDeletePrefetchSchedule = "DeletePrefetchSchedule"
	opListPrefetchSchedules  = "ListPrefetchSchedules"

	opCreateProgram      = "CreateProgram"
	opDescribeProgram    = "DescribeProgram"
	opUpdateProgram      = "UpdateProgram"
	opDeleteProgram      = "DeleteProgram"
	opGetChannelSchedule = "GetChannelSchedule"

	opPutChannelPolicy    = "PutChannelPolicy"
	opGetChannelPolicy    = "GetChannelPolicy"
	opDeleteChannelPolicy = "DeleteChannelPolicy"

	opPutFunction    = "PutFunction"
	opGetFunction    = "GetFunction"
	opDeleteFunction = "DeleteFunction"
	opListFunctions  = "ListFunctions"

	opListAlerts = "ListAlerts"

	opConfigureLogsForChannel               = "ConfigureLogsForChannel"
	opConfigureLogsForPlaybackConfiguration = "ConfigureLogsForPlaybackConfiguration"
)

// Handler handles MediaTailor HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaTailor" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opPutPlaybackConfiguration,
		opGetPlaybackConfiguration,
		opDeletePlaybackConfiguration,
		opListPlaybackConfigurations,
		opCreateChannel,
		opDescribeChannel,
		opUpdateChannel,
		opDeleteChannel,
		opListChannels,
		opStartChannel,
		opStopChannel,
		opCreateSourceLocation,
		opDescribeSourceLocation,
		opUpdateSourceLocation,
		opDeleteSourceLocation,
		opListSourceLocations,
		opCreateVodSource,
		opDescribeVodSource,
		opUpdateVodSource,
		opDeleteVodSource,
		opListVodSources,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		opCreateLiveSource,
		opDescribeLiveSource,
		opUpdateLiveSource,
		opDeleteLiveSource,
		opListLiveSources,
		opCreatePrefetchSchedule,
		opGetPrefetchSchedule,
		opDeletePrefetchSchedule,
		opListPrefetchSchedules,
		opCreateProgram,
		opDescribeProgram,
		opUpdateProgram,
		opDeleteProgram,
		opGetChannelSchedule,
		opPutChannelPolicy,
		opGetChannelPolicy,
		opDeleteChannelPolicy,
		opPutFunction,
		opGetFunction,
		opDeleteFunction,
		opListFunctions,
		opListAlerts,
		opConfigureLogsForChannel,
		opConfigureLogsForPlaybackConfiguration,
	}
}

// RouteMatcher returns a function that matches MediaTailor requests by path and
// Authorization header. MediaTailor shares /channels paths with MediaPackage and
// IoTAnalytics, so we distinguish by the service name in the AWS Signature header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if !isMediaTailorPath(path) {
			return false
		}

		// The bare "/channels" path is shared with MediaPackage and IoT Analytics,
		// which register matchers at the same priority. Claim it only for
		// SigV4-signed mediatailor requests so routing is deterministic regardless
		// of service registration order.
		if path == pathChannels {
			return httputils.ExtractServiceFromRequest(c.Request()) == sigV4Service
		}

		svc := httputils.ExtractServiceFromRequest(c.Request())

		return svc == "" || svc == mediaTailorService
	}
}

func isMediaTailorPath(path string) bool {
	return path == pathPlaybackConfig ||
		strings.HasPrefix(path, pathPlaybackConfig+"/") ||
		path == pathPlaybackConfigs ||
		path == pathChannels ||
		strings.HasPrefix(path, pathChannel) ||
		path == pathSourceLocations ||
		strings.HasPrefix(path, pathSourceLocation) ||
		strings.HasPrefix(path, pathPrefetchSchedule) ||
		path == pathFunctions ||
		strings.HasPrefix(path, pathFunction) ||
		path == pathAlerts ||
		strings.HasPrefix(path, pathConfigureLogs) ||
		isMediaTailorTagPath(path)
}

// isMediaTailorTagPath reports whether path is a /tags/{arn} path for a MediaTailor ARN.
// Other services (e.g. FIS) also expose /tags/{arn} at the same path prefix; we must not
// steal their requests. MediaTailor ARNs always contain ":mediatailor:".
func isMediaTailorTagPath(path string) bool {
	arn, ok := strings.CutPrefix(path, pathTags)

	return ok && strings.Contains(arn, ":mediatailor:")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	op, resource, extra := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "invalid JSON body"})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	handlers := map[string]func() error{
		opPutPlaybackConfiguration:    func() error { return h.handlePutPlaybackConfiguration(c, body) },
		opGetPlaybackConfiguration:    func() error { return h.handleGetPlaybackConfiguration(c, resource) },
		opDeletePlaybackConfiguration: func() error { return h.handleDeletePlaybackConfiguration(c, resource) },
		opListPlaybackConfigurations:  func() error { return h.handleListPlaybackConfigurations(c) },

		opCreateChannel:   func() error { return h.handleCreateChannel(c, resource, body) },
		opDescribeChannel: func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:   func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:   func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:    func() error { return h.handleListChannels(c) },
		opStartChannel:    func() error { return h.handleStartChannel(c, resource) },
		opStopChannel:     func() error { return h.handleStopChannel(c, resource) },

		opCreateSourceLocation:   func() error { return h.handleCreateSourceLocation(c, resource, body) },
		opDescribeSourceLocation: func() error { return h.handleDescribeSourceLocation(c, resource) },
		opUpdateSourceLocation:   func() error { return h.handleUpdateSourceLocation(c, resource, body) },
		opDeleteSourceLocation:   func() error { return h.handleDeleteSourceLocation(c, resource) },
		opListSourceLocations:    func() error { return h.handleListSourceLocations(c) },

		opCreateVodSource:   func() error { return h.handleCreateVodSource(c, resource, extra, body) },
		opDescribeVodSource: func() error { return h.handleDescribeVodSource(c, resource, extra) },
		opUpdateVodSource:   func() error { return h.handleUpdateVodSource(c, resource, extra, body) },
		opDeleteVodSource:   func() error { return h.handleDeleteVodSource(c, resource, extra) },
		opListVodSources:    func() error { return h.handleListVodSources(c, resource) },

		opListTagsForResource: func() error { return h.handleListTagsForResource(c, resource) },
		opTagResource:         func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:       func() error { return h.handleUntagResource(c, resource) },

		opCreateLiveSource:   func() error { return h.handleCreateLiveSource(c, resource, extra, body) },
		opDescribeLiveSource: func() error { return h.handleDescribeLiveSource(c, resource, extra) },
		opUpdateLiveSource:   func() error { return h.handleUpdateLiveSource(c, resource, extra, body) },
		opDeleteLiveSource:   func() error { return h.handleDeleteLiveSource(c, resource, extra) },
		opListLiveSources:    func() error { return h.handleListLiveSources(c, resource) },

		opCreatePrefetchSchedule: func() error { return h.handleCreatePrefetchSchedule(c, resource, extra, body) },
		opGetPrefetchSchedule:    func() error { return h.handleGetPrefetchSchedule(c, resource, extra) },
		opDeletePrefetchSchedule: func() error { return h.handleDeletePrefetchSchedule(c, resource, extra) },
		opListPrefetchSchedules:  func() error { return h.handleListPrefetchSchedules(c, resource, body) },

		opCreateProgram:      func() error { return h.handleCreateProgram(c, resource, extra, body) },
		opDescribeProgram:    func() error { return h.handleDescribeProgram(c, resource, extra) },
		opUpdateProgram:      func() error { return h.handleUpdateProgram(c, resource, extra, body) },
		opDeleteProgram:      func() error { return h.handleDeleteProgram(c, resource, extra) },
		opGetChannelSchedule: func() error { return h.handleGetChannelSchedule(c, resource) },

		opPutChannelPolicy:    func() error { return h.handlePutChannelPolicy(c, resource, body) },
		opGetChannelPolicy:    func() error { return h.handleGetChannelPolicy(c, resource) },
		opDeleteChannelPolicy: func() error { return h.handleDeleteChannelPolicy(c, resource) },

		opPutFunction:    func() error { return h.handlePutFunction(c, resource, body) },
		opGetFunction:    func() error { return h.handleGetFunction(c, resource) },
		opDeleteFunction: func() error { return h.handleDeleteFunction(c, resource) },
		opListFunctions:  func() error { return h.handleListFunctions(c) },

		opListAlerts: func() error { return h.handleListAlerts(c) },

		opConfigureLogsForChannel: func() error { return h.handleConfigureLogsForChannel(c, body) },
		opConfigureLogsForPlaybackConfiguration: func() error {
			return h.handleConfigureLogsForPlaybackConfiguration(c, body)
		},
	}

	if fn, ok := handlers[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
}

// classifyPath maps (method, path) → (operation, resource, extra).
// resource is the primary resource name; extra is a secondary name (e.g., vodSourceName).
func classifyPath(method, path string) (string, string, string) {
	if op, res, ok := classifyPlaybackConfigPath(method, path); ok {
		return op, res, ""
	}

	if op, res, extra, ok := classifyChannelPath(method, path); ok {
		return op, res, extra
	}

	if op, res, extra, ok := classifySourceLocationPath(method, path); ok {
		return op, res, extra
	}

	if op, res, extra, ok := classifyPrefetchSchedulePath(method, path); ok {
		return op, res, extra
	}

	if op, res, ok := classifyFunctionPath(method, path); ok {
		return op, res, ""
	}

	if op, ok := classifyConfigureLogsPath(method, path); ok {
		return op, "", ""
	}

	if path == pathAlerts && method == http.MethodGet {
		return opListAlerts, "", ""
	}

	if tagARN, ok := strings.CutPrefix(path, pathTags); ok && strings.Contains(tagARN, ":mediatailor:") {
		return classifyTagPath(method), tagARN, ""
	}

	return opUnknown, "", ""
}

func classifyPlaybackConfigPath(method, path string) (string, string, bool) {
	const prefix = pathPlaybackConfig + "/"

	switch {
	case path == pathPlaybackConfigs && method == http.MethodGet:
		return opListPlaybackConfigurations, "", true
	case path == pathPlaybackConfig && method == http.MethodPut:
		return opPutPlaybackConfiguration, "", true
	}

	name, ok := strings.CutPrefix(path, prefix)
	if !ok || strings.Contains(name, "/") {
		return "", "", false
	}

	switch method {
	case http.MethodGet:
		return opGetPlaybackConfiguration, name, true
	case http.MethodDelete:
		return opDeletePlaybackConfiguration, name, true
	}

	return "", "", false
}

func classifyChannelPath(method, path string) (string, string, string, bool) {
	if path == pathChannels && method == http.MethodGet {
		return opListChannels, "", "", true
	}

	after, ok := strings.CutPrefix(path, pathChannel)
	if !ok {
		return "", "", "", false
	}

	parts := strings.SplitN(after, "/", splitThree)
	channelName := parts[0]

	if len(parts) == 1 {
		return classifyChannelByMethod(method, channelName)
	}

	return classifyChannelSubPath(method, channelName, parts[1], strings.Join(parts[2:], "/"))
}

func classifyChannelByMethod(method, channelName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateChannel, channelName, "", true
	case http.MethodGet:
		return opDescribeChannel, channelName, "", true
	case http.MethodPut:
		return opUpdateChannel, channelName, "", true
	case http.MethodDelete:
		return opDeleteChannel, channelName, "", true
	}

	return "", "", "", false
}

func classifyChannelSubPath(method, channelName, subKey, extra string) (string, string, string, bool) {
	switch {
	case subKey == "start" && method == http.MethodPut:
		return opStartChannel, channelName, "", true
	case subKey == "stop" && method == http.MethodPut:
		return opStopChannel, channelName, "", true
	case subKey == "schedule" && method == http.MethodGet:
		return opGetChannelSchedule, channelName, "", true
	case subKey == "policy":
		return classifyChannelPolicyByMethod(method, channelName)
	case subKey == "program" && extra != "" && !strings.Contains(extra, "/"):
		return classifyProgramByMethod(method, channelName, extra)
	}

	return "", "", "", false
}

func classifyChannelPolicyByMethod(method, channelName string) (string, string, string, bool) {
	switch method {
	case http.MethodPut:
		return opPutChannelPolicy, channelName, "", true
	case http.MethodGet:
		return opGetChannelPolicy, channelName, "", true
	case http.MethodDelete:
		return opDeleteChannelPolicy, channelName, "", true
	}

	return "", "", "", false
}

func classifyProgramByMethod(method, channelName, programName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateProgram, channelName, programName, true
	case http.MethodGet:
		return opDescribeProgram, channelName, programName, true
	case http.MethodPut:
		return opUpdateProgram, channelName, programName, true
	case http.MethodDelete:
		return opDeleteProgram, channelName, programName, true
	}

	return "", "", "", false
}

// classifySourceLocationPath returns (op, sourceLocationName, secondaryName, ok).
func classifySourceLocationPath(method, path string) (string, string, string, bool) {
	if path == pathSourceLocations && method == http.MethodGet {
		return opListSourceLocations, "", "", true
	}

	after, ok := strings.CutPrefix(path, pathSourceLocation)
	if !ok {
		return "", "", "", false
	}

	// format: {slName} | {slName}/vodSources | {slName}/liveSources |
	//         {slName}/vodSource/{name} | {slName}/liveSource/{name}
	parts := strings.SplitN(after, "/", splitThree)
	slName := parts[0]

	switch len(parts) {
	case 1:
		return classifySourceLocationByMethod(method, slName)
	case splitTwo:
		if parts[1] == "vodSources" && method == http.MethodGet {
			return opListVodSources, slName, "", true
		}

		if parts[1] == "liveSources" && method == http.MethodGet {
			return opListLiveSources, slName, "", true
		}
	case splitThree:
		if parts[1] == "vodSource" {
			return classifyVodSourceByMethod(method, slName, parts[2])
		}

		if parts[1] == "liveSource" {
			return classifyLiveSourceByMethod(method, slName, parts[2])
		}
	}

	return "", "", "", false
}

func classifySourceLocationByMethod(method, slName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateSourceLocation, slName, "", true
	case http.MethodGet:
		return opDescribeSourceLocation, slName, "", true
	case http.MethodPut:
		return opUpdateSourceLocation, slName, "", true
	case http.MethodDelete:
		return opDeleteSourceLocation, slName, "", true
	}

	return "", "", "", false
}

func classifyVodSourceByMethod(method, slName, vodName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateVodSource, slName, vodName, true
	case http.MethodGet:
		return opDescribeVodSource, slName, vodName, true
	case http.MethodPut:
		return opUpdateVodSource, slName, vodName, true
	case http.MethodDelete:
		return opDeleteVodSource, slName, vodName, true
	}

	return "", "", "", false
}

func classifyLiveSourceByMethod(method, slName, lsName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateLiveSource, slName, lsName, true
	case http.MethodGet:
		return opDescribeLiveSource, slName, lsName, true
	case http.MethodPut:
		return opUpdateLiveSource, slName, lsName, true
	case http.MethodDelete:
		return opDeleteLiveSource, slName, lsName, true
	}

	return "", "", "", false
}

// classifyPrefetchSchedulePath handles /prefetchSchedule/{pc} and /prefetchSchedule/{pc}/{name}.
func classifyPrefetchSchedulePath(method, path string) (string, string, string, bool) {
	after, ok := strings.CutPrefix(path, pathPrefetchSchedule)
	if !ok {
		return "", "", "", false
	}

	parts := strings.SplitN(after, "/", splitTwo)
	pcName := parts[0]

	if len(parts) == 1 {
		// ListPrefetchSchedules is POST (not GET) on the bare
		// /prefetchSchedule/{PlaybackConfigurationName} path — confirmed
		// against aws-sdk-go-v2's serializer and botocore's service-2.json.
		// MaxResults/NextToken/ScheduleType/StreamId travel in the JSON
		// request body, not the query string.
		if method == http.MethodPost {
			return opListPrefetchSchedules, pcName, "", true
		}

		return "", "", "", false
	}

	scheduleName := parts[1]
	switch method {
	case http.MethodPost:
		return opCreatePrefetchSchedule, pcName, scheduleName, true
	case http.MethodGet:
		return opGetPrefetchSchedule, pcName, scheduleName, true
	case http.MethodDelete:
		return opDeletePrefetchSchedule, pcName, scheduleName, true
	}

	return "", "", "", false
}

// classifyFunctionPath handles /function/{id} and /functions.
func classifyFunctionPath(method, path string) (string, string, bool) {
	if path == pathFunctions && method == http.MethodGet {
		return opListFunctions, "", true
	}

	fnID, ok := strings.CutPrefix(path, pathFunction)
	if !ok || strings.Contains(fnID, "/") {
		return "", "", false
	}

	switch method {
	case http.MethodPut:
		return opPutFunction, fnID, true
	case http.MethodGet:
		return opGetFunction, fnID, true
	case http.MethodDelete:
		return opDeleteFunction, fnID, true
	}

	return "", "", false
}

// classifyConfigureLogsPath handles /configureLogs/channel and /configureLogs/playbackConfiguration.
func classifyConfigureLogsPath(method, path string) (string, bool) {
	if method != http.MethodPut {
		return "", false
	}

	suffix, ok := strings.CutPrefix(path, pathConfigureLogs)
	if !ok {
		return "", false
	}

	switch suffix {
	case "channel":
		return opConfigureLogsForChannel, true
	case "playbackConfiguration":
		return opConfigureLogsForPlaybackConfiguration, true
	}

	return "", false
}

func classifyTagPath(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
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

// errType returns the AWS modeled exception type name for err, matching the
// literal strings errors.go's sentinels wrap (ErrNotFound wraps
// "NotFoundException", etc). This travels in the X-Amzn-Errortype header --
// aws-sdk-go-v2's restjson.GetErrorInfo (aws/protocol/restjson/decoder_util.go)
// checks that header before falling back to a "code"/"__type" body field, and
// without it every mediatailor error deserializes client-side as a generic
// UnknownError regardless of the real failure.
func errType(err error) string {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return "NotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists):
		return "ConflictException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		return "BadRequestException"
	default:
		return "InternalFailure"
	}
}

func respondErr(c *echo.Context, err error) error {
	c.Response().Header().Set(amznErrorTypeHeader, errType(err))

	return c.JSON(errStatus(err), map[string]any{keyMessage: err.Error()})
}
