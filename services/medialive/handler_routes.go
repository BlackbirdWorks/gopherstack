package medialive

import (
	"net/http"
	"strings"
)

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
		return opDescribeInputDeviceThumbnail, extractSegment(
			path,
			prefix,
			"/"+subThumbnailData,
		), true
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
