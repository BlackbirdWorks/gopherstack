package azurearm

import (
	"regexp"
	"slices"
	"strings"
)

const (
	extFieldPublisher = "publisher"
	extFieldType      = "type"
	extFieldVersion   = "typeHandlerVersion"
	extFieldProtected = "protectedSettings"
	extFieldKeyVault  = "protectedSettingsFromKeyVault"
	extFieldAfter     = "provisionAfterExtensions"
	extFieldAllowOps  = "allowExtensionOperations"
	extStatusOK       = "ProvisioningState/succeeded"
	extStatusOKText   = "Provisioning succeeded"
)

var extVersionRe = regexp.MustCompile(`^\d+(\.\d+){1,3}$`)

// extImage is one entry of the documented extension catalog; versions are the typeHandlerVersion values the
// extension's Learn page tells callers to set.
type extImage struct {
	publisher string
	name      string
	os        string
	versions  []string
}

// extCatalog covers extensions named on learn.microsoft.com/azure/virtual-machines/extensions/*; the real service
// lists far more and full build numbers.
//
//nolint:gochecknoglobals // static read-only catalog
var extCatalog = []extImage{
	{"Microsoft.Azure.Extensions", "CustomScript", osLinux, []string{"2.0", "2.1"}},
	{"Microsoft.Compute", "CustomScriptExtension", osWindows, []string{"1.10"}},
	{"Microsoft.Azure.ActiveDirectory", "AADSSHLoginForLinux", osLinux, []string{cmpVersion10}},
	{"Microsoft.Azure.ActiveDirectory", "AADLoginForWindows", osWindows, []string{cmpVersion10}},
	{"Microsoft.EnterpriseCloud.Monitoring", "OmsAgentForLinux", osLinux, []string{cmpVersion10}},
	{"Microsoft.EnterpriseCloud.Monitoring", "MicrosoftMonitoringAgent", osWindows, []string{cmpVersion10}},
	{"Microsoft.OSTCExtensions", "VMAccessForLinux", osLinux, []string{"1.5"}},
	{"Microsoft.Compute", "VMAccessAgent", osWindows, []string{"2.4"}},
	{"Microsoft.Azure.Security", "IaaSAntimalware", osWindows, []string{"2.0"}},
	{"Microsoft.Azure.Monitor", "AzureMonitorLinuxAgent", osLinux, []string{cmpVersion10}},
	{"Microsoft.Azure.Monitor", "AzureMonitorWindowsAgent", osWindows, []string{cmpVersion10}},
	{"Microsoft.Azure.Monitoring.DependencyAgent", "DependencyAgentLinux", osLinux, []string{"9.10"}},
	{"Microsoft.Azure.Monitoring.DependencyAgent", "DependencyAgentWindows", osWindows, []string{"9.10"}},
	{"Microsoft.Azure.NetworkWatcher", "NetworkWatcherAgentLinux", osLinux, []string{"1.4"}},
	{"Microsoft.Azure.NetworkWatcher", "NetworkWatcherAgentWindows", osWindows, []string{"1.4"}},
}

func findExtImage(publisher, typ string) (extImage, bool) {
	i := slices.IndexFunc(extCatalog, func(e extImage) bool {
		return strings.EqualFold(e.publisher, publisher) && strings.EqualFold(e.name, typ)
	})
	if i < 0 {
		return extImage{}, false
	}

	return extCatalog[i], true
}

func majorOf(v string) string {
	major, _, _ := strings.Cut(v, ".")

	return major
}

func extReq(field string) error {
	return cmpInvalid("Required parameter '%s' is missing (null).", field)
}

func putExtension(_ *ComputeProvider, parent *cmpRes, k, _ *cmpKid, body map[string]any) error {
	props := cloneProps(propsOf(body))

	for _, f := range []string{extFieldProtected, extFieldKeyVault, fieldProvisioningState, cmpFieldInstView} {
		delete(props, f)
	}

	pub, typ, ver := strOf(props, extFieldPublisher), strOf(props, extFieldType), strOf(props, extFieldVersion)

	for field, v := range map[string]string{extFieldPublisher: pub, extFieldType: typ, extFieldVersion: ver} {
		if v == "" {
			return extReq("properties." + field)
		}
	}

	if !extVersionRe.MatchString(ver) {
		return cmpInvalid("The typeHandlerVersion '%s' is not valid; use Major.Minor, for example 2.1.", ver)
	}

	if img, ok := findExtImage(pub, typ); ok && !slices.ContainsFunc(img.versions, func(v string) bool {
		return majorOf(v) == majorOf(ver)
	}) {
		return cmpInvalid("The extension with publisher '%s', type '%s' and handler version '%s' does not exist; "+
			"available versions: %s.", pub, typ, ver, strings.Join(img.versions, ", "))
	}

	if parent.Type == cmpVMs && mapOf(parent.Props[cmpFieldOSProfile])[extFieldAllowOps] == false {
		return cmpNotAllowed("Extension operations are not allowed on virtual machine %s: "+
			"osProfile.allowExtensionOperations is false.", parent.Name)
	}

	for _, name := range strList(props[extFieldAfter]) {
		if _, found := parent.Kids[kidKey(kidExtensions, []string{name})]; !found || strings.EqualFold(name, k.leaf()) {
			return cmpInvalid("provisionAfterExtensions refers to extension '%s', which does not exist on %s.",
				name, parent.Name)
		}
	}

	k.Props = props

	return nil
}

func renderExtension(p *ComputeProvider, parent *cmpRes, k *cmpKid, expand string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	if strings.EqualFold(expand, cmpFieldInstView) {
		props[cmpFieldInstView] = extInstanceView(k)
	}

	return p.kidBase(parent, k, props)
}

func extInstanceView(k *cmpKid) map[string]any {
	return map[string]any{
		fieldName:       k.leaf(),
		extFieldType:    strOf(k.Props, extFieldType),
		extFieldVersion: strOf(k.Props, extFieldVersion),
		cmpKeyStatuses:  []any{statusEntry(extStatusOK, extStatusOKText, k.Created)},
	}
}

func vmExtensionViews(r *cmpRes) []any {
	kids := kidsOf(r, kidExtensions)
	out := make([]any, 0, len(kids))

	for _, k := range kids {
		out = append(out, extInstanceView(k))
	}

	return out
}

// stripProfileSecrets removes write-only extension settings from a scale set profile before it is stored.
func stripProfileSecrets(profile map[string]any) {
	ext := mapOf(profile["extensionProfile"])

	for _, e := range anyList(ext["extensions"]) {
		props := mapOf(mapOf(e)[fieldProperties])
		delete(props, extFieldProtected)
		delete(props, extFieldKeyVault)
	}
}

func extTypeImage(loc, sub string, img extImage, version string) map[string]any {
	return map[string]any{
		cmpFieldID: "/subscriptions/" + sub + "/providers/Microsoft.Compute/locations/" + loc + "/publishers/" +
			img.publisher + "/artifacttypes/vmextension/types/" + img.name + "/versions/" + version,
		fieldName: version, fieldType: "Microsoft.Compute/locations/publishers/artifacttypes/types/versions",
		fieldLocation: loc,
		fieldProperties: map[string]any{
			"computeRole": "IaaS", "operatingSystem": img.os,
		},
	}
}

// ExtensionImageTypes lists the extension types a publisher offers in a location.
func ExtensionImageTypes(sub, loc, publisher string) []any {
	out := []any{}

	for _, e := range extCatalog {
		if strings.EqualFold(e.publisher, publisher) {
			out = append(out, map[string]any{
				cmpFieldID: "/subscriptions/" + sub + "/providers/Microsoft.Compute/locations/" + loc +
					"/publishers/" + e.publisher + "/artifacttypes/vmextension/types/" + e.name,
				fieldName: e.name, fieldLocation: loc,
			})
		}
	}

	return out
}

// ExtensionImageVersions lists the versions of one extension type; ok is false for an unknown type.
func ExtensionImageVersions(sub, loc, publisher, typ string) ([]any, bool) {
	img, ok := findExtImage(publisher, typ)
	if !ok {
		return nil, false
	}

	out := make([]any, 0, len(img.versions))
	for _, v := range img.versions {
		out = append(out, extTypeImage(loc, sub, img, v))
	}

	return out, true
}

// ExtensionImage returns one extension image; ok is false when the type or version is not offered.
func ExtensionImage(sub, loc, publisher, typ, version string) (map[string]any, bool) {
	img, ok := findExtImage(publisher, typ)
	if !ok || !slices.Contains(img.versions, version) {
		return nil, false
	}

	return extTypeImage(loc, sub, img, version), true
}

// extensionImagesBody answers the locations/{l}/publishers/{p}/artifacttypes/vmextension/types paths.
func extensionImagesBody(segs []string) (any, error) {
	loc, pub, sub := normLocation(segs[cmpSubLocIdx+1]), segs[extImgPubIdx], segs[1]

	switch len(segs) {
	case extImgTypesSegs:
		return ExtensionImageTypes(sub, loc, pub), nil
	case extImgVersionsSegs:
		if v, ok := ExtensionImageVersions(sub, loc, pub, segs[extImgTypeIdx]); ok {
			return v, nil
		}
	default:
		if img, ok := ExtensionImage(sub, loc, pub, segs[extImgTypeIdx], segs[extImgVerIdx]); ok {
			return img, nil
		}
	}

	return nil, ErrResourceNotFound
}

const (
	extImgPubIdx       = 7
	extImgTypeIdx      = 11
	extImgVerIdx       = 13
	extImgTypesSegs    = 11
	extImgVersionsSegs = 13
	extImgVersionSegs  = 14
)
