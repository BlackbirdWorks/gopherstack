package azurearm

import (
	"net/http"
	"strings"
)

const (
	kidApps        = "applications"
	kidAppVersions = "applications/versions"

	appFieldOSType   = "supportedOSType"
	appFieldActions  = "customActions"
	appFieldManage   = "manageActions"
	appFieldSettings = "settings"
	appFieldInstall  = "install"
	appFieldRemove   = "remove"
	appFieldReboot   = "scriptBehaviorAfterReboot"
)

func checkCustomActions(raw any, path string) error {
	seen := map[string]bool{}

	for _, a := range anyList(raw) {
		m := mapOf(a)
		name := strOf(m, fieldName)

		if name == "" || strOf(m, "script") == "" {
			return cmpInvalid("Each entry of %s requires a name and a script.", path)
		}

		if seen[strings.ToLower(name)] {
			return cmpInvalid("The custom action name '%s' is used more than once in %s.", name, path)
		}

		seen[strings.ToLower(name)] = true

		for _, prm := range anyList(m["parameters"]) {
			if strOf(mapOf(prm), fieldName) == "" {
				return cmpInvalid("Each parameter of custom action '%s' requires a name.", name)
			}
		}
	}

	return nil
}

func putGalleryApp(_ *ComputeProvider, _ *cmpRes, k, old *cmpKid, body map[string]any) error {
	if err := validDefName(k.leaf()); err != nil {
		return err
	}

	props := cloneProps(propsOf(body))
	delete(props, fieldProvisioningState)

	osType, err := enumField(props, appFieldOSType, "properties", cmpOSTypes, "")
	if err != nil {
		return err
	}

	if osType == "" {
		return extReq("properties." + appFieldOSType)
	}

	props[appFieldOSType] = osType

	if old != nil && !strings.EqualFold(strOf(old.Props, appFieldOSType), osType) {
		return cmpChanged("galleryApplication.properties." + appFieldOSType)
	}

	if err = futureDate(props[gVerEOL]); err != nil {
		return err
	}

	if err = checkCustomActions(props[appFieldActions], "properties.customActions"); err != nil {
		return err
	}

	k.Props = props

	return nil
}

func renderGalleryApp(p *ComputeProvider, parent *cmpRes, k *cmpKid, _ string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	return p.kidBase(parent, k, props)
}

func deleteGalleryApp(_ *ComputeProvider, parent *cmpRes, k *cmpKid) error {
	prefix := kidKey(kidApps, []string{k.leaf()}) + "/"

	for key := range parent.Kids {
		if strings.HasPrefix(key, prefix) {
			return cannotDeleteNested()
		}
	}

	return nil
}

func checkAppPublishing(pub map[string]any) error {
	if strOf(mapOf(pub[gVerSource]), "mediaLink") == "" {
		return extReq("properties.publishingProfile.source.mediaLink")
	}

	manage := mapOf(pub[appFieldManage])
	for _, f := range []string{appFieldInstall, appFieldRemove} {
		if strOf(manage, f) == "" {
			return extReq("properties.publishingProfile.manageActions." + f)
		}
	}

	if _, set := mapOf(pub[appFieldSettings])[appFieldReboot]; set {
		if _, err := enumField(mapOf(pub[appFieldSettings]), appFieldReboot,
			"properties.publishingProfile.settings", []string{netNone, "Rerun"}, ""); err != nil {
			return err
		}
	}

	return checkCustomActions(pub[appFieldActions], "properties.publishingProfile.customActions")
}

func putGalleryAppVersion(_ *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	if err := parseVersionName(k.leaf()); err != nil {
		return err
	}

	if parent.Kids[kidKey(kidApps, k.Names[:1])] == nil {
		return netErr(http.StatusNotFound, codeParentNotFound, "Can not perform requested operation on nested "+
			"resource. Parent resource '%s/%s' not found.", parent.Name, k.Names[0])
	}

	props := cloneProps(propsOf(body))
	delete(props, fieldProvisioningState)
	delete(props, "replicationStatus")

	raw := mapOf(props[gVerPublish])
	if err := checkAppPublishing(raw); err != nil {
		return err
	}

	pub, err := normalizePublishing(raw, k.Location)
	if err != nil {
		return err
	}

	if err = futureDate(pub[gVerEOL]); err != nil {
		return err
	}

	pub["publishedDate"] = k.Created.Format(timeCreatedLayout)
	if old != nil {
		pub["publishedDate"] = mapOf(old.Props[gVerPublish])["publishedDate"]
	}

	props[gVerPublish] = pub
	k.Props = props

	return nil
}
