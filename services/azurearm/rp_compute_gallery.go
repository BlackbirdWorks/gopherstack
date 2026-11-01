package azurearm

import (
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	cmpImages    = "images"
	cmpGalleries = "galleries"

	gImgStorage         = "storageProfile"
	gImgOSState         = "osState"
	gImgHyperV          = "hyperVGeneration"
	gImgIdent           = "identifier"
	gVerPublish         = "publishingProfile"
	gVerRegions         = "targetRegions"
	gVerReplicas        = "replicaCount"
	gVerSAType          = "storageAccountType"
	gVerSource          = "source"
	gVerEOL             = "endOfLifeDate"
	verSALRS            = "Standard_LRS"
	cmpCachingRO        = "ReadOnly"
	repFieldState       = "state"
	verRepFull          = "Full"
	osStateGen          = "Generalized"
	osStateSpec         = "Specialized"
	imgNameMax          = 80
	imgPublishMax       = 128
	imgOfferMax         = 64
	verPartMax          = 2147483647
	replicationDone     = 100
	verParts            = 3
	sourceKeyFixed      = 3
	galleryVersionNames = 3
)

var (
	galleryNameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._]{0,78}[A-Za-z0-9])?$`)
	galleryImgRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$`)
	identPartRe   = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

//nolint:gochecknoglobals // static read-only enum lists from the Compute spec
var (
	cmpOSTypes     = []string{osLinux, osWindows}
	cmpOSStates    = []string{osStateGen, osStateSpec}
	cmpHyperV      = []string{"V1", "V2"}
	gallerySharing = []string{"Private", "Groups", "Community"}
	cmpCaching     = []string{cmpCachingNone, cmpCachingRO, cmpCachingRW}
	verSAs         = []string{verSALRS, "Standard_ZRS", cmpSKUPremium}
	verRepModes    = []string{verRepFull, "Shallow"}
)

func notFoundRes(idStr string) error {
	return netErr(
		http.StatusNotFound,
		netCodeNotFound,
		"The Resource '%s' was not found.",
		strings.TrimPrefix(idStr, "/"),
	)
}

func enumField(m map[string]any, key, path string, allowed []string, def string) (string, error) {
	v := strOf(m, key)
	if v == "" {
		if def == "" {
			return "", extReq(path + "." + key)
		}

		return def, nil
	}

	canon, ok := canonicalIn(allowed, v)
	if !ok {
		return "", cmpInvalid(
			"The %s.%s '%s' is not valid; allowed values are %s.",
			path,
			key,
			v,
			strings.Join(allowed, ", "),
		)
	}

	return canon, nil
}

func futureDate(v any, field string) error {
	s, _ := v.(string)
	if s == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse("2006-01-02", s)
	}

	if err != nil {
		return cmpInvalid("Could not convert string to DateTimeOffset: %s. Path 'properties.%s'", s, field)
	}

	if !t.After(time.Now()) {
		return cmpInvalid("EndOfLifeDate must be set to a future date.")
	}

	return nil
}

// ---- managed images ----

func (p *ComputeProvider) putImage(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], imgNameMax); err != nil {
		return nil, err
	}

	r, _, err := p.upsert(cmpImages, id, body)
	if err != nil {
		return nil, err
	}

	props := cloneProps(propsOf(body))
	delete(props, fieldProvisioningState)

	gen, err := enumField(props, gImgHyperV, "properties", cmpHyperV, "V1")
	if err != nil {
		return nil, err
	}

	props[gImgHyperV] = gen

	srcID := strOf(mapOf(props["sourceVirtualMachine"]), cmpFieldID)
	sp := mapOf(props[gImgStorage])

	switch {
	case srcID != "" && len(sp) > 0:
		return nil, cmpInvalid(
			"Only one of properties.sourceVirtualMachine and properties.storageProfile can be specified.",
		)
	case srcID != "":
		vm, ok := p.item(cmpVMs, srcID)
		if !ok {
			return nil, notFoundRes(srcID)
		}

		props[gImgStorage] = p.imageFromVM(vm)
		props["sourceVirtualMachine"] = map[string]any{cmpFieldID: vm.armID()}
	case len(sp) == 0:
		return nil, extReq("properties.storageProfile")
	default:
		stored, serr := p.validImageStorage(sp)
		if serr != nil {
			return nil, serr
		}

		props[gImgStorage] = stored
	}

	r.Props = props
	p.items[cmpKey(cmpImages, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) imageFromVM(vm *cmpRes) map[string]any {
	state := osStateSpec
	if vm.Aux[auxGeneralized] == cmpTrue {
		state = osStateGen
	}

	sp := mapOf(vm.Props[cmpFieldStorage])
	osd := mapOf(sp[cmpFieldOSDisk])
	md := mapOf(osd[cmpFieldManagedDsk])
	out := map[string]any{
		cmpKeyOSType: vmOSType(vm.Props), gImgOSState: state,
		cmpFieldManagedDsk: map[string]any{cmpFieldID: strOf(md, cmpFieldID)},
		cmpFieldCaching:    firstNonEmpty(strOf(osd, cmpFieldCaching), cmpCachingRW),
		cmpFieldDiskSizeGB: intOf(osd[cmpFieldDiskSizeGB]),
		cmpFieldStorType:   strOf(md, cmpFieldStorType),
	}
	rawDisks := anyList(sp[cmpFieldDataDisks])
	data := make([]any, 0, len(rawDisks))

	for _, e := range rawDisks {
		m := mapOf(e)
		data = append(data, map[string]any{
			cmpFieldLun: intOf(
				m[cmpFieldLun],
			),
			cmpFieldCaching:    firstNonEmpty(strOf(m, cmpFieldCaching), cmpCachingNone),
			cmpFieldManagedDsk: map[string]any{cmpFieldID: strOf(mapOf(m[cmpFieldManagedDsk]), cmpFieldID)},
			cmpFieldDiskSizeGB: intOf(m[cmpFieldDiskSizeGB]),
		})
	}

	return map[string]any{cmpFieldOSDisk: out, cmpFieldDataDisks: data, "zoneResilient": false}
}

func (p *ComputeProvider) validImageStorage(sp map[string]any) (map[string]any, error) {
	osd := cloneProps(mapOf(sp[cmpFieldOSDisk]))
	if len(osd) == 0 {
		return nil, extReq("properties.storageProfile.osDisk")
	}

	path := "storageProfile.osDisk"

	var err error
	if osd[cmpKeyOSType], err = enumField(osd, cmpKeyOSType, path, cmpOSTypes, ""); err != nil {
		return nil, err
	}

	if osd[gImgOSState], err = enumField(osd, gImgOSState, path, cmpOSStates, ""); err != nil {
		return nil, err
	}

	if err = p.validImageDisk(osd, path); err != nil {
		return nil, err
	}

	rawData := anyList(sp[cmpFieldDataDisks])
	data := make([]any, 0, len(rawData))
	seen := map[int]bool{}

	for _, e := range rawData {
		d := cloneProps(mapOf(e))

		lun, ok := d[cmpFieldLun].(float64)
		if !ok || lun < 0 || lun > cmpMaxLun {
			return nil, cmpInvalid("The data disk LUN is required and must be between 0 and %d.", cmpMaxLun)
		}

		if seen[int(lun)] {
			return nil, cmpInvalid(
				"Data disk LUN %d is used by more than one data disk; LUNs must be unique.",
				int(lun),
			)
		}

		seen[int(lun)] = true

		if err = p.validImageDisk(d, "storageProfile.dataDisks"); err != nil {
			return nil, err
		}

		data = append(data, d)
	}

	out := map[string]any{cmpFieldOSDisk: osd, cmpFieldDataDisks: data}
	out["zoneResilient"], _ = sp["zoneResilient"].(bool)

	return out, nil
}

func (p *ComputeProvider) validImageDisk(d map[string]any, path string) error {
	refs := 0

	for _, ref := range []struct {
		key string
		typ string
	}{{"snapshot", cmpSnaps}, {cmpFieldManagedDsk, cmpDisks}} {
		idStr := strOf(mapOf(d[ref.key]), cmpFieldID)
		if idStr == "" {
			continue
		}

		refs++

		if _, ok := p.item(ref.typ, idStr); !ok {
			return notFoundRes(idStr)
		}
	}

	if strOf(d, "blobUri") != "" {
		refs++
	}

	if refs != 1 {
		return cmpInvalid("Exactly one of %s.snapshot, managedDisk or blobUri must be specified.", path)
	}

	if _, err := enumField(d, cmpFieldCaching, path, cmpCaching, cmpCachingRW); err != nil {
		return err
	}

	if t := strOf(d, cmpFieldStorType); t != "" {
		if _, ok := canonicalIn(cmpDiskSKUs, t); !ok {
			return cmpInvalid("The storage account type '%s' is not valid; allowed values are %s.", t,
				strings.Join(cmpDiskSKUs, ", "))
		}
	}

	return nil
}

func (p *ComputeProvider) imageProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	return props
}

// ---- galleries ----

func (p *ComputeProvider) putGallery(id ResourceID, body map[string]any) (*cmpRes, error) {
	name := id.Names[0]
	if !galleryNameRe.MatchString(name) {
		return nil, cmpInvalid("Gallery name is invalid. Allowed characters are English alphanumeric characters, " +
			"with underscores, and periods allowed in the middle, up to 80 characters total. All other special " +
			"characters, including dashes, are disallowed.")
	}

	r, old, err := p.upsert(cmpGalleries, id, body)
	if err != nil {
		return nil, err
	}

	if old == nil {
		for _, g := range p.ofType(cmpGalleries) {
			if strings.EqualFold(g.Name, name) && g.Sub == r.Sub {
				return nil, cmpInvalid("The gallery name %s is not unique within the subscription %s. Please pick "+
					"another gallery name", name, r.Sub)
			}
		}
	}

	props := cloneProps(propsOf(body))
	for _, k := range []string{gImgIdent, fieldProvisioningState} {
		delete(props, k)
	}

	sharing := cloneProps(mapOf(props["sharingProfile"]))
	if sharing["permissions"], err = enumField(sharing, "permissions", "properties.sharingProfile",
		gallerySharing, "Private"); err != nil {
		return nil, err
	}

	props["sharingProfile"] = sharing

	r.Props = props
	p.items[cmpKey(cmpGalleries, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) galleryProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	props[gImgIdent] = map[string]any{"uniqueName": r.Sub + "-" + strings.ToUpper(r.Name)}
	props[fieldProvisioningState] = provisioningStateSucceeded

	return props
}

func cannotDeleteNested() error {
	return netErr(http.StatusConflict, codeCannotDelete, "Can not delete resource before nested resources are deleted.")
}

// ---- image definitions ----

func identPart(field, v string, limit int) error {
	if v == "" || len(v) > limit || !identPartRe.MatchString(v) || strings.HasSuffix(v, ".") {
		return cmpInvalid("Asset name galleryImage.properties.identifier.%s is not valid. It cannot be empty. "+
			"Allowed characters are uppercase or lowercase letters, digits, hyphen(-), period (.), underscore (_). "+
			"Names are not allowed to end with period(.). The length of the name cannot exceed %d characters.",
			field, limit)
	}

	return nil
}

func validDefName(name string) error {
	if !galleryImgRe.MatchString(name) || strings.HasSuffix(name, "-") || strings.HasSuffix(name, ".") {
		return cmpInvalid("The entity name %s is invalid according to its validation rule: "+
			`^[^_\W][\w-._]{0,79}(?<![-.])$`, name)
	}

	return nil
}

func minMaxOK(m map[string]any, label string) error {
	lo, hi := intOf(m["min"]), intOf(m["max"])
	if hi != 0 && lo > hi {
		return cmpInvalid("The minimum value of %s must not be greater than the maximum value of %s.", label, label)
	}

	return nil
}

func putGalleryImage(_ *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	if err := validDefName(k.leaf()); err != nil {
		return err
	}

	props := cloneProps(propsOf(body))
	delete(props, fieldProvisioningState)

	var err error
	if props[cmpKeyOSType], err = enumField(props, cmpKeyOSType, "properties", cmpOSTypes, ""); err != nil {
		return err
	}

	if props[gImgOSState], err = enumField(props, gImgOSState, "properties", cmpOSStates, ""); err != nil {
		return err
	}

	if props[gImgHyperV], err = enumField(props, gImgHyperV, "properties", cmpHyperV, "V1"); err != nil {
		return err
	}

	ident := mapOf(props[gImgIdent])
	for field, limit := range map[string]int{"publisher": imgPublishMax, "offer": imgOfferMax, cmpFieldSKU: imgOfferMax} {
		if err = identPart(field, strOf(ident, field), limit); err != nil {
			return err
		}
	}

	rec := mapOf(props["recommended"])
	for _, f := range []string{"vCPUs", cmpFieldMemory} {
		if err = minMaxOK(mapOf(rec[f]), f); err != nil {
			return err
		}
	}

	if err = futureDate(props[gVerEOL], gVerEOL); err != nil {
		return err
	}

	if err = checkDefIdentity(parent, k, old, props, ident); err != nil {
		return err
	}

	k.Props = props

	return nil
}

func checkDefIdentity(parent *cmpRes, k, old *cmpKid, props, ident map[string]any) error {
	if old != nil {
		for _, f := range []string{cmpKeyOSType, gImgOSState, gImgHyperV} {
			if props[f] != old.Props[f] {
				return cmpChanged("galleryImage.properties." + f)
			}
		}

		oi := mapOf(old.Props[gImgIdent])
		for _, f := range []string{"publisher", "offer", cmpFieldSKU} {
			if !strings.EqualFold(strOf(ident, f), strOf(oi, f)) {
				return cmpChanged("galleryImage.properties.identifier." + f)
			}
		}

		return nil
	}

	for _, other := range kidsOf(parent, kidImages) {
		oi := mapOf(other.Props[gImgIdent])
		if strings.EqualFold(strOf(oi, "publisher"), strOf(ident, "publisher")) &&
			strings.EqualFold(strOf(oi, "offer"), strOf(ident, "offer")) &&
			strings.EqualFold(strOf(oi, cmpFieldSKU), strOf(ident, cmpFieldSKU)) {
			return netErr(http.StatusConflict, cmpCodeConflict, "Gallery image: %s identified by (publisher:%s, "+
				"offer:%s, sku:%s) already exists. Choose a different publisher, offer, sku combination.",
				k.leaf(), strOf(ident, "publisher"), strOf(ident, "offer"), strOf(ident, cmpFieldSKU))
		}
	}

	return nil
}

func renderGalleryImage(p *ComputeProvider, parent *cmpRes, k *cmpKid, _ string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	return p.kidBase(parent, k, props)
}

func deleteGalleryImage(_ *ComputeProvider, parent *cmpRes, k *cmpKid) error {
	prefix := kidKey(kidImages, []string{k.leaf()}) + "/"

	for key := range parent.Kids {
		if strings.HasPrefix(key, prefix) {
			return cannotDeleteNested()
		}
	}

	return nil
}

// ---- image versions ----

func parseVersionName(name string) error {
	parts := strings.Split(name, ".")
	ok := len(parts) == verParts

	for _, part := range parts {
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || n < 0 || n > verPartMax || part != strconv.FormatInt(n, 10) {
			ok = false
		}
	}

	if !ok {
		return cmpInvalid("The image version name is invalid. The image version name should follow " +
			"Major(int).Minor(int).Patch(int) format, for e.g: 1.0.0, 2018.12.1 etc")
	}

	return nil
}

func versionNumbers(name string) [verParts]int {
	var out [verParts]int

	for i, part := range strings.Split(name, ".") {
		if i < verParts {
			out[i], _ = strconv.Atoi(part)
		}
	}

	return out
}

// versionSource is what an image version is built from.
type versionSource struct {
	osType, osState, hyper, location, id string
}

func subPrefixed(idStr string) bool {
	return strings.HasPrefix(strings.ToLower(idStr), "/subscriptions/")
}

func (p *ComputeProvider) sourceFor(idStr string, whole bool) (versionSource, error) {
	if !subPrefixed(idStr) {
		return versionSource{}, cmpInvalid("Property id %s at path 'properties.storageProfile.source.id' is invalid. "+
			"Expect fully qualified resource Id that start with '/subscriptions/<subscriptionID>' or "+
			"'/providers/<resourceProviderNamespace>/'", idStr)
	}

	if !whole {
		return p.diskSource(idStr)
	}

	if vm, ok := p.item(cmpVMs, idStr); ok {
		state := osStateSpec
		if vm.Aux[auxGeneralized] == cmpTrue {
			state = osStateGen
		}

		return versionSource{osType: vmOSType(vm.Props), osState: state, location: vm.Location, id: vm.armID()}, nil
	}

	if img, ok := p.item(cmpImages, idStr); ok {
		osd := mapOf(mapOf(img.Props[gImgStorage])[cmpFieldOSDisk])

		return versionSource{
			osType: strOf(osd, cmpKeyOSType), osState: strOf(osd, gImgOSState), hyper: strOf(img.Props, gImgHyperV),
			location: img.Location, id: img.armID(),
		}, nil
	}

	if info, err := p.galleryRef(idStr); err == nil && info.version != nil {
		return versionSource{
			osType: info.osType, osState: info.osState, hyper: info.hyper, location: info.version.Location,
			id: idStr,
		}, nil
	}

	return versionSource{}, p.sourceMissing(idStr)
}

func (p *ComputeProvider) sourceMissing(idStr string) error {
	rid, err := ParseGenericResourcePath(idStr)
	if err == nil && strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) {
		return netErr(
			http.StatusNotFound,
			netCodeNotFound,
			"Source %s is not found. Please check source exists, and is "+
				"in the same region as gallery image version being created",
			idStr,
		)
	}

	return cmpInvalid("The source id: %s must either be a managed image, virtual machine or another gallery image "+
		"version", idStr)
}

func (p *ComputeProvider) diskSource(idStr string) (versionSource, error) {
	if d, ok := p.item(cmpDisks, idStr); ok {
		return versionSource{osType: strOf(d.Props, cmpKeyOSType), location: d.Location, id: d.armID()}, nil
	}

	if s, ok := p.item(cmpSnaps, idStr); ok {
		return versionSource{osType: strOf(s.Props, cmpKeyOSType), location: s.Location, id: s.armID()}, nil
	}

	rid, err := ParseGenericResourcePath(idStr)
	if err == nil && strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) &&
		(strings.EqualFold(firstType(rid), cmpDisks) || strings.EqualFold(firstType(rid), cmpSnaps)) {
		return versionSource{}, netErr(http.StatusNotFound, netCodeNotFound, "Source was not found: %s. Please make "+
			"sure the source exists", idStr)
	}

	return versionSource{}, cmpInvalid("The source id: %s must be a managed disk or snapshot", idStr)
}

func (p *ComputeProvider) versionSources(sp map[string]any) ([]versionSource, error) {
	top := mapOf(sp[gVerSource])
	topID := firstNonEmpty(strOf(top, cmpFieldID), strOf(top, "virtualMachineId"))
	osImg := mapOf(sp["osDiskImage"])
	disks := anyList(sp["dataDiskImages"])

	hasDisks := len(osImg) > 0 || len(disks) > 0

	switch {
	case topID != "" && hasDisks, topID == "" && !hasDisks:
		return nil, cmpInvalid("The gallery artifact version source can only be specified either directly under " +
			"storageProfile or within individual OS or data disks. One and only one source type (user image, " +
			"snapshot, disk, virtual machine) can be provided")
	case topID != "":
		s, err := p.sourceFor(topID, true)

		return []versionSource{s}, err
	}

	var out []versionSource

	ids := map[string]bool{}

	for _, d := range append([]any{osImg}, disks...) {
		m := mapOf(d)
		if len(m) == 0 {
			continue
		}

		idStr := strOf(mapOf(m[gVerSource]), cmpFieldID)
		if idStr == "" {
			return nil, cmpInvalid("Source was not found: . Please make sure the source exists")
		}

		if ids[strings.ToLower(idStr)] {
			return nil, cmpInvalid("Duplicated source ids are found in the input disks. Source id should be unique " +
				"for each disk")
		}

		ids[strings.ToLower(idStr)] = true

		s, err := p.sourceFor(idStr, false)
		if err != nil {
			return nil, err
		}

		out = append(out, s)
	}

	return out, nil
}

func (p *ComputeProvider) checkVersionSources(k, def *cmpKid, sp map[string]any) error {
	srcs, err := p.versionSources(sp)
	if err != nil {
		return err
	}

	for _, s := range srcs {
		if s.location != "" && s.location != k.Location {
			return netErr(http.StatusNotFound, netCodeNotFound, "Source %s is not found. Please check source exists, "+
				"and is in the same region as gallery image version being created", s.id)
		}

		if s.osState != "" && !strings.EqualFold(s.osState, strOf(def.Props, gImgOSState)) {
			return cmpInvalid("Cannot create Gallery Image Version from: %s since the OS State in the parent gallery "+
				"image (%s) is not %s", s.id, strOf(def.Props, gImgOSState), s.osState)
		}

		if s.hyper != "" && !strings.EqualFold(s.hyper, strOf(def.Props, gImgHyperV)) {
			return cmpInvalid("The resource with id '%s' has a different Hypervisor generation ['%s'] than the parent "+
				"gallery image Hypervisor generation ['%s']", s.id, s.hyper, strOf(def.Props, gImgHyperV))
		}

		if s.osType != "" && !strings.EqualFold(s.osType, strOf(def.Props, cmpKeyOSType)) {
			return cmpInvalid("The resource with id '%s' has a different OS type ['%s'] than the parent gallery image "+
				"OS type generation ['%s']", s.id, s.osType, strOf(def.Props, cmpKeyOSType))
		}
	}

	return nil
}

func normalizePublishing(pp map[string]any, loc string) (map[string]any, error) {
	out := cloneProps(pp)
	delete(out, "publishedDate")

	replicas := 1
	if v, set := out[gVerReplicas]; set {
		replicas = intOf(v)
	}

	if replicas < 1 {
		return nil, cmpInvalid("The value %d of parameter replicaCount is out of range. The value must be at least 1.",
			replicas)
	}

	out[gVerReplicas] = replicas

	var err error
	if out[gVerSAType], err = enumField(out, gVerSAType, gVerPublish, verSAs, verSALRS); err != nil {
		return nil, err
	}

	if _, set := out["replicationMode"]; set {
		if out["replicationMode"], err = enumField(out, "replicationMode", gVerPublish, verRepModes, ""); err != nil {
			return nil, err
		}
	}

	setDefault(out, "excludeFromLatest", false)

	regions, err := normalizeRegions(anyList(out[gVerRegions]), loc, replicas, strOf(out, gVerSAType))
	if err != nil {
		return nil, err
	}

	out[gVerRegions] = regions

	return out, nil
}

func normalizeRegions(raw []any, loc string, replicas int, sa string) ([]any, error) {
	if len(raw) == 0 {
		return []any{map[string]any{fieldName: loc, "regionalReplicaCount": replicas, gVerSAType: sa}}, nil
	}

	seen := map[string]bool{}
	out := make([]any, 0, len(raw))

	for _, e := range raw {
		m := cloneProps(mapOf(e))
		name := normLocation(strOf(m, fieldName))

		if name == "" {
			return nil, extReq("properties.publishingProfile.targetRegions.name")
		}

		if seen[name] {
			return nil, cmpInvalid("Duplicate regions are not allowed in target publishing regions")
		}

		seen[name] = true
		m[fieldName] = name

		if n, set := m["regionalReplicaCount"]; !set {
			m["regionalReplicaCount"] = replicas
		} else if intOf(n) < 1 {
			return nil, cmpInvalid("The value %d of parameter regionalReplicaCount is out of range. The value must "+
				"be at least 1.", intOf(n))
		}

		var err error
		if m[gVerSAType], err = enumField(m, gVerSAType, gVerRegions, verSAs, sa); err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	if !seen[loc] {
		return nil, cmpInvalid("Gallery image version publishing profile regions %v must contain the location of "+
			"image version %s", regionNames(out), loc)
	}

	return out, nil
}

func regionNames(regions []any) []string {
	out := make([]string, 0, len(regions))
	for _, r := range regions {
		out = append(out, strOf(mapOf(r), fieldName))
	}

	return out
}

func putGalleryVersion(p *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	if err := parseVersionName(k.leaf()); err != nil {
		return err
	}

	def := parent.Kids[kidKey(kidImages, k.Names[:1])]
	if def == nil {
		return netErr(http.StatusNotFound, codeParentNotFound, "Can not perform requested operation on nested "+
			"resource. Parent resource '%s/%s' not found.", parent.Name, k.Names[0])
	}

	props := cloneProps(propsOf(body))
	delete(props, fieldProvisioningState)
	delete(props, "replicationStatus")

	sp := cloneProps(mapOf(props[gImgStorage]))

	if old != nil {
		if sourceKey(sp) != sourceKey(mapOf(old.Props[gImgStorage])) {
			return cmpChanged("galleryImageVersion.properties.storageProfile.source.id")
		}
	}

	if err := p.checkVersionSources(k, def, sp); err != nil {
		return err
	}

	pub, err := normalizePublishing(mapOf(props[gVerPublish]), k.Location)
	if err != nil {
		return err
	}

	if err = futureDate(pub[gVerEOL], gVerEOL); err != nil {
		return err
	}

	pub["publishedDate"] = k.Created.Format(timeCreatedLayout)

	if old != nil {
		pub["publishedDate"] = mapOf(old.Props[gVerPublish])["publishedDate"]
	}

	props[gVerPublish] = pub
	props[gImgStorage] = sp
	k.Props = props

	return nil
}

func sourceKey(sp map[string]any) string {
	src := mapOf(sp[gVerSource])
	parts := make([]string, 0, sourceKeyFixed+len(anyList(sp["dataDiskImages"])))
	parts = append(parts, strings.ToLower(strOf(src, cmpFieldID)), strings.ToLower(strOf(src, "virtualMachineId")),
		strings.ToLower(strOf(mapOf(mapOf(sp["osDiskImage"])[gVerSource]), cmpFieldID)))

	for _, d := range anyList(sp["dataDiskImages"]) {
		m := mapOf(d)
		parts = append(
			parts,
			fmt.Sprint(intOf(m[cmpFieldLun]), strings.ToLower(strOf(mapOf(m[gVerSource]), cmpFieldID))),
		)
	}

	return strings.Join(parts, "|")
}

func renderGalleryVersion(p *ComputeProvider, parent *cmpRes, k *cmpKid, expand string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	if strings.EqualFold(expand, "ReplicationStatus") {
		regions := regionNames(anyList(mapOf(props[gVerPublish])[gVerRegions]))
		summary := make([]any, 0, len(regions))

		for _, reg := range regions {
			summary = append(
				summary,
				map[string]any{"region": reg, repFieldState: cmpStateCompleted, "progress": replicationDone},
			)
		}

		props["replicationStatus"] = map[string]any{"aggregatedState": cmpStateCompleted, "summary": summary}
	}

	return p.kidBase(parent, k, props)
}

func deleteGalleryVersion(_ *ComputeProvider, _ *cmpRes, k *cmpKid) error {
	safety := mapOf(k.Props["safetyProfile"])
	block, _ := safety["blockDeletionBeforeEndOfLife"].(bool)

	if eol := strOf(mapOf(k.Props[gVerPublish]), gVerEOL); block && eol != "" {
		if t, err := time.Parse(time.RFC3339, eol); err != nil || t.After(time.Now()) {
			return netErr(http.StatusConflict, "DeleteGalleryImageVersionBlockedBeforeEndOfLife",
				"DeleteGalleryImageVersionBlockedBeforeEndOfLife. Deletion of Gallery image version is not allowed "+
					"because the image is enabled for blockDeletionBeforeEndOfLife and its endOfLifeDate has not "+
					"reached yet")
		}
	}

	return nil
}

// ---- image references on virtual machines ----

type imageInfo struct {
	version                *cmpKid
	osType, osState, hyper string
}

// galleryRef resolves a gallery image definition or version id; version is nil for a definition-only id.
func (p *ComputeProvider) galleryRef(idStr string) (imageInfo, error) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) || len(rid.Types) < 2 ||
		!strings.EqualFold(rid.Types[0], cmpGalleries) || !strings.EqualFold(rid.Types[1], kidImages) {
		return imageInfo{}, ErrResourceNotFound
	}

	g, ok := p.top(cmpGalleries, rid.ResourceGroup, rid.Names[0])
	if !ok || len(rid.Names) < 2 {
		return imageInfo{}, notFoundRes(idStr)
	}

	def := g.Kids[kidKey(kidImages, rid.Names[1:2])]
	if def == nil {
		return imageInfo{}, notFoundRes(idStr)
	}

	info := imageInfo{
		osType: strOf(
			def.Props,
			cmpKeyOSType,
		),
		osState: strOf(def.Props, gImgOSState),
		hyper:   strOf(def.Props, gImgHyperV),
	}

	if len(rid.Names) == galleryVersionNames {
		v := g.Kids[kidKey(kidVersions, rid.Names[1:3])]
		if v == nil {
			return imageInfo{}, notFoundRes(idStr)
		}

		info.version = v

		return info, nil
	}

	info.version = latestVersion(g, rid.Names[1])

	return info, nil
}

// latestVersion picks the highest version of def that is not excluded from latest.
func latestVersion(g *cmpRes, def string) *cmpKid {
	var best *cmpKid

	for _, v := range kidsOf(g, kidVersions) {
		if !strings.EqualFold(v.Names[0], def) || mapOf(v.Props[gVerPublish])["excludeFromLatest"] == true {
			continue
		}

		if best == nil || cmpVersion(versionNumbers(v.leaf()), versionNumbers(best.leaf())) > 0 {
			best = v
		}
	}

	return best
}

func cmpVersion(a, b [verParts]int) int {
	for i := range a {
		if a[i] != b[i] {
			return a[i] - b[i]
		}
	}

	return 0
}

// vmImage resolves storageProfile.imageReference.id for a new VM; nil means a platform image or no id.
func (p *ComputeProvider) vmImage(r *cmpRes, ref map[string]any) (*imageInfo, error) {
	idStr := strOf(ref, cmpFieldID)
	if idStr == "" {
		return nil, nil //nolint:nilnil // no image id is not an error
	}

	low := strings.ToLower(idStr)

	switch {
	case strings.Contains(low, "/publishers/") && strings.Contains(low, "/artifacttypes/vmimage/"):
		return nil, nil //nolint:nilnil // platform image ids are accepted as-is
	case strings.Contains(low, "/sharedgalleries/") || strings.Contains(low, "/communitygalleries/"):
		return nil, cmpNotImplemented("Shared and community gallery image references")
	}

	if img, ok := p.item(cmpImages, idStr); ok {
		osd := mapOf(mapOf(img.Props[gImgStorage])[cmpFieldOSDisk])

		return &imageInfo{
			osType: strOf(osd, cmpKeyOSType), osState: strOf(osd, gImgOSState), hyper: strOf(img.Props, gImgHyperV),
		}, nil
	}

	rid, perr := ParseGenericResourcePath(idStr)
	if perr != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) {
		return nil, cmpInvalid("The imageReference.id '%s' is not a valid image resource id.", idStr)
	}

	if strings.EqualFold(firstType(rid), cmpImages) {
		return nil, notFoundRes(idStr)
	}

	info, err := p.galleryRef(idStr)
	if err != nil {
		return nil, err
	}

	if info.version == nil {
		return nil, cmpInvalid("There is no latest image version exists for \"%s\"", idStr)
	}

	if slices.Contains(regionNames(anyList(mapOf(info.version.Props[gVerPublish])[gVerRegions])), r.Location) {
		return &info, nil
	}

	if len(rid.Names) == galleryVersionNames {
		return nil, cmpInvalid("The gallery image %s is not available in %s region. Please contact image owner to "+
			"replicate to this region, or change your requested region.", idStr, r.Location)
	}

	return nil, cmpInvalid("The gallery image %s is not available in %s region. Please contact image owner to "+
		"replicate to this region, or change your requested region", idStr, r.Location)
}
