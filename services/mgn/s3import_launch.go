package mgn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

const (
	csvLaunchPrefix        = "mgn:launch:"
	csvLaunchInstanceType  = "mgn:launch:instance-type"
	csvLaunchIamProfile    = "mgn:launch:iam-instance-profile:name"
	csvLaunchHostID        = "mgn:launch:placement:host-id"
	csvLaunchTenancy       = "mgn:launch:placement:tenancy"
	launchTagInstancePfx   = "tag:instance:"
	launchVolumePfx        = "volume:"
	launchNicPfx           = "nic:"
	launchPostActionsPfx   = "post-actions:"
	launchPostActionsOnKey = "post-actions:enabled"
)

var (
	errImportBadNicColumn = errors.New(
		"expected nic:<index>:<network-interface-id|subnet-id|private-ip:<n>|security-group-id:<n>>",
	)
	errImportBadVolColumn  = errors.New("expected volume:<device-name>:type")
	errImportBadActionCol  = errors.New("expected post-actions:<action-name>:<field>")
	errImportActionNoDoc   = errors.New("post-launch action needs ssmDocumentName")
	errImportBadParameters = errors.New("parameters must be a JSON object with only parameters and externalParameters")
	errImportBadPositive   = errors.New("expected a positive integer")
)

func isVolumeType(v string) bool {
	return slices.Contains([]string{"io1", "io2", "gp3", "gp2", "st1", "sc1", "standard"}, v)
}

// LaunchTemplateContents holds the EC2 launch template settings StartImport's
// mgn:launch:* columns carry. GetLaunchConfiguration exposes no field for them.
type LaunchTemplateContents struct {
	InstanceTags       map[string]string
	Volumes            map[string]string
	PostActions        map[string]PostActionSettings
	PostActionsEnabled *bool
	InstanceType       string
	IamProfileName     string
	HostID             string
	Tenancy            string
	MapTagKey          string
	NetworkInterfaces  []LaunchNetworkInterface
}

// LaunchNetworkInterface is one mgn:launch:nic:<index>:* group.
type LaunchNetworkInterface struct {
	NetworkInterfaceID string
	SubnetID           string
	PrivateIPs         []string
	SecurityGroupIDs   []string
	Index              int
}

// PostActionSettings holds the post-launch action columns SsmDocument has no field for.
type PostActionSettings struct {
	Active      *bool
	Description string
	Parameters  string
	Order       int
}

func (c *LaunchTemplateContents) clone() *LaunchTemplateContents {
	if c == nil {
		return nil
	}

	cp := *c
	cp.InstanceTags = cloneStrMap(c.InstanceTags)
	cp.Volumes = cloneStrMap(c.Volumes)
	cp.PostActions = maps.Clone(c.PostActions)
	cp.NetworkInterfaces = slices.Clone(c.NetworkInterfaces)

	for i, n := range cp.NetworkInterfaces {
		cp.NetworkInterfaces[i].PrivateIPs = slices.Clone(n.PrivateIPs)
		cp.NetworkInterfaces[i].SecurityGroupIDs = slices.Clone(n.SecurityGroupIDs)
	}

	return &cp
}

// importLaunchData is the parsed launch-template side of one CSV row.
type importLaunchData struct {
	contents *LaunchTemplateContents
	actions  []ssmDocument
}

func parseImportLaunchData(p *importConfigParser, idx importHeaderIndex) *importLaunchData {
	c := &LaunchTemplateContents{
		InstanceType:   p.str(csvLaunchInstanceType),
		IamProfileName: p.str(csvLaunchIamProfile),
		HostID:         p.str(csvLaunchHostID),
	}

	if t := p.enum(csvLaunchTenancy, "default", "dedicated", "host"); t != nil {
		c.Tenancy = *t
	}

	if k := p.enum(csvLaunchMapTagKey, "map-migrated", "aws-apn-id"); k != nil {
		c.MapTagKey = *k
	}

	c.InstanceTags = launchInstanceTags(p, idx)
	c.Volumes = launchVolumes(p, idx)
	c.NetworkInterfaces = launchNics(p, idx)
	c.PostActionsEnabled = launchBool(p, idx, launchPostActionsOnKey)

	actions := launchPostActions(p, idx, c)

	if launchContentsEmpty(c) && len(actions) == 0 {
		return nil
	}

	return &importLaunchData{contents: c, actions: actions}
}

func launchContentsEmpty(c *LaunchTemplateContents) bool {
	return c.InstanceType == "" && c.IamProfileName == "" && c.HostID == "" && c.Tenancy == "" &&
		c.MapTagKey == "" && len(c.InstanceTags) == 0 && len(c.Volumes) == 0 &&
		len(c.NetworkInterfaces) == 0 && c.PostActionsEnabled == nil && len(c.PostActions) == 0
}

func launchBool(p *importConfigParser, idx importHeaderIndex, key string) *bool {
	col := csvLaunchPrefix + key

	v := colValue(p.row, idx.launchCols, key)
	if v == "" {
		return nil
	}

	switch strings.ToLower(v) {
	case "true":
		t := true

		return &t
	case "false":
		f := false

		return &f
	}

	p.fail(col, errImportBadBool)

	return nil
}

func launchInstanceTags(p *importConfigParser, idx importHeaderIndex) map[string]string {
	tags := map[string]string{}

	for name := range idx.launchCols {
		if key, ok := strings.CutPrefix(name, launchTagInstancePfx); ok {
			if v := colValue(p.row, idx.launchCols, name); v != "" {
				tags[key] = v
			}
		}
	}

	return tags
}

func launchVolumes(p *importConfigParser, idx importHeaderIndex) map[string]string {
	vols := map[string]string{}

	for name := range idx.launchCols {
		rest, ok := strings.CutPrefix(name, launchVolumePfx)
		if !ok {
			continue
		}

		col := csvLaunchPrefix + name

		device, ok := strings.CutSuffix(rest, ":type")
		if !ok || device == "" {
			p.fail(col, errImportBadVolColumn)

			continue
		}

		v := colValue(p.row, idx.launchCols, name)
		if v == "" {
			continue
		}

		if !isVolumeType(v) {
			p.fail(col, fmt.Errorf("%w %q (want an EBS volume type)", errImportBadEnum, v))

			continue
		}

		vols[device] = v
	}

	return vols
}

func launchNics(p *importConfigParser, idx importHeaderIndex) []LaunchNetworkInterface {
	byIdx := map[int]*LaunchNetworkInterface{}

	for name := range idx.launchCols {
		rest, ok := strings.CutPrefix(name, launchNicPfx)
		if !ok {
			continue
		}

		v := colValue(p.row, idx.launchCols, name)
		if v == "" {
			continue
		}

		parts := strings.Split(rest, ":")

		n, err := strconv.Atoi(parts[0])
		if err != nil || n < 0 || len(parts) < 2 {
			p.fail(csvLaunchPrefix+name, errImportBadNicColumn)

			continue
		}

		nic := byIdx[n]
		if nic == nil {
			nic = &LaunchNetworkInterface{Index: n}
			byIdx[n] = nic
		}

		assignNicField(p, name, parts[1:], v, nic)
	}

	out := make([]LaunchNetworkInterface, 0, len(byIdx))
	for _, nic := range byIdx {
		out = append(out, *nic)
	}

	slices.SortFunc(out, func(a, b LaunchNetworkInterface) int { return a.Index - b.Index })

	return out
}

func assignNicField(p *importConfigParser, name string, field []string, v string, nic *LaunchNetworkInterface) {
	switch {
	case len(field) == 1 && field[0] == "network-interface-id":
		nic.NetworkInterfaceID = v
	case len(field) == 1 && field[0] == "subnet-id":
		nic.SubnetID = v
	case len(field) == 2 && field[0] == "private-ip":
		nic.PrivateIPs = append(nic.PrivateIPs, v)
	case len(field) == 2 && field[0] == "security-group-id":
		nic.SecurityGroupIDs = append(nic.SecurityGroupIDs, v)
	default:
		p.fail(csvLaunchPrefix+name, errImportBadNicColumn)
	}
}

type importActionFields struct {
	fields    map[string]string
	fieldCols map[string]string
	name      string
}

func launchPostActions(p *importConfigParser, idx importHeaderIndex, c *LaunchTemplateContents) []ssmDocument {
	groups := map[string]*importActionFields{}

	for name := range idx.launchCols {
		rest, ok := strings.CutPrefix(name, launchPostActionsPfx)
		if !ok || name == launchPostActionsOnKey {
			continue
		}

		action, field, ok := strings.Cut(rest, ":")
		if !ok || action == "" || field == "" {
			p.fail(csvLaunchPrefix+name, errImportBadActionCol)

			continue
		}

		v := colValue(p.row, idx.launchCols, name)
		if v == "" {
			continue
		}

		g := groups[action]
		if g == nil {
			g = &importActionFields{name: action, fields: map[string]string{}, fieldCols: map[string]string{}}
			groups[action] = g
		}

		g.fields[field] = v
		g.fieldCols[field] = csvLaunchPrefix + name
	}

	names := slices.Sorted(maps.Keys(groups))
	docs := make([]ssmDocument, 0, len(names))

	for _, n := range names {
		if doc, ok := buildPostAction(p, groups[n], c); ok {
			docs = append(docs, doc)
		}
	}

	return docs
}

func buildPostAction(p *importConfigParser, g *importActionFields, c *LaunchTemplateContents) (ssmDocument, bool) {
	doc := ssmDocument{ActionName: g.name, SsmDocumentName: g.fields["ssmDocumentName"]}
	settings := PostActionSettings{Description: g.fields["description"]}

	if doc.SsmDocumentName == "" {
		p.fail(csvLaunchPrefix+launchPostActionsPfx+g.name+":ssmDocumentName", errImportActionNoDoc)

		return doc, false
	}

	for field, v := range g.fields {
		applyActionField(p, g.fieldCols[field], field, v, &doc, &settings)
	}

	if c.PostActions == nil {
		c.PostActions = map[string]PostActionSettings{}
	}

	c.PostActions[g.name] = settings

	return doc, true
}

func applyActionField(p *importConfigParser, col, field, v string, doc *ssmDocument, st *PostActionSettings) {
	switch field {
	case "ssmDocumentName", "description":
	case "order":
		st.Order = int(positiveInt(p, col, v))
	case "timeoutSeconds":
		doc.TimeoutSeconds = positiveInt(p, col, v)
	case "active":
		if b, err := strconv.ParseBool(strings.ToLower(v)); err != nil {
			p.fail(col, errImportBadBool)
		} else {
			st.Active = &b
		}
	case "mustSucceedForCutover":
		b, err := strconv.ParseBool(strings.ToLower(v))
		if err != nil {
			p.fail(col, errImportBadBool)
		}

		doc.MustSucceedForCutover = b
	case "parameters":
		ext, err := parseActionParameters(v)
		if err != nil {
			p.fail(col, err)
		}

		doc.ExternalParameters = ext
		st.Parameters = v
	default:
		p.fail(col, errImportBadActionCol)
	}
}

func positiveInt(p *importConfigParser, col, v string) int32 {
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		p.fail(col, errImportBadPositive)

		return 0
	}

	return int32(n)
}

// parseActionParameters validates the documented parameters JSON and returns its
// externalParameters (dynamic paths), the only part SsmDocument has a field for.
func parseActionParameters(raw string) (map[string]string, error) {
	var v struct {
		Parameters map[string][]struct {
			Value string `json:"value"`
			Type  string `json:"type"`
		} `json:"parameters"`
		ExternalParameters map[string]string `json:"externalParameters"`
	}

	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %w", errImportBadParameters, err)
	}

	for _, list := range v.Parameters {
		for _, e := range list {
			if e.Type != "" && e.Type != "String" && e.Type != "StringList" {
				return nil, fmt.Errorf("%w: type %q", errImportBadParameters, e.Type)
			}
		}
	}

	return v.ExternalParameters, nil
}

// applyImportLaunchDataLocked merges d into serverID's launch configuration: set
// fields override, post-launch actions replace same-named ones and run in order.
func (b *InMemoryBackend) applyImportLaunchDataLocked(serverID string, d *importLaunchData) {
	lc, ok := b.launchConfigs.Get(serverID)
	if !ok || d == nil {
		return
	}

	lc.TemplateContents = mergeLaunchContents(lc.TemplateContents, d.contents)

	if len(d.actions) == 0 {
		return
	}

	if lc.PostLaunchActions == nil {
		lc.PostLaunchActions = &PostLaunchActions{}
	}

	docs := lc.PostLaunchActions.SsmDocuments
	for _, nd := range d.actions {
		docs = slices.DeleteFunc(docs, func(e ssmDocument) bool { return e.ActionName == nd.ActionName })
		docs = append(docs, nd)
	}

	order := lc.TemplateContents.PostActions
	slices.SortStableFunc(docs, func(a, b ssmDocument) int {
		return order[a.ActionName].Order - order[b.ActionName].Order
	})

	lc.PostLaunchActions.SsmDocuments = docs
}

func mergeLaunchContents(dst, src *LaunchTemplateContents) *LaunchTemplateContents {
	if dst == nil {
		dst = &LaunchTemplateContents{}
	}

	setIfNonEmpty := func(to *string, v string) {
		if v != "" {
			*to = v
		}
	}

	setIfNonEmpty(&dst.InstanceType, src.InstanceType)
	setIfNonEmpty(&dst.IamProfileName, src.IamProfileName)
	setIfNonEmpty(&dst.HostID, src.HostID)
	setIfNonEmpty(&dst.Tenancy, src.Tenancy)
	setIfNonEmpty(&dst.MapTagKey, src.MapTagKey)

	if src.PostActionsEnabled != nil {
		dst.PostActionsEnabled = src.PostActionsEnabled
	}

	dst.InstanceTags = mergeStrMaps(dst.InstanceTags, src.InstanceTags)
	dst.Volumes = mergeStrMaps(dst.Volumes, src.Volumes)

	if len(src.PostActions) > 0 {
		if dst.PostActions == nil {
			dst.PostActions = map[string]PostActionSettings{}
		}

		maps.Copy(dst.PostActions, src.PostActions)
	}

	for _, nic := range src.NetworkInterfaces {
		i := slices.IndexFunc(
			dst.NetworkInterfaces,
			func(e LaunchNetworkInterface) bool { return e.Index == nic.Index },
		)
		if i < 0 {
			dst.NetworkInterfaces = append(dst.NetworkInterfaces, nic)

			continue
		}

		dst.NetworkInterfaces[i] = nic
	}

	slices.SortFunc(dst.NetworkInterfaces, func(a, b LaunchNetworkInterface) int { return a.Index - b.Index })

	return dst
}

func mergeStrMaps(dst, src map[string]string) map[string]string {
	if len(src) == 0 {
		return dst
	}

	if dst == nil {
		dst = map[string]string{}
	}

	maps.Copy(dst, src)

	return dst
}
