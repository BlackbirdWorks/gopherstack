package ocicore

import (
	"encoding/json/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	kindInstanceConfig = "instanceconfiguration"
	kindInstancePool   = "instancepool"

	poolProvisioning = "PROVISIONING"
	poolRunning      = "RUNNING"
	poolScaling      = "SCALING"
	poolStarting     = "STARTING"
	poolStopping     = "STOPPING"
	poolStopped      = "STOPPED"
	maxPoolSize      = 1000
	launchToken      = "${launchCount}"
)

// InstanceConfiguration is a reusable launch template; instanceDetails is kept as the client sent it.
type InstanceConfiguration struct {
	InstanceDetails map[string]any `json:"instanceDetails"`
	Base
}

// PoolPlacement is one placement configuration of an instance pool.
type PoolPlacement struct {
	PrimaryVnicSubnets   map[string]any   `json:"primaryVnicSubnets,omitempty"`
	AvailabilityDomain   string           `json:"availabilityDomain"`
	PrimarySubnetID      string           `json:"primarySubnetId,omitempty"`
	ComputeClusterID     string           `json:"computeClusterId,omitempty"`
	FaultDomains         []string         `json:"faultDomains,omitempty"`
	SecondaryVnicSubnets []map[string]any `json:"secondaryVnicSubnets,omitempty"`
}

// InstancePool keeps a number of instances launched from an instance configuration.
type InstancePool struct {
	Base
	InstanceConfigurationID string             `json:"instanceConfigurationId"`
	DisplayNameFormatter    string             `json:"instanceDisplayNameFormatter,omitempty"`
	HostnameFormatter       string             `json:"instanceHostnameFormatter,omitempty"`
	Placements              []PoolPlacement    `json:"placementConfigurations"`
	Members                 []string           `json:"members,omitempty"`
	LoadBalancers           []PoolLoadBalancer `json:"loadBalancers,omitempty"`
	Size                    int                `json:"size"`
	CurrentSize             int                `json:"currentSize"`
	LaunchCount             int                `json:"launchCount,omitempty"`
}

func (v *InstanceConfiguration) vcnRef() string { return "" }
func (v *InstancePool) vcnRef() string          { return "" }

func (v *InstanceConfiguration) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *InstancePool) view() Resource {
	c := *v
	c.Internal, c.Members, c.LaunchCount = nil, nil, 0
	c.LoadBalancers = slices.Clone(v.LoadBalancers)

	for i := range c.LoadBalancers {
		c.LoadBalancers[i].Backends = nil
	}

	return &c
}

// CreateInstanceConfigurationIn is the CreateInstanceConfiguration request body (either shape).
type CreateInstanceConfigurationIn struct {
	TagsIn
	InstanceDetails map[string]any `json:"instanceDetails"`
	CompartmentID   string         `json:"compartmentId"`
	DisplayName     string         `json:"displayName"`
	InstanceID      string         `json:"instanceId"`
}

// configLaunch decodes the launchDetails of an instance configuration into a launch request.
func configLaunch(details map[string]any) (LaunchInstanceIn, *APIError) {
	var in LaunchInstanceIn

	raw, ok := details["launchDetails"].(map[string]any)
	if !ok {
		return in, errMissing("instanceDetails.launchDetails is required")
	}

	buf, err := json.Marshal(raw, jsonx.V1())
	if err == nil {
		err = json.Unmarshal(buf, &in, jsonx.V1())
	}

	if err != nil {
		return in, errParam("instanceDetails.launchDetails is malformed: " + err.Error())
	}

	return in, nil
}

func (b *InMemoryBackend) checkConfigDetails(details map[string]any) *APIError {
	if details["instanceType"] != "compute" {
		return errParam("instanceDetails.instanceType must be compute")
	}

	in, aerr := configLaunch(details)
	if aerr != nil {
		return aerr
	}

	sh, ok := shapeByName(in.Shape)
	if !ok {
		return errParam("Shape " + in.Shape + " is not a valid shape")
	}

	if _, aerr = sh.resolve(in.ShapeConfig); aerr != nil {
		return aerr
	}

	if aerr = checkPlatform(sh, in.PlatformConfig); aerr != nil {
		return aerr
	}

	if in.SourceDetails != nil && in.SourceDetails.SourceType == srcImage {
		if _, err := b.findImage(in.SourceDetails.ImageID); err != nil {
			return errNotFound(in.SourceDetails.ImageID)
		}
	}

	return nil
}

// detailsFromInstance builds instanceDetails that would relaunch an existing instance.
func (b *InMemoryBackend) detailsFromInstance(inst *Instance) map[string]any {
	launch := map[string]any{
		"availabilityDomain": inst.AvailabilityDomain, "shape": inst.Shape, "displayName": inst.DisplayName,
		"metadata": inst.Metadata,
	}

	if inst.ShapeConfig != nil {
		launch["shapeConfig"] = map[string]any{
			"ocpus":       inst.ShapeConfig.Ocpus,
			"memoryInGBs": inst.ShapeConfig.MemoryInGBs,
		}
	}

	if inst.SourceDetails != nil && inst.SourceDetails.SourceType == srcImage {
		launch["sourceDetails"] = map[string]any{"sourceType": srcImage, "imageId": inst.SourceDetails.ImageID}
	}

	if a := liveOf(b, func(x *VnicAttachment) bool { return x.InstanceID == inst.ID && x.NicIndex == 0 }); len(a) > 0 {
		launch["createVnicDetails"] = map[string]any{"subnetId": a[0].SubnetID}
	}

	return map[string]any{"instanceType": "compute", "launchDetails": launch}
}

// CreateInstanceConfiguration stores a launch template, given directly or taken from an instance.
func (b *InMemoryBackend) CreateInstanceConfiguration(in CreateInstanceConfigurationIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	details := in.InstanceDetails

	switch {
	case in.InstanceID != "" && details != nil:
		return nil, "", errParam("provide either instanceId or instanceDetails")
	case in.InstanceID != "":
		inst, aerr := getLive[*Instance](b, in.InstanceID)
		if aerr != nil {
			return nil, "", aerr
		}

		details = b.detailsFromInstance(inst)
	case details == nil:
		return nil, "", errMissing("instanceDetails is required")
	}

	if aerr := b.checkConfigDetails(details); aerr != nil {
		return nil, "", aerr
	}

	c := &InstanceConfiguration{
		Base:            b.newBase(kindInstanceConfig, in.CompartmentID, in.DisplayName, in.TagsIn),
		InstanceDetails: details,
	}
	b.ready(c)
	b.add(c)

	return c.view(), c.Internal.Etag, nil
}

// UpdateInstanceConfiguration changes the name and tags of a configuration.
func (b *InMemoryBackend) UpdateInstanceConfiguration(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*InstanceConfiguration) *APIError { return nil })
}

// DeleteInstanceConfiguration deletes a configuration no pool uses.
func (b *InMemoryBackend) DeleteInstanceConfiguration(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteInstanceConfiguration")
	defer b.mu.Unlock()

	c, aerr := get[*InstanceConfiguration](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(c) {
		return nil
	}

	if aerr = checkEtag(c, ifMatch); aerr != nil {
		return aerr
	}

	if p := firstID(liveOf(b, func(x *InstancePool) bool { return x.InstanceConfigurationID == id })); p != "" {
		return errConflict("The instance configuration " + id + " is used by instance pool " + p)
	}

	c.LifecycleState = statDeleted
	b.bump(c)
	delete(b.res, id)

	return nil
}

// LaunchInstanceConfiguration launches an instance from a configuration, with optional overriding details.
func (b *InMemoryBackend) LaunchInstanceConfiguration(
	id string, override map[string]any, token string,
) (Resource, string, *APIError) {
	return b.createOnce("launchInstanceConfiguration "+id, token, func() (Resource, string, *APIError) {
		c, aerr := getLive[*InstanceConfiguration](b, id)
		if aerr != nil {
			return nil, "", aerr
		}

		in, aerr := configLaunch(c.InstanceDetails)
		if aerr != nil {
			return nil, "", aerr
		}

		if override != nil {
			if oin, oerr := configLaunch(override); oerr == nil {
				in = mergeLaunch(in, oin)
			}
		}

		in.CompartmentID = cmpOrStr(in.CompartmentID, c.CompartmentID)

		return b.LaunchInstance(in)
	})
}

func mergeLaunch(base, over LaunchInstanceIn) LaunchInstanceIn {
	if over.AvailabilityDomain != "" {
		base.AvailabilityDomain = over.AvailabilityDomain
	}

	if over.CompartmentID != "" {
		base.CompartmentID = over.CompartmentID
	}

	if over.DisplayName != "" {
		base.DisplayName = over.DisplayName
	}

	if over.Shape != "" {
		base.Shape, base.ShapeConfig = over.Shape, over.ShapeConfig
	}

	if over.CreateVnicDetails != nil {
		base.CreateVnicDetails = over.CreateVnicDetails
	}

	return base
}

func checkFormatter(name, f string) *APIError {
	if f != "" && !strings.Contains(f, launchToken) {
		return errParam(name + " must contain " + launchToken)
	}

	return nil
}

// CreateInstancePoolIn is the CreateInstancePool request body.
type CreateInstancePoolIn struct {
	TagsIn
	Size                    *int                   `json:"size"`
	LifecycleManagement     map[string]any         `json:"lifecycleManagement"`
	CompartmentID           string                 `json:"compartmentId"`
	DisplayName             string                 `json:"displayName"`
	InstanceConfigurationID string                 `json:"instanceConfigurationId"`
	InstanceDisplayNameFmt  string                 `json:"instanceDisplayNameFormatter"`
	InstanceHostnameFmt     string                 `json:"instanceHostnameFormatter"`
	LoadBalancers           []AttachLoadBalancerIn `json:"loadBalancers"`
	Placements              []PoolPlacement        `json:"placementConfigurations"`
}

func (b *InMemoryBackend) checkPlacements(ps []PoolPlacement) *APIError {
	if len(ps) == 0 {
		return errMissing("placementConfigurations is required")
	}

	seen := map[string]bool{}

	for _, p := range ps {
		subnet := p.PrimarySubnetID
		if subnet == "" && p.PrimaryVnicSubnets != nil {
			subnet, _ = p.PrimaryVnicSubnets["subnetId"].(string)
		}

		switch {
		case !adRe.MatchString(p.AvailabilityDomain):
			return errParam("availabilityDomain " + p.AvailabilityDomain + " is not a valid availability domain")
		case seen[p.AvailabilityDomain]:
			return errParam("placementConfigurations lists " + p.AvailabilityDomain + " twice")
		case subnet == "":
			return errMissing("primarySubnetId is required in every placement configuration")
		}

		seen[p.AvailabilityDomain] = true

		s, aerr := getLive[*Subnet](b, subnet)
		if aerr != nil {
			return aerr
		}

		if s.AvailabilityDomain != nil && *s.AvailabilityDomain != p.AvailabilityDomain {
			return errParam("The subnet " + s.ID + " is specific to availability domain " + *s.AvailabilityDomain)
		}

		for _, fd := range p.FaultDomains {
			if !faultDomainRe.MatchString(fd) {
				return errParam("faultDomains must be FAULT-DOMAIN-1, FAULT-DOMAIN-2 or FAULT-DOMAIN-3")
			}
		}
	}

	return nil
}

func (p PoolPlacement) subnet() string {
	if p.PrimarySubnetID != "" {
		return p.PrimarySubnetID
	}

	s, _ := p.PrimaryVnicSubnets["subnetId"].(string)

	return s
}

// CreateInstancePool creates a pool and launches its instances.
func (b *InMemoryBackend) CreateInstancePool(in CreateInstancePoolIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.LifecycleManagement != nil {
		return nil, "", errParam("lifecycleManagement is not supported by instance pools here")
	}

	if in.Size == nil || *in.Size < 0 || *in.Size > maxPoolSize {
		return nil, "", errParam("size must be between 0 and 1000")
	}

	for name, f := range map[string]string{
		"instanceDisplayNameFormatter": in.InstanceDisplayNameFmt, "instanceHostnameFormatter": in.InstanceHostnameFmt,
	} {
		if aerr := checkFormatter(name, f); aerr != nil {
			return nil, "", aerr
		}
	}

	cfg, aerr := getLive[*InstanceConfiguration](b, in.InstanceConfigurationID)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = b.checkPlacements(in.Placements); aerr != nil {
		return nil, "", aerr
	}

	p := &InstancePool{
		Base:                    b.newBase(kindInstancePool, in.CompartmentID, in.DisplayName, in.TagsIn),
		InstanceConfigurationID: in.InstanceConfigurationID, Placements: in.Placements, Size: *in.Size,
		DisplayNameFormatter: in.InstanceDisplayNameFmt, HostnameFormatter: in.InstanceHostnameFmt,
	}

	if aerr = b.newPoolAttachments(p, cfg, in.LoadBalancers); aerr != nil {
		return nil, "", aerr
	}

	b.moveTo(p, poolProvisioning, poolRunning)
	b.add(p)
	b.reconcilePool(p)

	return p.view(), p.Internal.Etag, nil
}

// poolMembers returns the live instances of a pool, oldest first.
func (b *InMemoryBackend) poolMembers(p *InstancePool) []*Instance {
	var out []*Instance

	for _, id := range p.Members {
		if i, ok := b.res[id].(*Instance); ok && live(i) {
			out = append(out, i)
		}
	}

	slices.SortFunc(out, func(a, c *Instance) int { return int(a.Internal.Seq - c.Internal.Seq) })

	return out
}

// reconcilePool launches or terminates instances until the pool has its configured size.
func (b *InMemoryBackend) reconcilePool(p *InstancePool) {
	cfg, ok := b.res[p.InstanceConfigurationID].(*InstanceConfiguration)
	if !ok {
		return
	}

	members := b.poolMembers(p)
	p.Members = nil

	for _, m := range members {
		p.Members = append(p.Members, m.ID)
	}

	for len(members) < p.Size {
		i, aerr := b.launchPoolInstance(p, cfg, members)
		if aerr != nil {
			break
		}

		members = append(members, i)
		p.Members = append(p.Members, i.ID)
	}

	for len(members) > p.Size {
		victim := b.scaleInVictim(p, members)
		_ = b.terminateLocked(victim.ID, "", TerminateOpts{PreserveLaunchData: true})
		members = slices.DeleteFunc(members, func(m *Instance) bool { return m.ID == victim.ID })
	}

	p.Members = p.Members[:0]
	for _, m := range members {
		p.Members = append(p.Members, m.ID)
	}

	p.CurrentSize = len(members)
	b.bump(p)
}

// scaleInVictim picks the oldest instance of the availability domain that holds the most pool instances.
func (b *InMemoryBackend) scaleInVictim(_ *InstancePool, members []*Instance) *Instance {
	per := map[string]int{}
	for _, m := range members {
		per[m.AvailabilityDomain]++
	}

	best := members[0]
	for _, m := range members {
		if per[m.AvailabilityDomain] > per[best.AvailabilityDomain] {
			best = m
		}
	}

	return best
}

func (b *InMemoryBackend) launchPoolInstance(
	p *InstancePool,
	cfg *InstanceConfiguration,
	members []*Instance,
) (*Instance, *APIError) {
	in, aerr := configLaunch(cfg.InstanceDetails)
	if aerr != nil {
		return nil, aerr
	}

	count := map[string]int{}
	for _, m := range members {
		count[m.AvailabilityDomain]++
	}

	pl := p.Placements[0]
	for _, c := range p.Placements {
		if count[c.AvailabilityDomain] < count[pl.AvailabilityDomain] {
			pl = c
		}
	}

	p.LaunchCount++
	n := strconv.Itoa(p.LaunchCount)

	in.CompartmentID, in.AvailabilityDomain = p.CompartmentID, pl.AvailabilityDomain
	vd := CreateVnicIn{}

	if in.CreateVnicDetails != nil {
		vd = *in.CreateVnicDetails
	}

	vd.SubnetID = pl.subnet()
	in.CreateVnicDetails = &vd
	in.SubnetID = ""

	if len(pl.FaultDomains) > 0 {
		in.FaultDomain = pl.FaultDomains[(p.LaunchCount-1)%len(pl.FaultDomains)]
	}

	if p.DisplayNameFormatter != "" {
		in.DisplayName = strings.ReplaceAll(p.DisplayNameFormatter, launchToken, n)
	} else {
		in.DisplayName = p.DisplayName + "-" + n
	}

	if p.HostnameFormatter != "" {
		vd.HostnameLabel = strings.ReplaceAll(p.HostnameFormatter, launchToken, n)
	}

	r, _, aerr := b.LaunchInstance(in)
	if aerr != nil {
		return nil, aerr
	}

	inst, _ := b.res[r.base().ID].(*Instance)
	inst.Internal.PoolID = p.ID

	if aerr = b.attachWorkerVnics(inst, configSecondaryVnics(cfg)); aerr != nil {
		_ = b.terminateLocked(inst.ID, "", TerminateOpts{})

		return nil, aerr
	}

	return inst, nil
}

// configSecondaryVnics reads the secondaryVnics of an instance configuration.
func configSecondaryVnics(cfg *InstanceConfiguration) []WorkerVnic {
	var out []WorkerVnic

	list, _ := cfg.InstanceDetails["secondaryVnics"].([]any)
	for _, v := range list {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}

		w := WorkerVnic{}
		w.DisplayName, _ = m["displayName"].(string)
		w.Details, _ = m["createVnicDetails"].(map[string]any)

		if n, isNum := m["nicIndex"].(float64); isNum {
			idx := int(n)
			w.NicIndex = &idx
		}

		out = append(out, w)
	}

	return out
}

// UpdateInstancePoolIn is the UpdateInstancePool request body.
type UpdateInstancePoolIn struct {
	UpdateIn
	Size                    *int             `json:"size"`
	InstanceConfigurationID *string          `json:"instanceConfigurationId"`
	Placements              *[]PoolPlacement `json:"placementConfigurations"`
	DisplayNameFormatter    *string          `json:"instanceDisplayNameFormatter"`
	HostnameFormatter       *string          `json:"instanceHostnameFormatter"`
}

// UpdateInstancePool changes the size, configuration, placement and formatters of a pool.
func (b *InMemoryBackend) UpdateInstancePool(
	id, ifMatch string,
	in UpdateInstancePoolIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(p *InstancePool) *APIError {
		if p.LifecycleState != poolRunning && p.LifecycleState != poolStopped {
			return errState("The instance pool " + p.ID + " is " + p.LifecycleState)
		}

		next := *p

		if aerr := b.applyPoolUpdate(&next, in); aerr != nil {
			return aerr
		}

		resize := next.Size != p.Size || in.Placements != nil || in.InstanceConfigurationID != nil
		*p = next

		if resize && p.LifecycleState == poolRunning {
			b.moveTo(p, poolScaling, poolRunning)
			b.reconcilePool(p)
		}

		return nil
	})
}

func (b *InMemoryBackend) applyPoolUpdate(p *InstancePool, in UpdateInstancePoolIn) *APIError {
	if in.Size != nil {
		if *in.Size < 0 || *in.Size > maxPoolSize {
			return errParam("size must be between 0 and 1000")
		}

		p.Size = *in.Size
	}

	if in.InstanceConfigurationID != nil {
		if _, aerr := getLive[*InstanceConfiguration](b, *in.InstanceConfigurationID); aerr != nil {
			return aerr
		}

		p.InstanceConfigurationID = *in.InstanceConfigurationID
	}

	if in.Placements != nil {
		if aerr := b.checkPlacements(*in.Placements); aerr != nil {
			return aerr
		}

		p.Placements = *in.Placements
	}

	for name, f := range map[string]*string{
		"instanceDisplayNameFormatter": in.DisplayNameFormatter, "instanceHostnameFormatter": in.HostnameFormatter,
	} {
		if f != nil {
			if aerr := checkFormatter(name, *f); aerr != nil {
				return aerr
			}
		}
	}

	if in.DisplayNameFormatter != nil {
		p.DisplayNameFormatter = *in.DisplayNameFormatter
	}

	if in.HostnameFormatter != nil {
		p.HostnameFormatter = *in.HostnameFormatter
	}

	return nil
}

// TerminateInstancePool terminates the pool and its instances.
func (b *InMemoryBackend) TerminateInstancePool(id, ifMatch string) *APIError {
	aerr := b.terminateInstancePool(id, ifMatch)
	if aerr == nil {
		b.syncLB()
	}

	return aerr
}

func (b *InMemoryBackend) terminateInstancePool(id, ifMatch string) *APIError {
	b.mu.Lock("TerminateInstancePool")
	defer b.mu.Unlock()

	p, aerr := get[*InstancePool](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(p) {
		return nil
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return aerr
	}

	for _, m := range b.poolMembers(p) {
		_ = b.terminateLocked(m.ID, "", TerminateOpts{PreserveLaunchData: true})
	}

	p.CurrentSize = 0
	b.moveTo(p, statTerminating, statTerminated)

	return nil
}

// poolPower applies a power action to every instance of a pool.
func (b *InMemoryBackend) poolPower(id, ifMatch, action, interim, final string) (Resource, string, *APIError) {
	b.mu.Lock("InstancePoolAction")
	defer b.mu.Unlock()

	p, aerr := getLive[*InstancePool](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	want := poolRunning
	if action == "START" {
		want = poolStopped
	}

	if p.LifecycleState != want {
		return nil, "", errState(
			"The instance pool " + id + " is " + p.LifecycleState + "; " + action + " needs " + want,
		)
	}

	for _, m := range b.poolMembers(p) {
		_, _, _ = b.instanceActionLocked(m.ID, action, "")
	}

	if interim != "" {
		b.moveTo(p, interim, final)
	} else {
		b.bump(p)
	}

	return p.view(), p.Internal.Etag, nil
}

func poolActionOp(action, interim, final string) opFunc {
	return func(h *Handler, q *req) error {
		v, etag, aerr := h.Backend.poolPower(q.param("instancePoolId"), q.ifMatch(), action, interim, final)

		return send(q, v, etag, aerr)
	}
}

// PoolInstance is an InstancePoolInstance (and InstanceSummary) of the pool APIs.
type PoolInstance struct {
	TimeCreated             any    `json:"timeCreated"`
	ID                      string `json:"id"`
	InstancePoolID          string `json:"instancePoolId,omitempty"`
	AvailabilityDomain      string `json:"availabilityDomain"`
	CompartmentID           string `json:"compartmentId"`
	InstanceConfigurationID string `json:"instanceConfigurationId"`
	LifecycleState          string `json:"lifecycleState"`
	Region                  string `json:"region"`
	Shape                   string `json:"shape"`
	State                   string `json:"state"`
	DisplayName             string `json:"displayName"`
	FaultDomain             string `json:"faultDomain"`
}

func (b *InMemoryBackend) poolInstance(p *InstancePool, i *Instance) PoolInstance {
	return PoolInstance{
		TimeCreated:             i.TimeCreated,
		ID:                      i.ID,
		InstancePoolID:          p.ID,
		AvailabilityDomain:      i.AvailabilityDomain,
		CompartmentID:           i.CompartmentID,
		InstanceConfigurationID: p.InstanceConfigurationID,
		LifecycleState:          "ACTIVE",
		Region:                  i.Region,
		Shape:                   i.Shape,
		State:                   i.LifecycleState,
		DisplayName:             i.DisplayName,
		FaultDomain:             i.FaultDomain,
	}
}

func (b *InMemoryBackend) listPoolInstances(id string) ([]PoolInstance, *APIError) {
	b.mu.RLock("ListInstancePoolInstances")
	defer b.mu.RUnlock()

	p, aerr := get[*InstancePool](b, id)
	if aerr != nil {
		return nil, aerr
	}

	out := []PoolInstance{}

	for _, m := range b.poolMembers(p) {
		out = append(out, b.poolInstance(p, m))
	}

	return out, nil
}

// AttachPoolInstance adds an existing instance to a pool and grows the pool by one.
func (b *InMemoryBackend) AttachPoolInstance(poolID, instID string) (PoolInstance, *APIError) {
	out, aerr := b.attachPoolInstance(poolID, instID)
	if aerr == nil {
		b.syncLB()
	}

	return out, aerr
}

func (b *InMemoryBackend) attachPoolInstance(poolID, instID string) (PoolInstance, *APIError) {
	b.mu.Lock("AttachInstancePoolInstance")
	defer b.mu.Unlock()

	p, aerr := getLive[*InstancePool](b, poolID)
	if aerr != nil {
		return PoolInstance{}, aerr
	}

	inst, aerr := getLive[*Instance](b, instID)
	if aerr != nil {
		return PoolInstance{}, aerr
	}

	switch {
	case inst.Internal.PoolID != "":
		return PoolInstance{}, errConflict(
			"The instance " + instID + " already belongs to instance pool " + inst.Internal.PoolID,
		)
	case p.Size >= maxPoolSize:
		return PoolInstance{}, errLimit("An instance pool can have at most 1000 instances")
	case !slices.ContainsFunc(p.Placements, func(c PoolPlacement) bool {
		return c.AvailabilityDomain == inst.AvailabilityDomain
	}):
		return PoolInstance{}, errParam(
			"The instance is in an availability domain the pool does not place instances in",
		)
	case inst.CompartmentID != p.CompartmentID:
		return PoolInstance{}, errParam("The instance must be in the compartment of the pool")
	}

	inst.Internal.PoolID = p.ID
	p.Members = append(p.Members, inst.ID)
	p.Size++
	p.CurrentSize = len(b.poolMembers(p))
	b.bump(p)

	return b.poolInstance(p, inst), nil
}

// DetachPoolInstance removes an instance from a pool, optionally shrinking the pool and terminating it.
func (b *InMemoryBackend) DetachPoolInstance(poolID, instID string, decrement, terminate bool) (string, *APIError) {
	wr, aerr := b.detachPoolInstance(poolID, instID, decrement, terminate)
	if aerr == nil {
		b.syncLB()
	}

	return wr, aerr
}

func (b *InMemoryBackend) detachPoolInstance(poolID, instID string, decrement, terminate bool) (string, *APIError) {
	b.mu.Lock("DetachInstancePoolInstance")
	defer b.mu.Unlock()

	p, aerr := getLive[*InstancePool](b, poolID)
	if aerr != nil {
		return "", aerr
	}

	if !slices.Contains(p.Members, instID) {
		return "", errNotFound(instID)
	}

	inst, _ := b.res[instID].(*Instance)
	inst.Internal.PoolID = ""
	p.Members = slices.DeleteFunc(slices.Clone(p.Members), func(id string) bool { return id == instID })

	if decrement {
		p.Size = max(0, p.Size-1)
	}

	if terminate {
		_ = b.terminateLocked(instID, "", TerminateOpts{PreserveLaunchData: true})
	}

	if p.LifecycleState == poolRunning {
		b.reconcilePool(p)
	}

	return b.workFor("DETACH_INSTANCE_POOL_INSTANCE", p.CompartmentID, "instancepool", p.ID, "/instancePools/"), nil
}

// sweepPools keeps running pools at their size, replacing terminated instances.
func (b *InMemoryBackend) sweepPools() {
	for _, r := range b.res {
		if p, ok := r.(*InstancePool); ok && p.LifecycleState == poolRunning &&
			len(b.poolMembers(p)) != len(p.Members) {
			b.reconcilePool(p)
		}
	}
}

func poolInstancesOp(h *Handler, q *req) error {
	out, aerr := h.Backend.listPoolInstances(q.param("instancePoolId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func getPoolInstanceOp(h *Handler, q *req) error {
	out, aerr := h.Backend.listPoolInstances(q.param("instancePoolId"))
	if aerr != nil {
		return aerr
	}

	for _, i := range out {
		if i.ID == q.param("instanceId") {
			return q.json(http.StatusOK, i, "")
		}
	}

	return errNotFound(q.param("instanceId"))
}

func attachPoolInstanceOp(h *Handler, q *req) error {
	var in struct {
		InstanceID string `json:"instanceId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	out, aerr := h.Backend.AttachPoolInstance(q.param("instancePoolId"), in.InstanceID)
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func detachPoolInstanceOp(h *Handler, q *req) error {
	var in struct {
		IsDecrementSize *bool  `json:"isDecrementSize"`
		IsAutoTerminate *bool  `json:"isAutoTerminate"`
		InstanceID      string `json:"instanceId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.DetachPoolInstance(
		q.param("instancePoolId"),
		in.InstanceID,
		flag(in.IsDecrementSize),
		flag(in.IsAutoTerminate),
	)
	if aerr != nil {
		return aerr
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, struct{}{}, "")
}

func launchConfigOp(h *Handler, q *req) error {
	var over map[string]any
	if len(q.body) > 0 {
		if err := q.decode(&over); err != nil {
			return err
		}
	}

	v, etag, aerr := h.Backend.LaunchInstanceConfiguration(
		q.param("instanceConfigurationId"), over, q.r.Header.Get(headerRetry),
	)

	return send(q, v, etag, aerr)
}

func instancePoolEntries() []entry {
	ic, ip := idPath("/instanceConfigurations", "instanceConfigurationId"), idPath("/instancePools", "instancePoolId")

	return []entry{
		{
			createOp((*InMemoryBackend).CreateInstanceConfiguration), http.MethodPost, "/instanceConfigurations",
			"CreateInstanceConfiguration",
		},
		{listOp[*InstanceConfiguration](nil), http.MethodGet, "/instanceConfigurations", "ListInstanceConfigurations"},
		{getOp[*InstanceConfiguration]("instanceConfigurationId"), http.MethodGet, ic, "GetInstanceConfiguration"},
		{
			updateOp("instanceConfigurationId", (*InMemoryBackend).UpdateInstanceConfiguration), http.MethodPut, ic,
			"UpdateInstanceConfiguration",
		},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.DeleteInstanceConfiguration(q.param("instanceConfigurationId"), q.ifMatch()))
		}, http.MethodDelete, ic, "DeleteInstanceConfiguration"},
		{
			changeOp[*InstanceConfiguration](
				"instanceConfigurationId",
			),
			http.MethodPost,
			ic + "/actions/changeCompartment",
			"ChangeInstanceConfigurationCompartment",
		},
		{launchConfigOp, http.MethodPost, ic + "/actions/launch", "LaunchInstanceConfiguration"},
		{createPoolOp, http.MethodPost, "/instancePools", "CreateInstancePool"},
		{listOp[*InstancePool](nil), http.MethodGet, "/instancePools", "ListInstancePools"},
		{getOp[*InstancePool]("instancePoolId"), http.MethodGet, ip, "GetInstancePool"},
		{updateOp("instancePoolId", (*InMemoryBackend).updatePoolAndSync), http.MethodPut, ip, "UpdateInstancePool"},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.TerminateInstancePool(q.param("instancePoolId"), q.ifMatch()))
		}, http.MethodDelete, ip, "TerminateInstancePool"},
		{
			changeOp[*InstancePool]("instancePoolId"), http.MethodPost, ip + "/actions/changeCompartment",
			"ChangeInstancePoolCompartment",
		},
		{poolActionOp("START", poolStarting, poolRunning), http.MethodPost, ip + "/actions/start", "StartInstancePool"},
		{poolActionOp("STOP", poolStopping, poolStopped), http.MethodPost, ip + "/actions/stop", "StopInstancePool"},
		{
			poolActionOp("SOFTSTOP", poolStopping, poolStopped), http.MethodPost, ip + "/actions/softstop",
			"SoftstopInstancePool",
		},
		{poolActionOp("RESET", "", ""), http.MethodPost, ip + "/actions/reset", "ResetInstancePool"},
		{poolActionOp("SOFTRESET", "", ""), http.MethodPost, ip + "/actions/softreset", "SoftresetInstancePool"},
		{poolInstancesOp, http.MethodGet, ip + "/instances", "ListInstancePoolInstances"},
		{getPoolInstanceOp, http.MethodGet, ip + "/instances/{instanceId}", "GetInstancePoolInstance"},
		{attachPoolInstanceOp, http.MethodPost, ip + "/instances", "AttachInstancePoolInstance"},
		{detachPoolInstanceOp, http.MethodPost, ip + "/actions/detachInstance", "DetachInstancePoolInstance"},
		{attachLoadBalancerOp, http.MethodPost, ip + "/actions/attachLoadBalancer", "AttachLoadBalancer"},
		{detachLoadBalancerOp, http.MethodPost, ip + "/actions/detachLoadBalancer", "DetachLoadBalancer"},
		{
			getPoolLoadBalancerOp, http.MethodGet,
			idPath(ip+"/loadBalancerAttachments", "instancePoolLoadBalancerAttachmentId"),
			"GetInstancePoolLoadBalancerAttachment",
		},
	}
}

func (b *InMemoryBackend) updatePoolAndSync(id, ifMatch string, in UpdateInstancePoolIn) (Resource, string, *APIError) {
	v, etag, aerr := b.UpdateInstancePool(id, ifMatch, in)
	if aerr == nil {
		b.syncLB()
	}

	return v, etag, aerr
}

func createPoolOp(h *Handler, q *req) error {
	var in CreateInstancePoolIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if aerr := h.Backend.checkTags(in); aerr != nil {
		return aerr
	}

	if aerr := h.Backend.checkLoadBalancerTargets(in.LoadBalancers); aerr != nil {
		return aerr
	}

	scope, token := q.r.Method+" "+q.r.URL.Path, q.r.Header.Get(headerRetry)

	v, etag, aerr := h.Backend.createOnce(scope, token, func() (Resource, string, *APIError) {
		return h.Backend.CreateInstancePool(in)
	})
	if aerr == nil {
		h.Backend.syncLB()
	}

	return send(q, v, etag, aerr)
}
