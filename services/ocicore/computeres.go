package ocicore

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	kindCapacityRes = "capacityreservation"
	kindDvh         = "dedicatedvmhost"
	kindCluster     = "computecluster"

	opCreateReservation = "CREATE_COMPUTE_CAPACITY_RESERVATION"
	opDeleteReservation = "DELETE_COMPUTE_CAPACITY_RESERVATION"
	opCreateDvh         = "CREATE_DEDICATED_VM_HOST"
	opDeleteDvh         = "DELETE_DEDICATED_VM_HOST"
	dvhSockets          = 2
)

// ReservationShapeConfig is the OCPU and memory of a reserved flexible shape.
type ReservationShapeConfig struct {
	Ocpus              *float64 `json:"ocpus,omitempty"`
	MemoryInGBs        *float64 `json:"memoryInGBs,omitempty"`
	ResourceManagement string   `json:"resourceManagement,omitempty"`
}

// ReservationConfig is one instanceReservationConfigs entry.
type ReservationConfig struct {
	InstanceShapeConfig *ReservationShapeConfig `json:"instanceShapeConfig,omitempty"`
	ClusterConfig       map[string]any          `json:"clusterConfig,omitempty"`
	InstanceShape       string                  `json:"instanceShape"`
	FaultDomain         string                  `json:"faultDomain,omitempty"`
	ReservedCount       int64                   `json:"reservedCount"`
	UsedCount           int64                   `json:"usedCount"`
}

// CapacityReservation reserves instance capacity in an availability domain; no capacity is physically held.
type CapacityReservation struct {
	TimeUpdated *time.Time `json:"timeUpdated,omitempty"`
	Base
	AvailabilityDomain   string              `json:"availabilityDomain"`
	Configs              []ReservationConfig `json:"instanceReservationConfigs"`
	ReservedInstanceCnt  int64               `json:"reservedInstanceCount"`
	UsedInstanceCnt      int64               `json:"usedInstanceCount"`
	IsDefaultReservation bool                `json:"isDefaultReservation"`
}

// DedicatedVMHost is a dedicated VM host with two sockets; instances are placed on one socket each.
type DedicatedVMHost struct {
	PlacementConstraintDetails map[string]any `json:"placementConstraintDetails,omitempty"`
	Base
	AvailabilityDomain   string  `json:"availabilityDomain"`
	DedicatedVMHostShape string  `json:"dedicatedVmHostShape"`
	FaultDomain          string  `json:"faultDomain,omitempty"`
	CapacityConfig       string  `json:"capacityConfig,omitempty"`
	TotalOcpus           float64 `json:"totalOcpus"`
	RemainingOcpus       float64 `json:"remainingOcpus"`
	TotalMemoryInGBs     float64 `json:"totalMemoryInGBs"`
	RemainingMemoryInGBs float64 `json:"remainingMemoryInGBs"`
	IsMemoryEncryption   bool    `json:"isMemoryEncryptionEnabled"`
	IsBurstable          bool    `json:"isBurstable"`
}

// ComputeCluster groups RDMA bare metal instances.
type ComputeCluster struct {
	TimeUpdated                *time.Time     `json:"timeUpdated,omitempty"`
	PlacementConstraintDetails map[string]any `json:"placementConstraintDetails,omitempty"`
	Base
	AvailabilityDomain string `json:"availabilityDomain"`
}

type dvhShape struct {
	name      string
	series    string
	usableOc  float64
	usableMem float64
}

// dvhShapes follows the Dedicated Virtual Machine Host Shapes table of the compute shapes documentation.
func dvhShapes() []dvhShape {
	return []dvhShape{
		{"DVH.Standard2.52", "VM.Standard2.", 48, 736},
		{"DVH.Standard3.64", "VM.Standard3.", 60, 960},
		{"DVH.Standard.E2.64", "VM.Standard.E2.", 59, 480},
		{"DVH.Standard.E3.128", "VM.Standard.E3.", 124, 1912},
		{"DVH.Standard.E4.128", "VM.Standard.E4.", 124, 1912},
		{"DVH.Standard.E5.192", "VM.Standard.E5.", 188, 2098},
		{"DVH.DenseIO2.52", "VM.DenseIO2.", 48, 736},
		{"DVH.Optimized3.36", "VM.Optimized3.", 34, 472},
	}
}

func dvhShapeByName(n string) (dvhShape, bool) {
	i := slices.IndexFunc(dvhShapes(), func(s dvhShape) bool { return s.name == n })
	if i < 0 {
		return dvhShape{}, false
	}

	return dvhShapes()[i], true
}

func (v *CapacityReservation) vcnRef() string { return "" }
func (v *DedicatedVMHost) vcnRef() string     { return "" }
func (v *ComputeCluster) vcnRef() string      { return "" }

func (v *CapacityReservation) view() Resource {
	c := *v
	c.Internal = nil
	c.Configs = slices.Clone(v.Configs)

	return &c
}

func (v *DedicatedVMHost) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *ComputeCluster) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

// CreateReservationIn is the CreateComputeCapacityReservation request body.
type CreateReservationIn struct {
	TagsIn
	IsDefaultReservation *bool               `json:"isDefaultReservation"`
	CompartmentID        string              `json:"compartmentId"`
	AvailabilityDomain   string              `json:"availabilityDomain"`
	DisplayName          string              `json:"displayName"`
	Configs              []ReservationConfig `json:"instanceReservationConfigs"`
}

func (b *InMemoryBackend) checkReservationConfigs(cfgs []ReservationConfig) ([]ReservationConfig, *APIError) {
	out := make([]ReservationConfig, 0, len(cfgs))

	for _, c := range cfgs {
		sh, ok := shapeByName(c.InstanceShape)
		switch {
		case !ok:
			return nil, errParam("instanceShape " + c.InstanceShape + " is not a valid shape")
		case c.ReservedCount < 1:
			return nil, errParam("reservedCount must be at least 1")
		case c.FaultDomain != "" && !faultDomainRe.MatchString(c.FaultDomain):
			return nil, errParam("faultDomain must be FAULT-DOMAIN-1, FAULT-DOMAIN-2 or FAULT-DOMAIN-3")
		}

		var in *ShapeConfigIn
		if c.InstanceShapeConfig != nil {
			in = &ShapeConfigIn{Ocpus: c.InstanceShapeConfig.Ocpus, MemoryInGBs: c.InstanceShapeConfig.MemoryInGBs}
		}

		resolved, aerr := sh.resolve(in)
		if aerr != nil {
			return nil, aerr
		}

		if sh.flexible {
			c.InstanceShapeConfig = &ReservationShapeConfig{
				Ocpus: &resolved.Ocpus, MemoryInGBs: &resolved.MemoryInGBs, ResourceManagement: "STATIC",
			}
		}

		c.UsedCount = 0
		out = append(out, c)
	}

	return out, nil
}

func reservedTotal(cfgs []ReservationConfig) int64 {
	var n int64

	for _, c := range cfgs {
		n += c.ReservedCount
	}

	return n
}

// CreateComputeCapacityReservation creates a reservation.
func (b *InMemoryBackend) CreateComputeCapacityReservation(in CreateReservationIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if !adRe.MatchString(in.AvailabilityDomain) {
		return nil, "", errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	cfgs, aerr := b.checkReservationConfigs(in.Configs)
	if aerr != nil {
		return nil, "", aerr
	}

	r := &CapacityReservation{
		Base:               b.newBase(kindCapacityRes, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain: in.AvailabilityDomain, Configs: cfgs, ReservedInstanceCnt: reservedTotal(cfgs),
		IsDefaultReservation: flag(in.IsDefaultReservation),
	}
	b.moveTo(r, statCreating, byoipActive)
	b.add(r)

	return r.view(), r.Internal.Etag, nil
}

// UpdateReservationIn is the UpdateComputeCapacityReservation request body.
type UpdateReservationIn struct {
	UpdateIn
	IsDefaultReservation *bool                `json:"isDefaultReservation"`
	Configs              *[]ReservationConfig `json:"instanceReservationConfigs"`
}

// UpdateComputeCapacityReservation renames a reservation or replaces its configurations.
func (b *InMemoryBackend) UpdateComputeCapacityReservation(
	id, ifMatch string, in UpdateReservationIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(r *CapacityReservation) *APIError {
		if in.IsDefaultReservation != nil {
			r.IsDefaultReservation = *in.IsDefaultReservation
		}

		if in.Configs == nil {
			return nil
		}

		cfgs, aerr := b.checkReservationConfigs(*in.Configs)
		if aerr != nil {
			return aerr
		}

		for _, old := range r.Configs {
			if old.UsedCount > 0 && !slices.ContainsFunc(cfgs, func(c ReservationConfig) bool {
				return sameReservation(c, old) && c.ReservedCount >= old.UsedCount
			}) {
				return errConflict(
					"Instances use the reservation of " + old.InstanceShape + "; keep at least that many",
				)
			}
		}

		r.Configs, r.ReservedInstanceCnt = cfgs, reservedTotal(cfgs)
		at := b.nowUTC()
		r.TimeUpdated = &at

		return nil
	})
}

func sameReservation(a, c ReservationConfig) bool {
	if a.InstanceShape != c.InstanceShape || a.FaultDomain != c.FaultDomain {
		return false
	}

	if a.InstanceShapeConfig == nil || c.InstanceShapeConfig == nil {
		return a.InstanceShapeConfig == c.InstanceShapeConfig
	}

	return *a.InstanceShapeConfig.Ocpus == *c.InstanceShapeConfig.Ocpus &&
		*a.InstanceShapeConfig.MemoryInGBs == *c.InstanceShapeConfig.MemoryInGBs
}

// DeleteComputeCapacityReservation deletes a reservation no instance uses.
func (b *InMemoryBackend) DeleteComputeCapacityReservation(id, ifMatch string) (string, *APIError) {
	b.mu.Lock("DeleteComputeCapacityReservation")
	defer b.mu.Unlock()

	r, aerr := get[*CapacityReservation](b, id)
	if aerr != nil {
		return "", aerr
	}

	if !live(r) || r.LifecycleState == statDeleted {
		return b.workFor(
			opDeleteReservation,
			r.CompartmentID,
			"computecapacityreservation",
			id,
			"/computeCapacityReservations/",
		), nil
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return "", aerr
	}

	if r.UsedInstanceCnt > 0 {
		return "", errConflict("The capacity reservation " + id + " is used by instances")
	}

	b.moveTo(r, statDeleting, statDeleted)

	return b.workFor(
		opDeleteReservation,
		r.CompartmentID,
		"computecapacityreservation",
		id,
		"/computeCapacityReservations/",
	), nil
}

func (b *InMemoryBackend) workFor(op, comp, entity, id, path string) string {
	return b.newWorkRequest(op, comp, []WorkResource{{
		ActionType: wrActionUpdated, EntityType: entity, Identifier: id, EntityURI: apiBase + path + id,
	}})
}

// recordWork records a work request for a resource that was just created.
func (b *InMemoryBackend) recordWork(op, comp, entity, id, path string) string {
	b.mu.Lock("recordWork")
	defer b.mu.Unlock()

	return b.workFor(op, comp, entity, id, path)
}

// syncCompute recomputes the used and remaining counters of reservations and dedicated hosts from the instances.
func (b *InMemoryBackend) syncCompute() {
	insts := liveOf(b, func(i *Instance) bool { return i.CapacityReservationID != "" || i.DedicatedVMHostID != "" })

	for _, r := range b.res {
		switch v := r.(type) {
		case *CapacityReservation:
			v.syncUsed(insts)
		case *DedicatedVMHost:
			v.syncRemaining(insts)
		}
	}
}

func (v *CapacityReservation) syncUsed(insts []*Instance) {
	v.UsedInstanceCnt = 0

	for i := range v.Configs {
		v.Configs[i].UsedCount = 0
	}

	for _, in := range insts {
		if in.CapacityReservationID != v.ID {
			continue
		}

		if i := slices.IndexFunc(v.Configs, func(c ReservationConfig) bool { return in.fits(c) }); i >= 0 {
			v.Configs[i].UsedCount++
			v.UsedInstanceCnt++
		}
	}
}

func (i *Instance) fits(c ReservationConfig) bool {
	if i.Shape != c.InstanceShape || (c.FaultDomain != "" && c.FaultDomain != i.FaultDomain) {
		return false
	}

	return c.InstanceShapeConfig == nil ||
		(i.ShapeConfig != nil && *c.InstanceShapeConfig.Ocpus == i.ShapeConfig.Ocpus &&
			*c.InstanceShapeConfig.MemoryInGBs == i.ShapeConfig.MemoryInGBs)
}

func (v *DedicatedVMHost) syncRemaining(insts []*Instance) {
	v.RemainingOcpus, v.RemainingMemoryInGBs = v.TotalOcpus, v.TotalMemoryInGBs

	for _, in := range insts {
		if in.DedicatedVMHostID == v.ID && in.ShapeConfig != nil {
			v.RemainingOcpus -= in.ShapeConfig.Ocpus
			v.RemainingMemoryInGBs -= in.ShapeConfig.MemoryInGBs
		}
	}
}

// socketRoom returns the OCPUs and memory still free on each socket of the host.
func (b *InMemoryBackend) socketRoom(h *DedicatedVMHost) ([dvhSockets]float64, [dvhSockets]float64) {
	var oc, mem [dvhSockets]float64

	for s := range dvhSockets {
		oc[s], mem[s] = h.TotalOcpus/dvhSockets, h.TotalMemoryInGBs/dvhSockets
	}

	for _, in := range liveOf(b, func(i *Instance) bool { return i.DedicatedVMHostID == h.ID }) {
		if in.ShapeConfig != nil && in.Internal.Socket >= 0 && in.Internal.Socket < dvhSockets {
			oc[in.Internal.Socket] -= in.ShapeConfig.Ocpus
			mem[in.Internal.Socket] -= in.ShapeConfig.MemoryInGBs
		}
	}

	return oc, mem
}

// CreateDvhIn is the CreateDedicatedVMHost request body.
type CreateDvhIn struct {
	TagsIn
	PlacementConstraintDetails map[string]any `json:"placementConstraintDetails"`
	IsMemoryEncryptionEnabled  *bool          `json:"isMemoryEncryptionEnabled"`
	IsBurstable                *bool          `json:"isBurstable"`
	CompartmentID              string         `json:"compartmentId"`
	AvailabilityDomain         string         `json:"availabilityDomain"`
	DedicatedVMHostShape       string         `json:"dedicatedVmHostShape"`
	DisplayName                string         `json:"displayName"`
	FaultDomain                string         `json:"faultDomain"`
	CapacityConfig             string         `json:"capacityConfig"`
}

// CreateDedicatedVMHost creates a host of one of the published DVH shapes.
func (b *InMemoryBackend) CreateDedicatedVMHost(in CreateDvhIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	sh, ok := dvhShapeByName(in.DedicatedVMHostShape)

	switch {
	case !adRe.MatchString(in.AvailabilityDomain):
		return nil, "", errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	case !ok:
		return nil, "", errParam("dedicatedVmHostShape " + in.DedicatedVMHostShape + " is not a valid host shape")
	case in.FaultDomain != "" && !faultDomainRe.MatchString(in.FaultDomain):
		return nil, "", errParam("faultDomain must be FAULT-DOMAIN-1, FAULT-DOMAIN-2 or FAULT-DOMAIN-3")
	}

	h := &DedicatedVMHost{
		Base:                       b.newBase(kindDvh, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain:         in.AvailabilityDomain,
		DedicatedVMHostShape:       sh.name,
		FaultDomain:                in.FaultDomain,
		CapacityConfig:             in.CapacityConfig,
		TotalOcpus:                 sh.usableOc,
		RemainingOcpus:             sh.usableOc,
		TotalMemoryInGBs:           sh.usableMem,
		RemainingMemoryInGBs:       sh.usableMem,
		PlacementConstraintDetails: in.PlacementConstraintDetails,
		IsMemoryEncryption:         flag(in.IsMemoryEncryptionEnabled),
		IsBurstable:                flag(in.IsBurstable),
	}
	b.moveTo(h, statCreating, byoipActive)
	b.add(h)

	return h.view(), h.Internal.Etag, nil
}

// UpdateDedicatedVMHost changes the name and tags of a host.
func (b *InMemoryBackend) UpdateDedicatedVMHost(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*DedicatedVMHost) *APIError { return nil })
}

// DeleteDedicatedVMHost deletes a host that runs no instance.
func (b *InMemoryBackend) DeleteDedicatedVMHost(id, ifMatch string) (string, *APIError) {
	b.mu.Lock("DeleteDedicatedVmHost")
	defer b.mu.Unlock()

	h, aerr := get[*DedicatedVMHost](b, id)
	if aerr != nil {
		return "", aerr
	}

	wr := b.workFor(opDeleteDvh, h.CompartmentID, "dedicatedvmhost", id, "/dedicatedVmHosts/")
	if !live(h) || h.LifecycleState == statDeleted {
		return wr, nil
	}

	if aerr = checkEtag(h, ifMatch); aerr != nil {
		return "", aerr
	}

	if i := firstID(liveOf(b, func(x *Instance) bool { return x.DedicatedVMHostID == id })); i != "" {
		return "", errConflict("The dedicated VM host " + id + " still hosts instance " + i)
	}

	b.moveTo(h, statDeleting, statDeleted)

	return wr, nil
}

// CreateComputeClusterIn is the CreateComputeCluster request body.
type CreateComputeClusterIn struct {
	TagsIn
	PlacementConstraintDetails map[string]any `json:"placementConstraintDetails"`
	CompartmentID              string         `json:"compartmentId"`
	AvailabilityDomain         string         `json:"availabilityDomain"`
	DisplayName                string         `json:"displayName"`
}

// CreateComputeCluster creates a compute cluster.
func (b *InMemoryBackend) CreateComputeCluster(in CreateComputeClusterIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if !adRe.MatchString(in.AvailabilityDomain) {
		return nil, "", errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	c := &ComputeCluster{
		Base:               b.newBase(kindCluster, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain: in.AvailabilityDomain, PlacementConstraintDetails: in.PlacementConstraintDetails,
	}
	b.ready(c)
	c.LifecycleState = byoipActive
	b.add(c)

	return c.view(), c.Internal.Etag, nil
}

// UpdateComputeClusterIn is the UpdateComputeCluster request body.
type UpdateComputeClusterIn struct {
	UpdateIn
	PlacementConstraintDetails map[string]any `json:"placementConstraintDetails"`
}

// UpdateComputeCluster changes the name, tags and placement constraint of a cluster.
func (b *InMemoryBackend) UpdateComputeCluster(
	id, ifMatch string,
	in UpdateComputeClusterIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(c *ComputeCluster) *APIError {
		if in.PlacementConstraintDetails != nil {
			c.PlacementConstraintDetails = in.PlacementConstraintDetails
		}

		at := b.nowUTC()
		c.TimeUpdated = &at

		return nil
	})
}

// DeleteComputeCluster deletes a cluster no instance belongs to.
func (b *InMemoryBackend) DeleteComputeCluster(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteComputeCluster")
	defer b.mu.Unlock()

	c, aerr := get[*ComputeCluster](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(c) || c.LifecycleState == statDeleted {
		return nil
	}

	if aerr = checkEtag(c, ifMatch); aerr != nil {
		return aerr
	}

	if i := firstID(liveOf(b, func(x *Instance) bool { return x.ComputeClusterID == id })); i != "" {
		return errConflict("The compute cluster " + id + " still contains instance " + i)
	}

	c.LifecycleState = statDeleted
	at := b.nowUTC()
	c.Internal.TimeDeleted = &at
	b.bump(c)

	return nil
}

// rdmaShape reports whether a shape can join a compute cluster (cluster networking over RDMA).
func rdmaShape(s shapeDef) bool {
	prefixes := []string{
		"BM.HPC", "BM.Optimized3", "BM.GPU4", "BM.GPU.A100", "BM.GPU.H", "BM.GPU.MI", "BM.GPU.B", "BM.GPU.GB",
		"BM.GPU.L40S", "BM.GPU.RTXPRO",
	}

	for _, p := range prefixes {
		if strings.HasPrefix(s.name, p) {
			return true
		}
	}

	return false
}

// PlacementIn names the placement resources of a launch.
type PlacementIn struct {
	CapacityReservationID string `json:"capacityReservationId"`
	DedicatedVMHostID     string `json:"dedicatedVmHostId"`
	ComputeClusterID      string `json:"computeClusterId"`
}

// placeInstance validates and applies the placement of a launch onto a reservation, host or cluster.
func (b *InMemoryBackend) placeInstance(
	in LaunchInstanceIn,
	sh shapeDef,
	cfg *InstanceShapeConfig,
	inst *Instance,
) *APIError {
	if aerr := b.placeReservation(in, inst); aerr != nil {
		return aerr
	}

	if in.ComputeClusterID != "" {
		c, aerr := getLive[*ComputeCluster](b, in.ComputeClusterID)

		switch {
		case aerr != nil:
			return aerr
		case c.AvailabilityDomain != in.AvailabilityDomain:
			return errParam("The compute cluster " + c.ID + " is in a different availability domain")
		case !rdmaShape(sh):
			return errParam("Shape " + sh.name + " cannot join a compute cluster: it has no RDMA cluster network")
		}

		inst.ComputeClusterID = c.ID
	}

	if in.DedicatedVMHostID != "" {
		return b.placeOnHost(in, sh, cfg, inst)
	}

	return nil
}

func (b *InMemoryBackend) placeReservation(in LaunchInstanceIn, inst *Instance) *APIError {
	if in.CapacityReservationID == "" {
		return nil
	}

	r, aerr := getLive[*CapacityReservation](b, in.CapacityReservationID)

	switch {
	case aerr != nil:
		return aerr
	case r.LifecycleState != byoipActive:
		return errState("The capacity reservation " + r.ID + " is " + r.LifecycleState)
	case r.AvailabilityDomain != in.AvailabilityDomain:
		return errParam("The capacity reservation " + r.ID + " is in a different availability domain")
	}

	r.syncUsed(liveOf(b, func(i *Instance) bool { return i.CapacityReservationID == r.ID }))

	if !slices.ContainsFunc(
		r.Configs,
		func(c ReservationConfig) bool { return inst.fits(c) && c.UsedCount < c.ReservedCount },
	) {
		return errLimit("The capacity reservation " + r.ID + " has no unused capacity for " + inst.Shape)
	}

	inst.CapacityReservationID = r.ID

	return nil
}

func (b *InMemoryBackend) placeOnHost(
	in LaunchInstanceIn,
	sh shapeDef,
	cfg *InstanceShapeConfig,
	inst *Instance,
) *APIError {
	h, aerr := getLive[*DedicatedVMHost](b, in.DedicatedVMHostID)
	if aerr != nil {
		return aerr
	}

	hs, _ := dvhShapeByName(h.DedicatedVMHostShape)

	switch {
	case h.LifecycleState != byoipActive:
		return errState("The dedicated VM host " + h.ID + " is " + h.LifecycleState)
	case h.AvailabilityDomain != in.AvailabilityDomain:
		return errParam("The dedicated VM host " + h.ID + " is in a different availability domain")
	case sh.bareMetal || !strings.HasPrefix(sh.name, hs.series):
		return errParam("Shape " + sh.name + " is not supported on dedicated VM host shape " + h.DedicatedVMHostShape)
	case in.FaultDomain != "" && h.FaultDomain != "" && in.FaultDomain != h.FaultDomain:
		return errParam("faultDomain must match the fault domain of the dedicated VM host")
	}

	oc, mem := b.socketRoom(h)

	for s := range dvhSockets {
		if cfg.Ocpus <= oc[s] && cfg.MemoryInGBs <= mem[s] {
			inst.DedicatedVMHostID, inst.Internal.Socket = h.ID, s

			if h.FaultDomain != "" {
				inst.FaultDomain = h.FaultDomain
			}

			return nil
		}
	}

	return errLimit("The dedicated VM host " + h.ID + " has no socket with room for " + sh.name)
}

func reservationEntries() []entry {
	res, dvh, cc := idPath("/computeCapacityReservations", "capacityReservationId"),
		idPath("/dedicatedVmHosts", "dedicatedVmHostId"), idPath("/computeClusters", "computeClusterId")

	return []entry{
		{
			createWithWork((*InMemoryBackend).CreateComputeCapacityReservation, opCreateReservation,
				"computecapacityreservation", "/computeCapacityReservations/"), http.MethodPost,
			"/computeCapacityReservations", "CreateComputeCapacityReservation",
		},
		{
			listOp[*CapacityReservation](reservationFilter), http.MethodGet, "/computeCapacityReservations",
			"ListComputeCapacityReservations",
		},
		{getOp[*CapacityReservation]("capacityReservationId"), http.MethodGet, res, "GetComputeCapacityReservation"},
		{
			updateOp("capacityReservationId", (*InMemoryBackend).UpdateComputeCapacityReservation), http.MethodPut, res,
			"UpdateComputeCapacityReservation",
		},
		{
			workAction(
				"capacityReservationId",
				(*InMemoryBackend).DeleteComputeCapacityReservation,
			),
			http.MethodDelete,
			res,
			"DeleteComputeCapacityReservation",
		},
		{
			changeOp[*CapacityReservation](
				"capacityReservationId",
			),
			http.MethodPost,
			res + "/actions/changeCompartment",
			"ChangeComputeCapacityReservationCompartment",
		},
		{reservationInstancesOp, http.MethodGet, res + "/instances", "ListComputeCapacityReservationInstances"},
		{
			reservationShapesOp, http.MethodGet, "/computeCapacityReservationInstanceShapes",
			"ListComputeCapacityReservationInstanceShapes",
		},
		{
			createWithWork(
				(*InMemoryBackend).CreateDedicatedVMHost,
				opCreateDvh,
				"dedicatedvmhost",
				"/dedicatedVmHosts/",
			),
			http.MethodPost,
			"/dedicatedVmHosts",
			"CreateDedicatedVmHost",
		},
		{listOp[*DedicatedVMHost](dvhFilter), http.MethodGet, "/dedicatedVmHosts", "ListDedicatedVmHosts"},
		{getOp[*DedicatedVMHost]("dedicatedVmHostId"), http.MethodGet, dvh, "GetDedicatedVmHost"},
		{
			updateOp("dedicatedVmHostId", (*InMemoryBackend).UpdateDedicatedVMHost),
			http.MethodPut,
			dvh,
			"UpdateDedicatedVmHost",
		},
		{
			workAction("dedicatedVmHostId", (*InMemoryBackend).DeleteDedicatedVMHost), http.MethodDelete, dvh,
			"DeleteDedicatedVmHost",
		},
		{
			changeOp[*DedicatedVMHost]("dedicatedVmHostId"), http.MethodPost, dvh + "/actions/changeCompartment",
			"ChangeDedicatedVmHostCompartment",
		},
		{dvhInstancesOp, http.MethodGet, dvh + "/instances", "ListDedicatedVmHostInstances"},
		{dvhShapesOp, http.MethodGet, "/dedicatedVmHostShapes", "ListDedicatedVmHostShapes"},
		{dvhInstanceShapesOp, http.MethodGet, "/dedicatedVmHostInstanceShapes", "ListDedicatedVmHostInstanceShapes"},
		{
			createOp((*InMemoryBackend).CreateComputeCluster),
			http.MethodPost,
			"/computeClusters",
			"CreateComputeCluster",
		},
		{listOp[*ComputeCluster](clusterFilter), http.MethodGet, "/computeClusters", "ListComputeClusters"},
		{getOp[*ComputeCluster]("computeClusterId"), http.MethodGet, cc, "GetComputeCluster"},
		{
			updateOp("computeClusterId", (*InMemoryBackend).UpdateComputeCluster),
			http.MethodPut,
			cc,
			"UpdateComputeCluster",
		},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.DeleteComputeCluster(q.param("computeClusterId"), q.ifMatch()))
		}, http.MethodDelete, cc, "DeleteComputeCluster"},
		{
			changeOp[*ComputeCluster]("computeClusterId"), http.MethodPost, cc + "/actions/changeCompartment",
			"ChangeComputeClusterCompartment",
		},
	}
}

func createWithWork[In any](
	fn func(*InMemoryBackend, In) (Resource, string, *APIError), op, entity, path string,
) opFunc {
	return func(h *Handler, q *req) error {
		var in In
		if err := q.decode(&in); err != nil {
			return err
		}

		if aerr := h.Backend.checkTags(in); aerr != nil {
			return aerr
		}

		scope, token := q.r.Method+" "+q.r.URL.Path, q.r.Header.Get(headerRetry)

		v, etag, aerr := h.Backend.createOnce(scope, token, func() (Resource, string, *APIError) {
			return fn(h.Backend, in)
		})
		if aerr != nil {
			return aerr
		}

		bs := v.base()
		q.header("opc-work-request-id", h.Backend.recordWork(op, bs.CompartmentID, entity, bs.ID, path))

		return send(q, v, etag, nil)
	}
}

func reservationFilter(q *req) func(Resource) bool {
	return fieldEq[*CapacityReservation](
		q,
		"availabilityDomain",
		func(x *CapacityReservation) string { return x.AvailabilityDomain },
	)
}

func dvhFilter(q *req) func(Resource) bool {
	atLeast := func(key string, get func(*DedicatedVMHost) float64) func(Resource) bool {
		want, err := strconv.ParseFloat(q.query(key), 64)

		return func(r Resource) bool {
			x, _ := r.(*DedicatedVMHost)

			return q.query(key) == "" || (err == nil && get(x) >= want)
		}
	}

	return and(
		fieldEq[*DedicatedVMHost](
			q,
			"availabilityDomain",
			func(x *DedicatedVMHost) string { return x.AvailabilityDomain },
		),
		atLeast("remainingOcpusGreaterThanOrEqualTo", func(h *DedicatedVMHost) float64 { return h.RemainingOcpus }),
		atLeast(
			"remainingMemoryInGBsGreaterThanOrEqualTo",
			func(h *DedicatedVMHost) float64 { return h.RemainingMemoryInGBs },
		),
		func(r Resource) bool {
			x, _ := r.(*DedicatedVMHost)
			want := q.query("isMemoryEncryptionEnabled")

			return want == "" || want == boolStr(x.IsMemoryEncryption)
		},
		func(r Resource) bool {
			x, _ := r.(*DedicatedVMHost)
			name := q.query("instanceShapeName")
			hs, _ := dvhShapeByName(x.DedicatedVMHostShape)

			return name == "" || strings.HasPrefix(name, hs.series)
		},
	)
}

func clusterFilter(q *req) func(Resource) bool {
	return fieldEq[*ComputeCluster](
		q,
		"availabilityDomain",
		func(x *ComputeCluster) string { return x.AvailabilityDomain },
	)
}

// ReservationInstance is a CapacityReservationInstanceSummary.
type ReservationInstance struct {
	ShapeConfig        *ReservationShapeConfig `json:"shapeConfig,omitempty"`
	ID                 string                  `json:"id"`
	AvailabilityDomain string                  `json:"availabilityDomain"`
	CompartmentID      string                  `json:"compartmentId"`
	Shape              string                  `json:"shape"`
	FaultDomain        string                  `json:"faultDomain,omitempty"`
}

func reservationInstancesOp(h *Handler, q *req) error {
	h.Backend.mu.RLock("ListComputeCapacityReservationInstances")
	defer h.Backend.mu.RUnlock()

	if _, aerr := get[*CapacityReservation](h.Backend, q.param("capacityReservationId")); aerr != nil {
		return aerr
	}

	out := []ReservationInstance{}

	id := q.param("capacityReservationId")

	for _, i := range liveOf(h.Backend, func(x *Instance) bool { return x.CapacityReservationID == id }) {
		ri := ReservationInstance{
			ID: i.ID, AvailabilityDomain: i.AvailabilityDomain, CompartmentID: i.CompartmentID,
			Shape: i.Shape, FaultDomain: i.FaultDomain,
		}
		if i.ShapeConfig != nil {
			oc, mem := i.ShapeConfig.Ocpus, i.ShapeConfig.MemoryInGBs
			ri.ShapeConfig = &ReservationShapeConfig{Ocpus: &oc, MemoryInGBs: &mem}
		}

		out = append(out, ri)
	}

	slices.SortFunc(out, func(a, c ReservationInstance) int { return compareStr(a.ID, c.ID) })

	return q.json(http.StatusOK, out, "")
}

func reservationShapesOp(h *Handler, q *req) error {
	if aerr := h.Backend.checkCompartmentLocked(q.query("compartmentId")); aerr != nil {
		return aerr
	}

	ad := q.query("availabilityDomain")
	if !adRe.MatchString(ad) {
		return errParam("availabilityDomain " + ad + " is not a valid availability domain")
	}

	out := []map[string]string{}

	for _, s := range shapeDefs {
		out = append(out, map[string]string{"availabilityDomain": ad, "instanceShape": s.name})
	}

	return q.json(http.StatusOK, out, "")
}

// DvhInstance is a DedicatedVMHostInstanceSummary.
type DvhInstance struct {
	TimeCreated        time.Time `json:"timeCreated"`
	InstanceID         string    `json:"instanceId"`
	AvailabilityDomain string    `json:"availabilityDomain"`
	CompartmentID      string    `json:"compartmentId"`
	Shape              string    `json:"shape"`
}

func dvhInstancesOp(h *Handler, q *req) error {
	h.Backend.mu.RLock("ListDedicatedVmHostInstances")
	defer h.Backend.mu.RUnlock()

	if _, aerr := get[*DedicatedVMHost](h.Backend, q.param("dedicatedVmHostId")); aerr != nil {
		return aerr
	}

	out := []DvhInstance{}

	id := q.param("dedicatedVmHostId")

	for _, i := range liveOf(h.Backend, func(x *Instance) bool { return x.DedicatedVMHostID == id }) {
		out = append(out, DvhInstance{
			TimeCreated: i.TimeCreated, InstanceID: i.ID, AvailabilityDomain: i.AvailabilityDomain,
			CompartmentID: i.CompartmentID, Shape: i.Shape,
		})
	}

	slices.SortFunc(out, func(a, c DvhInstance) int { return compareStr(a.InstanceID, c.InstanceID) })

	return q.json(http.StatusOK, out, "")
}

func dvhShapesOp(h *Handler, q *req) error {
	if aerr := h.Backend.checkCompartmentLocked(q.query("compartmentId")); aerr != nil {
		return aerr
	}

	out := []map[string]string{}
	want := q.query("instanceShapeName")

	for _, s := range dvhShapes() {
		if want == "" || strings.HasPrefix(want, s.series) {
			out = append(out, map[string]string{"dedicatedVmHostShape": s.name})
		}
	}

	return q.json(http.StatusOK, out, "")
}

func dvhInstanceShapesOp(h *Handler, q *req) error {
	if aerr := h.Backend.checkCompartmentLocked(q.query("compartmentId")); aerr != nil {
		return aerr
	}

	out := []map[string]string{}
	want := q.query("dedicatedVmHostShape")

	for _, hs := range dvhShapes() {
		if want != "" && want != hs.name {
			continue
		}

		for _, s := range shapeDefs {
			if !s.bareMetal && strings.HasPrefix(s.name, hs.series) {
				out = append(out, map[string]string{"instanceShapeName": s.name})
			}
		}
	}

	return q.json(http.StatusOK, out, "")
}

func (b *InMemoryBackend) checkCompartmentLocked(id string) *APIError {
	b.mu.RLock("checkCompartment")
	defer b.mu.RUnlock()

	return b.checkCompartment(id)
}
