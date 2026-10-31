package ocicontainerinstances

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	apiBase       = "/20210415"
	percentMid    = 50
	percentDone   = 100
	percentMax    = 99
	percentFactor = 100
)

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	Action     string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri"`
}

// WorkRequestWire is the wire form of a work request.
type WorkRequestWire struct {
	TimeAccepted time.Time        `json:"timeAccepted"`
	TimeStarted  time.Time        `json:"timeStarted"`
	TimeFinished *time.Time       `json:"timeFinished,omitempty"`
	Type         string           `json:"operationType"`
	Status       string           `json:"status"`
	ID           string           `json:"id"`
	Compartment  string           `json:"compartmentId"`
	Resources    []wrResourceWire `json:"resources"`
	Percent      float32          `json:"percentComplete"`
}

type wrErrorWire struct {
	Timestamp time.Time `json:"timestamp"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
}

type wrLogWire struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	uri := apiBase + "/containerInstances/" + wr.ResourceID
	if wr.ResourceType == "container" {
		uri = apiBase + "/containers/" + wr.ResourceID
	}

	pct := float32(percentMid)

	switch {
	case wr.FinishedAt != nil:
		pct = percentDone
	case wr.FinishAt != nil:
		total := wr.FinishAt.Sub(wr.Accepted)
		if total > 0 {
			pct = min(percentMax, float32(b.nowUTC().Sub(wr.Accepted))*percentFactor/float32(total))
		}
	}

	return WorkRequestWire{
		ID: wr.ID, Type: wr.Type, Status: wr.Status, Compartment: wr.Compartment, Percent: max(pct, 0),
		TimeAccepted: wr.Accepted, TimeStarted: wr.Accepted, TimeFinished: wr.FinishedAt,
		Resources: []wrResourceWire{{
			EntityType: wr.ResourceType, Action: wr.Action, Identifier: wr.ResourceID, EntityURI: uri,
		}},
	}
}

// GetWorkRequest returns one work request.
func (b *InMemoryBackend) GetWorkRequest(id string) (WorkRequestWire, error) {
	b.mu.RLock("GetWorkRequest")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return WorkRequestWire{}, errNotFound(id)
	}

	return b.wrView(wr), nil
}

// WorkRequestFilter selects work requests in a list.
type WorkRequestFilter struct {
	Compartment string
	ID          string
	Status      string
	ResourceID  string
	AD          string
}

// ListWorkRequests lists the work requests of a compartment, newest first.
func (b *InMemoryBackend) ListWorkRequests(f WorkRequestFilter, o ListOpts) (page.Page[WorkRequestWire], error) {
	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[WorkRequestWire]{}, aerr
	}

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if wr.Compartment != f.Compartment || (f.ID != "" && wr.ID != f.ID) ||
			(f.Status != "" && wr.Status != f.Status) || (f.ResourceID != "" && wr.ResourceID != f.ResourceID) {
			continue
		}

		if in := b.instances[wr.ResourceID]; f.AD != "" && (in == nil || in.AD != f.AD) {
			continue
		}

		all = append(all, wr)
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit, Order: o.Order},
		func(w *WorkRequest) string { return w.ID }, func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}

// WorkRequestErrors lists the errors of a failed work request.
func (b *InMemoryBackend) WorkRequestErrors(id string, o ListOpts) (page.Page[wrErrorWire], error) {
	b.mu.RLock("WorkRequestErrors")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[wrErrorWire]{}, errNotFound(id)
	}

	all := []wrErrorWire{}
	if wr.Status == wrFailed && wr.FinishedAt != nil {
		all = append(all, wrErrorWire{Timestamp: *wr.FinishedAt, Code: wr.ErrCode, Message: wr.ErrMsg})
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}

// WorkRequestLogs lists the progress log of a work request, oldest first.
func (b *InMemoryBackend) WorkRequestLogs(id string, o ListOpts) (page.Page[wrLogWire], error) {
	b.mu.RLock("WorkRequestLogs")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[wrLogWire]{}, errNotFound(id)
	}

	all := []wrLogWire{
		{Timestamp: wr.Accepted, Message: wr.Type + " accepted"},
		{Timestamp: wr.Accepted, Message: wr.Type + " in progress"},
	}

	if wr.FinishedAt != nil {
		msg := wr.Type + " succeeded"
		if wr.Status == wrFailed {
			msg = wr.Type + " failed: " + wr.ErrMsg
		}

		all = append(all, wrLogWire{Timestamp: *wr.FinishedAt, Message: msg})
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}

// ShapeWire is the wire form of a container instance shape.
type ShapeWire struct {
	Name      string `json:"name"`
	Processor string `json:"processorDescription"`
	Ocpu      struct {
		Min float32 `json:"min"`
		Max float32 `json:"max"`
	} `json:"ocpuOptions"`
	Memory struct {
		MinInGBs   float32 `json:"minInGBs"`
		MaxInGBs   float32 `json:"maxInGBs"`
		DefPerOcpu float32 `json:"defaultPerOcpuInGBs"`
		MinPerOcpu float32 `json:"minPerOcpuInGBs"`
		MaxPerOcpu float32 `json:"maxPerOcpuInGBs"`
	} `json:"memoryOptions"`
	Bandwidth struct {
		Min        float32 `json:"minInGbps"`
		Max        float32 `json:"maxInGbps"`
		DefPerOcpu float32 `json:"defaultPerOcpuInGbps"`
	} `json:"networkingBandwidthOptions"`
}

// ListShapes lists the container instance shapes.
func (b *InMemoryBackend) ListShapes(compartment string, o ListOpts) (page.Page[ShapeWire], error) {
	b.mu.RLock("ListShapes")
	err := b.checkCompartment(compartment)
	b.mu.RUnlock()

	if err != nil {
		return page.Page[ShapeWire]{}, err
	}

	var all []ShapeWire

	for _, s := range shapeCatalog() {
		w := ShapeWire{Name: s.Name, Processor: s.Processor}
		w.Ocpu.Min, w.Ocpu.Max = s.MinOcpus, s.MaxOcpus
		w.Memory.MinInGBs, w.Memory.MaxInGBs, w.Memory.DefPerOcpu = minMemoryGB, s.MaxMemory, s.DefPerOcpu
		w.Memory.MinPerOcpu, w.Memory.MaxPerOcpu = minPerOcpuGB, s.MaxPerOcpu
		w.Bandwidth.Min, w.Bandwidth.Max, w.Bandwidth.DefPerOcpu = gbpsPerOcpu, s.MaxBandwith, gbpsPerOcpu
		all = append(all, w)
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}
