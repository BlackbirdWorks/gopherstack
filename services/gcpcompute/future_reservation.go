package gcpcompute

import (
	"regexp"
	"strconv"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collFutureRes     = "futureReservations"
	frDraft           = "DRAFT"
	frSubmitted       = "SUBMITTED"
	frDrafting        = "DRAFTING"
	frApproved        = "APPROVED"
	frCancelled       = "CANCELLED"
	frAmendApproved   = "AMENDMENT_APPROVED"
	frMaxPrefixLength = 20
)

var (
	frPrefixRE  = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)
	frPatchDrop = []string{ //nolint:gochecknoglobals // read-only lookup table
		"id", jsonKind, jsonSelfLink, jsonCreated, jsonName, "zone", "status", jsonSelfLinkID, jsonParams,
		"resourceMetadata",
	}
	frEnums = map[string][]string{ //nolint:gochecknoglobals // read-only lookup table
		"reservationMode": {"CALENDAR", "DEFAULT", "RESERVATION_MODE_UNSPECIFIED"},
		"schedulingType":  {"GROUPED", "INDEPENDENT", "GROUP_MAINTENANCE_TYPE_UNSPECIFIED"},
		"deploymentType":  {"DENSE", "DEPLOYMENT_TYPE_UNSPECIFIED"},
		"planningStatus":  {frDraft, frSubmitted, "PLANNING_STATUS_UNSPECIFIED"},
		"confidentialComputeType": {
			"CONFIDENTIAL_COMPUTE_TYPE_BMSAI",
			"CONFIDENTIAL_COMPUTE_TYPE_TDX",
			"CONFIDENTIAL_COMPUTE_TYPE_UNSPECIFIED",
		},
	}
)

func futureReservationSpec() lbSpec[computeapi.FutureReservation] {
	return lbSpec[computeapi.FutureReservation]{
		coll: collFutureRes, kind: "compute#futureReservation", noun: "future_reservation",
		m: func(b *InMemoryBackend) map[string]*computeapi.FutureReservation { return b.futureRes },
		meta: func(v *computeapi.FutureReservation) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				zone: &v.Zone, id: &v.Id,
			}
		},
		prep: prepFutureReservation,
		keep: func(old, next *computeapi.FutureReservation) { next.Status = old.Status },
	}
}

func frProcurement(f *computeapi.FutureReservation) string {
	if f.Status == nil {
		return ""
	}

	return f.Status.ProcurementStatus
}

func frEnum(field, v string) *APIError {
	if v != "" && !inList(frEnums[field], v) {
		return errField("resource."+field, v, "")
	}

	return nil
}

func prepFutureReservation(
	b *InMemoryBackend, c lbCtx, in *computeapi.FutureReservation, _ map[string]any,
) *APIError {
	if !zoneExists(c.loc) {
		return errNotFound(scopePath(c.project, scopeZone, c.loc))
	}

	in.Params = nil

	for field, v := range map[string]string{
		"reservationMode": in.ReservationMode, "schedulingType": in.SchedulingType,
		"deploymentType": in.DeploymentType, "planningStatus": in.PlanningStatus,
		"confidentialComputeType": in.ConfidentialComputeType,
	} {
		if err := frEnum(field, v); err != nil {
			return err
		}
	}

	if len(in.NamePrefix) > frMaxPrefixLength || in.NamePrefix != "" && !frPrefixRE.MatchString(in.NamePrefix) {
		return errField("resource.namePrefix", in.NamePrefix, "Must be an RFC1035 name of at most 20 characters.")
	}

	if in.ReservationName != "" {
		if err := validName("resource.reservationName", in.ReservationName); err != nil {
			return err
		}
	}

	if err := validFutureWindow(in); err != nil {
		return err
	}

	if err := b.validFutureSKU(c, in); err != nil {
		return err
	}

	in.SelfLinkWithId = link(c.project, scopeZone, c.loc, collFutureRes, strconv.FormatUint(in.Id, 10))
	b.applyFutureStatus(in)

	return nil
}

func validFutureWindow(in *computeapi.FutureReservation) *APIError {
	w := in.TimeWindow
	if w == nil || w.StartTime == "" {
		return errRequired("resource.timeWindow.startTime")
	}

	start, err := time.Parse(time.RFC3339, w.StartTime)
	if err != nil {
		return errField("resource.timeWindow.startTime", w.StartTime, "Must be an RFC3339 timestamp.")
	}

	if err := validWindowEnd(w, start); err != nil {
		return err
	}

	if t := in.AutoCreatedReservationsDeleteTime; t != "" {
		if _, terr := time.Parse(time.RFC3339, t); terr != nil {
			return errField("resource.autoCreatedReservationsDeleteTime", t, "Must be an RFC3339 timestamp.")
		}
	}

	return nil
}

func validWindowEnd(w *computeapi.FutureReservationTimeWindow, start time.Time) *APIError {
	switch {
	case w.EndTime == "" && w.Duration == nil:
		return errRequired("resource.timeWindow.endTime")
	case w.EndTime != "" && w.Duration != nil:
		return errField("resource.timeWindow", "", "Specify either endTime or duration, not both.")
	case w.Duration != nil:
		if w.Duration.Seconds < 0 || w.Duration.Nanos < 0 || w.Duration.Seconds == 0 && w.Duration.Nanos == 0 {
			return errField("resource.timeWindow.duration", "", "The duration must be positive.")
		}

		return nil
	}

	end, err := time.Parse(time.RFC3339, w.EndTime)
	if err != nil {
		return errField("resource.timeWindow.endTime", w.EndTime, "Must be an RFC3339 timestamp.")
	}

	if !end.After(start) {
		return errField("resource.timeWindow.endTime", w.EndTime, "Must be after the start time.")
	}

	return nil
}

func (b *InMemoryBackend) validFutureSKU(c lbCtx, in *computeapi.FutureReservation) *APIError {
	sku := in.SpecificSkuProperties
	if sku == nil && in.AggregateReservation == nil && in.StoragePoolProperties == nil {
		return errRequired("resource.specificSkuProperties")
	}

	if sku == nil {
		return nil
	}

	if sku.TotalCount < 1 {
		return errField("resource.specificSkuProperties.totalCount", strconv.FormatInt(sku.TotalCount, 10),
			"At least one instance is required.")
	}

	if sku.SourceInstanceTemplate != "" {
		t, err := b.resolveTemplateLocked(c.project, sku.SourceInstanceTemplate,
			"specificSkuProperties.sourceInstanceTemplate")
		if err != nil {
			return err
		}

		sku.SourceInstanceTemplate = t.SelfLink

		return nil
	}

	if sku.InstanceProperties == nil || sku.InstanceProperties.MachineType == "" {
		return errRequired("resource.specificSkuProperties.instanceProperties.machineType")
	}

	_, err := b.resolveMachineLocked(c.project, c.loc, sku.InstanceProperties.MachineType)

	return err
}

// applyFutureStatus derives the procurement state. Google's approval is mocked: a submitted request is
// approved at once and no capacity is ever procured.
func (b *InMemoryBackend) applyFutureStatus(in *computeapi.FutureReservation) {
	st := in.Status
	if st == nil {
		st = &computeapi.FutureReservationStatus{}
	}

	switch {
	case st.ProcurementStatus == frCancelled:
	case in.PlanningStatus == frDraft:
		st.ProcurementStatus = frDrafting
	default:
		if st.ProcurementStatus == frApproved {
			st.AmendmentStatus = frAmendApproved
		}

		st.ProcurementStatus = frApproved
	}

	if in.SpecificSkuProperties != nil && in.SpecificSkuProperties.SourceInstanceTemplate != "" {
		st.SpecificSkuProperties = &computeapi.FutureReservationStatusSpecificSKUProperties{
			SourceInstanceTemplateId: lastSegment(in.SpecificSkuProperties.SourceInstanceTemplate),
		}
	}

	in.Status = st
}

func (b *InMemoryBackend) updateFutureReservation(
	c lbCtx, name string, raw map[string]any, mask string,
) (*computeapi.Operation, error) {
	return lbMutate(b, futureReservationSpec(), c, name, "update", nil,
		func(cur *computeapi.FutureReservation) (*computeapi.FutureReservation, *APIError) {
			if frProcurement(cur) == frCancelled {
				return nil, errInvalid("The future reservation is cancelled and cannot be modified.")
			}

			next, err := mergeInto(cur, maskedBody(raw, mask, frPatchDrop))
			if err != nil {
				return nil, err
			}

			if cur.PlanningStatus == frSubmitted && next.PlanningStatus == frDraft {
				return nil, errField("resource.planningStatus", next.PlanningStatus,
					"A submitted future reservation cannot return to draft.")
			}

			futureReservationSpec().keep(cur, next)

			return next, nil
		})
}

func (b *InMemoryBackend) cancelFutureReservation(c lbCtx, name string) (*computeapi.Operation, error) {
	return lbMutate(b, futureReservationSpec(), c, name, "cancel", nil,
		func(cur *computeapi.FutureReservation) (*computeapi.FutureReservation, *APIError) {
			if frProcurement(cur) == frCancelled {
				return nil, errInvalid("The future reservation is already cancelled.")
			}

			if cur.Status == nil {
				cur.Status = &computeapi.FutureReservationStatus{}
			}

			cur.Status.ProcurementStatus = frCancelled

			return cur, nil
		})
}

func (h *Handler) registerFutureReservationOps() {
	z := []string{scopeZone}

	registerLB(h, futureReservationSpec(), lbOpts[computeapi.FutureReservation]{scopes: z, agg: zoneScopes()})
	h.reg(collFutureRes, scopeZone, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.FutureReservation](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateFutureReservation(ctxOf(rq), rq.rt.name, raw, rq.q.Get("updateMask")))
	})
	h.reg(collFutureRes, scopeZone, "cancel", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.cancelFutureReservation(ctxOf(rq), rq.rt.name))
	})
}
