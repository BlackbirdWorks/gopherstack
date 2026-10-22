package connect

import (
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	dateLayout        = "2006-01-02"
	maxEffectiveDays  = 92
	overrideStandard  = "STANDARD"
	overrideOpen      = "OPEN"
	overrideClosed    = "CLOSED"
	maxHoursParents   = 5
	maxOverrideNameLn = 127
)

// ParentHoursConfig references a parent schedule.
type ParentHoursConfig struct {
	HoursOfOperationID string `json:"HoursOfOperationId"`
}

// HoursIdentifier identifies a related schedule.
type HoursIdentifier struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// OverrideConfig is one day of an override.
type OverrideConfig struct {
	Day       string    `json:"Day"`
	EndTime   TimeSlice `json:"EndTime"`
	StartTime TimeSlice `json:"StartTime"`
}

// RecurrencePattern describes how an override repeats.
type RecurrencePattern struct {
	Interval            *int32  `json:"Interval,omitempty"`
	Frequency           string  `json:"Frequency,omitempty"`
	ByMonth             []int32 `json:"ByMonth,omitempty"`
	ByMonthDay          []int32 `json:"ByMonthDay,omitempty"`
	ByWeekdayOccurrence []int32 `json:"ByWeekdayOccurrence,omitempty"`
}

// RecurrenceConfig wraps the recurrence pattern.
type RecurrenceConfig struct {
	RecurrencePattern *RecurrencePattern `json:"RecurrencePattern,omitempty"`
}

// HoursOfOperationOverride is a dated exception to a schedule.
type HoursOfOperationOverride struct {
	RecurrenceConfig           *RecurrenceConfig `json:"RecurrenceConfig,omitempty"`
	Description                string            `json:"Description,omitempty"`
	EffectiveFrom              string            `json:"EffectiveFrom"`
	EffectiveTill              string            `json:"EffectiveTill"`
	HoursOfOperationArn        string            `json:"HoursOfOperationArn"`
	HoursOfOperationID         string            `json:"HoursOfOperationId"`
	HoursOfOperationOverrideID string            `json:"HoursOfOperationOverrideId"`
	Name                       string            `json:"Name"`
	OverrideType               string            `json:"OverrideType,omitempty"`
	Config                     []OverrideConfig  `json:"Config"`
}

type hoursOverrideRec struct {
	InstanceID string                   `json:"instanceId"`
	Override   HoursOfOperationOverride `json:"override"`
}

type overrideBody struct {
	RecurrenceConfig *RecurrenceConfig
	Description      *string
	EffectiveFrom    *string
	EffectiveTill    *string
	Name             *string
	OverrideType     *string
	Config           []OverrideConfig
}

// CreateHoursOfOperationOverrideInput is the CreateHoursOfOperationOverride request.
type CreateHoursOfOperationOverrideInput struct {
	HoursID string `json:"-" path:"HoursOfOperationId"`
	instPath
	overrideBody
}

// UpdateHoursOfOperationOverrideInput is the UpdateHoursOfOperationOverride request.
type UpdateHoursOfOperationOverrideInput struct {
	HoursID    string `json:"-" path:"HoursOfOperationId"`
	OverrideID string `json:"-" path:"HoursOfOperationOverrideId"`
	instPath
	overrideBody
}

// ParentHoursInput is the Associate/DisassociateHoursOfOperations request.
type ParentHoursInput struct {
	HoursID string `json:"-"                         path:"HoursOfOperationId"`
	instPath
	ParentHoursOfOperationConfigs []ParentHoursConfig
	ParentHoursOfOperationIDs     []string `json:"ParentHoursOfOperationIds"`
}

// EffectiveHoursInput is the GetEffectiveHoursOfOperations request.
type EffectiveHoursInput struct {
	HoursID  string `json:"-" path:"HoursOfOperationId"`
	FromDate string `json:"-"                           query:"fromDate"`
	ToDate   string `json:"-"                           query:"toDate"`
	instPath
}

type overrideOut struct {
	ID string `json:"HoursOfOperationOverrideId"`
}

type operationalHour struct {
	End   TimeSlice `json:"End"`
	Start TimeSlice `json:"Start"`
}

type effectiveHours struct {
	Date             string            `json:"Date"`
	OperationalHours []operationalHour `json:"OperationalHours"`
}

type overrideHour struct {
	OperationalStatus string    `json:"OperationalStatus"`
	OverrideName      string    `json:"OverrideName"`
	End               TimeSlice `json:"End"`
	Start             TimeSlice `json:"Start"`
}

type effectiveOverrideHours struct {
	Date          string         `json:"Date"`
	OverrideHours []overrideHour `json:"OverrideHours"`
}

// EffectiveHoursOut is the GetEffectiveHoursOfOperations response.
type EffectiveHoursOut struct {
	TimeZone                      string                   `json:"TimeZone"`
	EffectiveHoursOfOperationList []effectiveHours         `json:"EffectiveHoursOfOperationList"`
	EffectiveOverrideHoursList    []effectiveOverrideHours `json:"EffectiveOverrideHoursList"`
}

// ChildHoursOut is the ListChildHoursOfOperations response.
type ChildHoursOut struct {
	LastModifiedTime                  *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	NextToken                         string             `json:"NextToken,omitempty"`
	LastModifiedRegion                string             `json:"LastModifiedRegion,omitempty"`
	ChildHoursOfOperationsSummaryList []HoursIdentifier  `json:"ChildHoursOfOperationsSummaryList"`
}

// OverrideListOut is the ListHoursOfOperationOverrides response.
type OverrideListOut struct {
	LastModifiedTime             *awstime.EpochTime         `json:"LastModifiedTime,omitempty"`
	NextToken                    string                     `json:"NextToken,omitempty"`
	LastModifiedRegion           string                     `json:"LastModifiedRegion,omitempty"`
	HoursOfOperationOverrideList []HoursOfOperationOverride `json:"HoursOfOperationOverrideList"`
}

func parseDate(field, v string) (time.Time, error) {
	t, err := time.Parse(dateLayout, v)
	if err != nil {
		return t, invalidParamErr("%s must be a YYYY-MM-DD date", field)
	}

	return t, nil
}

func checkOverrideConfig(typ string, cfg []OverrideConfig) error {
	if typ == overrideStandard && len(cfg) == 0 {
		return invalidParamErr("Config is required for STANDARD overrides")
	}

	for _, c := range cfg {
		if err := oneOf("Config.Day", c.Day, weekDays()...); err != nil {
			return err
		}

		for _, t := range []TimeSlice{c.StartTime, c.EndTime} {
			if t.Hours < 0 || t.Hours > maxHoursInDay || t.Minutes < 0 || t.Minutes > maxMinutes {
				return invalidParamErr("Config times must be between 00:00 and 23:59")
			}
		}
	}

	return nil
}

func checkOverride(o *HoursOfOperationOverride) error {
	if err := firstError(
		checkName("Name", o.Name, maxOverrideNameLn),
		checkDescription(o.Description),
		oneOf("OverrideType", o.OverrideType, overrideStandard, overrideOpen, overrideClosed),
		checkOverrideConfig(o.OverrideType, o.Config),
	); err != nil {
		return err
	}

	from, err := parseDate("EffectiveFrom", o.EffectiveFrom)
	if err != nil {
		return err
	}

	till, err := parseDate("EffectiveTill", o.EffectiveTill)
	if err != nil {
		return err
	}

	if till.Before(from) {
		return invalidParamErr("EffectiveTill must not be before EffectiveFrom")
	}

	if rp := o.RecurrenceConfig; rp != nil && rp.RecurrencePattern != nil {
		return oneOf("RecurrencePattern.Frequency", rp.RecurrencePattern.Frequency, "WEEKLY", "MONTHLY", "YEARLY")
	}

	return nil
}

func overrideNameTaken(b *Backend, inst, hoursID, name, except string) bool {
	return slices.ContainsFunc(b.hoursOverrides.in(inst), func(o *hoursOverrideRec) bool {
		return o.Override.HoursOfOperationID == hoursID && o.Override.HoursOfOperationOverrideID != except &&
			o.Override.Name == name
	})
}

// CreateHoursOfOperationOverride creates a dated exception to a schedule.
func (b *Backend) CreateHoursOfOperationOverride(
	ctx context.Context, in *CreateHoursOfOperationOverrideInput,
) (*overrideOut, error) {
	o := overrideFromBody(&in.overrideBody)
	o.HoursOfOperationID = in.HoursID

	if o.OverrideType == "" {
		o.OverrideType = overrideStandard
	}

	if err := checkOverride(&o); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateHoursOfOperationOverride", in.InstanceID, func(inst string) (*overrideOut, error) {
		h, err := b.hours.get(inst, in.HoursID)
		if err != nil {
			return nil, err
		}

		if overrideNameTaken(b, inst, in.HoursID, o.Name, "") {
			return nil, duplicateErr("HoursOfOperationOverride", o.Name)
		}

		o.HoursOfOperationArn = h.Hours.HoursOfOperationArn
		o.HoursOfOperationOverrideID = newID()
		b.hoursOverrides.put(&hoursOverrideRec{InstanceID: inst, Override: o})
		h.Hours.Stamp = b.stamp(ctx)

		return &overrideOut{ID: o.HoursOfOperationOverrideID}, nil
	})
}

func overrideFromBody(in *overrideBody) HoursOfOperationOverride {
	o := HoursOfOperationOverride{Config: orEmpty(in.Config), RecurrenceConfig: in.RecurrenceConfig}
	for dst, src := range map[*string]*string{
		&o.Name: in.Name, &o.Description: in.Description, &o.EffectiveFrom: in.EffectiveFrom,
		&o.EffectiveTill: in.EffectiveTill, &o.OverrideType: in.OverrideType,
	} {
		if src != nil {
			*dst = *src
		}
	}

	return o
}

// DescribeHoursOfOperationOverride returns an override.
func (b *Backend) DescribeHoursOfOperationOverride(inst, hoursID, id string) (*HoursOfOperationOverride, error) {
	return readInstance(
		b,
		"DescribeHoursOfOperationOverride",
		inst,
		func(inst string) (*HoursOfOperationOverride, error) {
			o, err := b.overrideOf(inst, hoursID, id)
			if err != nil {
				return nil, err
			}

			out := o.Override

			return &out, nil
		},
	)
}

func (b *Backend) overrideOf(inst, hoursID, id string) (*hoursOverrideRec, error) {
	if !b.hours.has(inst, hoursID) {
		return nil, notFoundErr("HoursOfOperation", hoursID)
	}

	o, err := b.hoursOverrides.get(inst, id)
	if err != nil || o.Override.HoursOfOperationID != hoursID {
		return nil, notFoundErr("HoursOfOperationOverride", id)
	}

	return o, nil
}

// ListHoursOfOperationOverrides pages through the overrides of a schedule.
func (b *Backend) ListHoursOfOperationOverrides(
	inst, hoursID, token string,
	maxResults int32,
) (*OverrideListOut, error) {
	var stamp Stamp

	items, next, err := listIn(b, "ListHoursOfOperationOverrides", inst, b.hoursOverrides,
		func(o *hoursOverrideRec) (HoursOfOperationOverride, bool) {
			return o.Override, o.Override.HoursOfOperationID == hoursID
		}, token, maxResults)
	if err != nil {
		return nil, err
	}

	if _, err = readInstance(b, "ListHoursOfOperationOverrides", inst, func(i string) (*empty, error) {
		h, herr := b.hours.get(i, hoursID)
		if herr == nil {
			stamp = h.Hours.Stamp
		}

		return nil, herr
	}); err != nil {
		return nil, err
	}

	return &OverrideListOut{
		HoursOfOperationOverrideList: orEmpty(items), NextToken: next,
		LastModifiedTime: stamp.LastModifiedTime, LastModifiedRegion: stamp.LastModifiedRegion,
	}, nil
}

// UpdateHoursOfOperationOverride changes the supplied settings of an override.
func (b *Backend) UpdateHoursOfOperationOverride(
	ctx context.Context,
	in *UpdateHoursOfOperationOverrideInput,
) (*empty, error) {
	return writeInstance(b, "UpdateHoursOfOperationOverride", in.InstanceID, func(inst string) (*empty, error) {
		rec, err := b.overrideOf(inst, in.HoursID, in.OverrideID)
		if err != nil {
			return nil, err
		}

		o := rec.Override
		patch := overrideFromBody(&in.overrideBody)
		applyOverridePatch(&o, &patch, &in.overrideBody)

		if err = checkOverride(&o); err != nil {
			return nil, err
		}

		if overrideNameTaken(b, inst, in.HoursID, o.Name, in.OverrideID) {
			return nil, duplicateErr("HoursOfOperationOverride", o.Name)
		}

		rec.Override = o
		b.hours.table.Range(func(h *hoursRec) bool {
			if h.InstanceID == inst && h.Hours.HoursOfOperationID == in.HoursID {
				h.Hours.Stamp = b.stamp(ctx)
			}

			return true
		})

		return &empty{}, nil
	})
}

func applyOverridePatch(dst, patch *HoursOfOperationOverride, in *overrideBody) {
	for d, s := range map[*string]*string{
		&dst.Name: in.Name, &dst.Description: in.Description, &dst.EffectiveFrom: in.EffectiveFrom,
		&dst.EffectiveTill: in.EffectiveTill, &dst.OverrideType: in.OverrideType,
	} {
		if s != nil {
			*d = *s
		}
	}

	if len(patch.Config) > 0 {
		dst.Config = patch.Config
	}

	if patch.RecurrenceConfig != nil {
		dst.RecurrenceConfig = patch.RecurrenceConfig
	}
}

// DeleteHoursOfOperationOverride removes an override.
func (b *Backend) DeleteHoursOfOperationOverride(inst, hoursID, id string) (*empty, error) {
	return writeInstance(b, "DeleteHoursOfOperationOverride", inst, func(inst string) (*empty, error) {
		if _, err := b.overrideOf(inst, hoursID, id); err != nil {
			return nil, err
		}

		return nil, b.hoursOverrides.del(inst, id)
	})
}

func (b *Backend) hoursIdentifiers(inst string, ids []string) []HoursIdentifier {
	var out []HoursIdentifier

	for _, id := range ids {
		if h, err := b.hours.get(inst, id); err == nil {
			out = append(out, HoursIdentifier{
				Arn: h.Hours.HoursOfOperationArn, ID: id, Name: h.Hours.Name,
			})
		}
	}

	return out
}

// isAncestor reports whether anc is id itself or reachable through id's parents.
func (b *Backend) isAncestor(inst, anc, id string) bool {
	if anc == id {
		return true
	}

	h, err := b.hours.get(inst, id)
	if err != nil {
		return false
	}

	return slices.ContainsFunc(h.Parents, func(p string) bool { return b.isAncestor(inst, anc, p) })
}

// checkParentHours validates parent references and returns their IDs; self is the schedule being changed.
func (b *Backend) checkParentHours(inst, self string, cfgs []ParentHoursConfig) ([]string, error) {
	var ids []string

	for _, c := range cfgs {
		if !b.hours.has(inst, c.HoursOfOperationID) {
			return nil, notFoundErr("HoursOfOperation", c.HoursOfOperationID)
		}

		if self != "" && b.isAncestor(inst, self, c.HoursOfOperationID) {
			return nil, invalidParamErr("a schedule cannot be its own ancestor")
		}

		if !slices.Contains(ids, c.HoursOfOperationID) {
			ids = append(ids, c.HoursOfOperationID)
		}
	}

	if len(ids) > maxHoursParents {
		return nil, invalidParamErr("at most %d parent schedules are allowed", maxHoursParents)
	}

	return ids, nil
}

// AssociateHoursOfOperations adds parent schedules.
func (b *Backend) AssociateHoursOfOperations(ctx context.Context, in *ParentHoursInput) (*empty, error) {
	return writeInstance(b, "AssociateHoursOfOperations", in.InstanceID, func(inst string) (*empty, error) {
		h, err := b.hours.get(inst, in.HoursID)
		if err != nil {
			return nil, err
		}

		if len(in.ParentHoursOfOperationConfigs) == 0 {
			return nil, invalidParamErr("ParentHoursOfOperationConfigs is required")
		}

		add, err := b.checkParentHours(inst, in.HoursID, in.ParentHoursOfOperationConfigs)
		if err != nil {
			return nil, err
		}

		merged := slices.Clone(h.Parents)

		for _, id := range add {
			if !slices.Contains(merged, id) {
				merged = append(merged, id)
			}
		}

		if len(merged) > maxHoursParents {
			return nil, resourceConflictErr("at most %d parent schedules are allowed", maxHoursParents)
		}

		h.Parents = merged
		h.Hours.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// DisassociateHoursOfOperations removes parent schedules.
func (b *Backend) DisassociateHoursOfOperations(ctx context.Context, in *ParentHoursInput) (*empty, error) {
	return writeInstance(b, "DisassociateHoursOfOperations", in.InstanceID, func(inst string) (*empty, error) {
		h, err := b.hours.get(inst, in.HoursID)
		if err != nil {
			return nil, err
		}

		if len(in.ParentHoursOfOperationIDs) == 0 {
			return nil, invalidParamErr("ParentHoursOfOperationIds is required")
		}

		for _, id := range in.ParentHoursOfOperationIDs {
			if !slices.Contains(h.Parents, id) {
				return nil, notFoundErr("ParentHoursOfOperation", id)
			}
		}

		h.Parents = slices.DeleteFunc(slices.Clone(h.Parents), func(p string) bool {
			return slices.Contains(in.ParentHoursOfOperationIDs, p)
		})
		h.Hours.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// ListChildHoursOfOperations pages through the schedules that name hoursID as a parent.
func (b *Backend) ListChildHoursOfOperations(inst, hoursID, token string, maxResults int32) (*ChildHoursOut, error) {
	var stamp Stamp

	items, next, err := listIn(
		b,
		"ListChildHoursOfOperations",
		inst,
		b.hours,
		func(h *hoursRec) (HoursIdentifier, bool) {
			if h.Hours.HoursOfOperationID == hoursID {
				stamp = h.Hours.Stamp
			}

			return HoursIdentifier{
				Arn: h.Hours.HoursOfOperationArn, ID: h.Hours.HoursOfOperationID, Name: h.Hours.Name,
			}, slices.Contains(h.Parents, hoursID)
		},
		token,
		maxResults,
	)
	if err != nil {
		return nil, err
	}

	if _, err = readInstance(b, "ListChildHoursOfOperations", inst, func(i string) (*empty, error) {
		return nil, ignoreFound(b.hours.get(i, hoursID))
	}); err != nil {
		return nil, err
	}

	return &ChildHoursOut{
		ChildHoursOfOperationsSummaryList: orEmpty(items), NextToken: next,
		LastModifiedTime: stamp.LastModifiedTime, LastModifiedRegion: stamp.LastModifiedRegion,
	}, nil
}

func ignoreFound[T any](_ *T, err error) error { return err }

func (o *HoursOfOperationOverride) appliesOn(d time.Time) bool {
	from, err1 := time.Parse(dateLayout, o.EffectiveFrom)
	till, err2 := time.Parse(dateLayout, o.EffectiveTill)

	return err1 == nil && err2 == nil && !d.Before(from) && !d.After(till)
}

func overrideHoursFor(o *HoursOfOperationOverride, day string) []overrideHour {
	switch o.OverrideType {
	case overrideClosed:
		return []overrideHour{{
			End: TimeSlice{Hours: maxHoursInDay, Minutes: maxMinutes}, OperationalStatus: overrideClosed,
			OverrideName: o.Name,
		}}
	case overrideOpen:
		return []overrideHour{{
			End: TimeSlice{Hours: maxHoursInDay, Minutes: maxMinutes}, OperationalStatus: overrideOpen,
			OverrideName: o.Name,
		}}
	}

	var out []overrideHour

	for _, c := range o.Config {
		if c.Day == day {
			out = append(out, overrideHour{
				End: c.EndTime, Start: c.StartTime, OperationalStatus: overrideOpen, OverrideName: o.Name,
			})
		}
	}

	return out
}

// GetEffectiveHoursOfOperations resolves the schedule plus its overrides over a date range.
func (b *Backend) GetEffectiveHoursOfOperations(in *EffectiveHoursInput) (*EffectiveHoursOut, error) {
	from, err := parseDate("fromDate", in.FromDate)
	if err != nil {
		return nil, err
	}

	to, err := parseDate("toDate", in.ToDate)
	if err != nil {
		return nil, err
	}

	if to.Before(from) || to.Sub(from) > maxEffectiveDays*24*time.Hour {
		return nil, invalidParamErr("toDate must be within %d days after fromDate", maxEffectiveDays)
	}

	return readInstance(
		b,
		"GetEffectiveHoursOfOperations",
		in.InstanceID,
		func(inst string) (*EffectiveHoursOut, error) {
			h, herr := b.hours.get(inst, in.HoursID)
			if herr != nil {
				return nil, herr
			}

			var overrides []*HoursOfOperationOverride

			for _, o := range b.hoursOverrides.in(inst) {
				if o.Override.HoursOfOperationID == in.HoursID {
					overrides = append(overrides, &o.Override)
				}
			}

			out := &EffectiveHoursOut{
				TimeZone: h.Hours.TimeZone, EffectiveHoursOfOperationList: []effectiveHours{},
				EffectiveOverrideHoursList: []effectiveOverrideHours{},
			}

			for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
				eff, ov := effectiveDay(&h.Hours, overrides, d)
				out.EffectiveHoursOfOperationList = append(out.EffectiveHoursOfOperationList, eff)

				if len(ov.OverrideHours) > 0 {
					out.EffectiveOverrideHoursList = append(out.EffectiveOverrideHoursList, ov)
				}
			}

			return out, nil
		},
	)
}

func effectiveDay(
	h *HoursOfOperation, overrides []*HoursOfOperationOverride, d time.Time,
) (effectiveHours, effectiveOverrideHours) {
	date := d.Format(dateLayout)
	day := weekDays()[d.Weekday()]
	eff := effectiveHours{Date: date, OperationalHours: []operationalHour{}}
	ov := effectiveOverrideHours{Date: date, OverrideHours: []overrideHour{}}

	for _, o := range overrides {
		if o.appliesOn(d) {
			ov.OverrideHours = append(ov.OverrideHours, overrideHoursFor(o, day)...)
		}
	}

	if len(ov.OverrideHours) > 0 {
		for _, oh := range ov.OverrideHours {
			if oh.OperationalStatus == overrideOpen {
				eff.OperationalHours = append(eff.OperationalHours, operationalHour{End: oh.End, Start: oh.Start})
			}
		}

		return eff, ov
	}

	for _, c := range h.Config {
		if c.Day == day {
			eff.OperationalHours = append(eff.OperationalHours, operationalHour{End: c.EndTime, Start: c.StartTime})
		}
	}

	return eff, ov
}
