package connect

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

func weekDays() []string {
	return []string{
		"SUNDAY",
		"MONDAY",
		"TUESDAY",
		"WEDNESDAY",
		"THURSDAY",
		"FRIDAY",
		"SATURDAY",
	}
}

const (
	maxNameLen    = 127
	maxHoursInDay = 23
	maxMinutes    = 59
)

// CreateHoursOfOperationInput is the CreateHoursOfOperation request.
type CreateHoursOfOperationInput struct {
	Tags        map[string]string
	Name        string
	Description string
	TimeZone    string
	instPath
	Config                        []HoursOfOperationConfig
	ParentHoursOfOperationConfigs []ParentHoursConfig
}

// UpdateHoursOfOperationInput is the UpdateHoursOfOperation request.
type UpdateHoursOfOperationInput struct {
	Name        *string
	Description *string
	TimeZone    *string
	HoursID     string `json:"-" path:"HoursOfOperationId"`
	instPath
	Config []HoursOfOperationConfig
}

func checkHoursConfig(cfg []HoursOfOperationConfig) error {
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

func checkTimeZone(tz string) error {
	if _, err := time.LoadLocation(tz); err != nil || tz == "" {
		return invalidParamErr("TimeZone %q is not a valid time zone", tz)
	}

	return nil
}

func nameExists[V any](f family[V], inst string, nameOf func(*V) string, name, exceptID string) bool {
	for _, v := range f.in(inst) {
		if strings.EqualFold(nameOf(v), name) && f.idOf(v) != exceptID {
			return true
		}
	}

	return false
}

// CreateHoursOfOperation creates a weekly schedule.
func (b *Backend) CreateHoursOfOperation(ctx context.Context, in *CreateHoursOfOperationInput) (*idArnOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := checkTimeZone(in.TimeZone); err != nil {
		return nil, err
	}

	if len(in.Config) == 0 {
		return nil, invalidParamErr("Config is required")
	}

	if err := firstError(
		checkHoursConfig(in.Config),
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateHoursOfOperation")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	if nameExists(b.hours, r.Instance.ID, func(h *hoursRec) string { return h.Hours.Name }, in.Name, "") {
		return nil, duplicateErr("HoursOfOperation", in.Name)
	}

	parents, err := b.checkParentHours(r.Instance.ID, "", in.ParentHoursOfOperationConfigs)
	if err != nil {
		return nil, err
	}

	rec := b.newHours(ctx, r.Instance.ID, in.Name, in.Description, in.TimeZone, in.Config, in.Tags)
	rec.Parents = parents
	b.hours.put(rec)

	return &idArnOut{ID: rec.Hours.HoursOfOperationID, Arn: rec.Hours.HoursOfOperationArn}, nil
}

func (b *Backend) newHours(
	ctx context.Context, inst, name, desc, tz string, cfg []HoursOfOperationConfig, tags map[string]string,
) *hoursRec {
	id := uuid.NewString()

	return &hoursRec{InstanceID: inst, Hours: HoursOfOperation{
		Stamp: b.stamp(ctx), Tags: cloneTags(tags), Description: desc, Name: name, TimeZone: tz, Config: cfg,
		HoursOfOperationArn: b.buildARN(ctx, "instance/"+inst+"/operating-hours/"+id), HoursOfOperationID: id,
	}}
}

// DescribeHoursOfOperation returns a schedule.
func (b *Backend) DescribeHoursOfOperation(inst, id string) (*HoursOfOperation, error) {
	b.mu.RLock("DescribeHoursOfOperation")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	h, err := b.hours.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := h.Hours
	out.Tags = cloneTags(out.Tags)
	out.ParentHoursOfOperations = b.hoursIdentifiers(r.Instance.ID, h.Parents)

	return &out, nil
}

// ListHoursOfOperations pages through schedules.
func (b *Backend) ListHoursOfOperations(inst, token string, maxResults int32) ([]Summary, string, error) {
	b.mu.RLock("ListHoursOfOperations")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []Summary
	for _, h := range b.hours.in(r.Instance.ID) {
		all = append(
			all,
			Summary{
				Stamp: h.Hours.Stamp,
				Arn:   h.Hours.HoursOfOperationArn,
				ID:    h.Hours.HoursOfOperationID,
				Name:  h.Hours.Name,
			},
		)
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateHoursOfOperation changes the supplied settings of a schedule.
func (b *Backend) UpdateHoursOfOperation(ctx context.Context, in *UpdateHoursOfOperationInput) (*empty, error) {
	if in.TimeZone != nil {
		if err := checkTimeZone(*in.TimeZone); err != nil {
			return nil, err
		}
	}

	if err := checkHoursConfig(in.Config); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateHoursOfOperation")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	h, err := b.hours.get(r.Instance.ID, in.HoursID)
	if err != nil {
		return nil, err
	}

	if in.Name != nil {
		if err = checkName("Name", *in.Name, maxNameLen); err != nil {
			return nil, err
		}

		if nameExists(b.hours, r.Instance.ID, func(x *hoursRec) string { return x.Hours.Name }, *in.Name, in.HoursID) {
			return nil, duplicateErr("HoursOfOperation", *in.Name)
		}

		h.Hours.Name = *in.Name
	}

	setIf(&h.Hours.Description, in.Description)
	setIf(&h.Hours.TimeZone, in.TimeZone)

	if len(in.Config) > 0 {
		h.Hours.Config = in.Config
	}

	h.Hours.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

func setIf[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

// DeleteHoursOfOperation removes a schedule that no queue uses.
func (b *Backend) DeleteHoursOfOperation(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteHoursOfOperation")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(
		b.queues.in(r.Instance.ID),
		func(q *queueRec) bool { return q.Queue.HoursOfOperationID == id },
	) {
		return nil, inUseErr("HoursOfOperation %q is used by a queue", id)
	}

	if slices.ContainsFunc(
		b.hours.in(r.Instance.ID),
		func(h *hoursRec) bool { return slices.Contains(h.Parents, id) },
	) {
		return nil, inUseErr("HoursOfOperation %q is the parent of another schedule", id)
	}

	for _, o := range b.hoursOverrides.in(r.Instance.ID) {
		if o.Override.HoursOfOperationID == id {
			b.hoursOverrides.table.Delete(r.Instance.ID + "/" + o.Override.HoursOfOperationOverrideID)
		}
	}

	return nil, b.hours.del(r.Instance.ID, id)
}
