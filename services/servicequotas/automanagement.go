package servicequotas

import (
	"context"
	"slices"
	"strings"
)

// ExclusionEntry is an excluded quota with its catalogued name.
type ExclusionEntry struct{ Code, Name string }

// AutoManagementView is the automatic management configuration with resolved exclusion names.
type AutoManagementView struct {
	Exclusions map[string][]ExclusionEntry
	Config     AutoManagement
}

// AutoManagementInput carries the editable settings of StartAutoManagement and UpdateAutoManagement.
type AutoManagementInput struct {
	Exclusions      map[string][]string
	NotificationArn *string
	OptInLevel      string
	OptInType       string
}

func autoKey(s scope) string { return s.account + "|" + s.region }

func validateOptInType(t string) error {
	if t != "NotifyOnly" && t != "NotifyAndAdjust" {
		return fail(ErrIllegalArgument, "OptInType %q must be NotifyOnly or NotifyAndAdjust", t)
	}

	return nil
}

func validateNotificationArn(v *string) error {
	if v == nil {
		return nil
	}

	parts := strings.SplitN(*v, ":", 6) //nolint:mnd // ARN has six fields
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sns" || parts[5] == "" {
		return fail(ErrIllegalArgument, "NotificationArn %q is not a valid SNS topic ARN", *v)
	}

	return nil
}

func (b *InMemoryBackend) validateExclusionsLocked(ex map[string][]string) error {
	for svcCode, codes := range ex {
		for _, c := range codes {
			if _, _, err := b.lookupQuotaLocked(svcCode, c); err != nil {
				return fail(ErrIllegalArgument, "exclusion %s/%s does not name a known quota", svcCode, c)
			}
		}
	}

	return nil
}

// StartAutoManagement opts the account into automatic quota management.
func (b *InMemoryBackend) StartAutoManagement(ctx context.Context, in AutoManagementInput) error {
	if in.OptInLevel != levelAccount {
		return fail(ErrIllegalArgument, "OptInLevel must be %s", levelAccount)
	}

	if err := validateOptInType(in.OptInType); err != nil {
		return err
	}

	if err := validateNotificationArn(in.NotificationArn); err != nil {
		return err
	}

	b.mu.Lock("StartAutoManagement")
	defer b.mu.Unlock()

	if err := b.validateExclusionsLocked(in.Exclusions); err != nil {
		return err
	}

	s := b.scopeOf(ctx)
	if cur, ok := b.autoMgmt.Get(autoKey(s)); ok && cur.Status == optInEnabled {
		return fail(ErrResourceAlreadyExists, "Automatic quota management is already enabled")
	}

	am := &AutoManagement{
		Key: autoKey(s), Account: s.account, Region: s.region, OptInLevel: in.OptInLevel,
		OptInType: in.OptInType, Status: optInEnabled, Exclusions: cloneExclusions(in.Exclusions),
	}

	if in.NotificationArn != nil {
		am.NotificationArn = *in.NotificationArn
	}

	b.autoMgmt.Put(am)

	return nil
}

// StopAutoManagement opts the account out of automatic quota management.
func (b *InMemoryBackend) StopAutoManagement(ctx context.Context) error {
	b.mu.Lock("StopAutoManagement")
	defer b.mu.Unlock()

	key := autoKey(b.scopeOf(ctx))
	if cur, ok := b.autoMgmt.Get(key); !ok || cur.Status != optInEnabled {
		return fail(ErrNoSuchResource, "Automatic quota management is not enabled")
	}

	b.autoMgmt.Delete(key)

	return nil
}

// UpdateAutoManagement changes the settings of an enabled configuration.
func (b *InMemoryBackend) UpdateAutoManagement(ctx context.Context, in AutoManagementInput) error {
	if in.OptInType == "" && in.Exclusions == nil && in.NotificationArn == nil {
		return fail(ErrIllegalArgument, "at least one of OptInType, ExclusionList or NotificationArn is required")
	}

	if in.OptInType != "" {
		if err := validateOptInType(in.OptInType); err != nil {
			return err
		}
	}

	if err := validateNotificationArn(in.NotificationArn); err != nil {
		return err
	}

	b.mu.Lock("UpdateAutoManagement")
	defer b.mu.Unlock()

	if err := b.validateExclusionsLocked(in.Exclusions); err != nil {
		return err
	}

	cur, ok := b.autoMgmt.Get(autoKey(b.scopeOf(ctx)))
	if !ok || cur.Status != optInEnabled {
		return fail(ErrNoSuchResource, "Automatic quota management is not enabled")
	}

	if in.OptInType != "" {
		cur.OptInType = in.OptInType
	}

	if in.Exclusions != nil {
		cur.Exclusions = cloneExclusions(in.Exclusions)
	}

	if in.NotificationArn != nil {
		cur.NotificationArn = *in.NotificationArn
	}

	return nil
}

// GetAutoManagement returns the configuration; a never-started account reports DISABLED.
func (b *InMemoryBackend) GetAutoManagement(ctx context.Context) AutoManagementView {
	b.mu.RLock("GetAutoManagementConfiguration")
	defer b.mu.RUnlock()

	cur, ok := b.autoMgmt.Get(autoKey(b.scopeOf(ctx)))
	if !ok {
		return AutoManagementView{Config: AutoManagement{Status: optInDisabled}}
	}

	view := AutoManagementView{Config: *cur, Exclusions: map[string][]ExclusionEntry{}}
	view.Config.Exclusions = cloneExclusions(cur.Exclusions)

	for svcCode, codes := range cur.Exclusions {
		for _, c := range codes {
			name := ""
			if _, def, found := b.catalog.Quota(svcCode, c); found {
				name = def.Name
			}

			view.Exclusions[svcCode] = append(view.Exclusions[svcCode], ExclusionEntry{Code: c, Name: name})
		}
	}

	return view
}

func cloneExclusions(in map[string][]string) map[string][]string {
	if in == nil {
		return nil
	}

	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = slices.Clone(v)
	}

	return out
}
