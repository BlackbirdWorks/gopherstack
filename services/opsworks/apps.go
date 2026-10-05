package opsworks

import (
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
)

// isValidAppType reports whether appType is one of the exact AppType enum
// values from aws-sdk-go-v2/service/opsworks/types.AppType.Values() --
// CreateApp's Type member on the real API is restricted to this set, not a
// free string.
func isValidAppType(appType string) bool {
	switch appType {
	case "aws-flow-ruby", "java", "rails", "php", "nodejs", "static", "other":
		return true
	default:
		return false
	}
}

// CreateApp creates a new app in a stack. Name, StackId, and Type are all
// "This member is required" on the real CreateAppInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_CreateApp.go), and Type is
// restricted to the AppType enum, not a free string.
func (b *InMemoryBackend) CreateApp(stackID, name, appType string, opts AppOptions) (*App, error) {
	if name == "" || stackID == "" || !isValidAppType(appType) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	a := &storedApp{
		CreatedAt: now,
		StackID:   stackID,
		AppID:     id,
		Arn:       b.appARN(id),
		Name:      name,
		Type:      appType,
	}
	if o := cloneAppOptions(opts); !isZeroAppOptions(o) {
		a.Options = &o
	}
	b.apps.Put(a)

	return a.toApp(), nil
}

// DescribeApps returns apps filtered by stack and/or app IDs.
func (b *InMemoryBackend) DescribeApps(stackID string, appIDs []string) ([]*App, error) {
	b.mu.RLock("DescribeApps")
	defer b.mu.RUnlock()

	if len(appIDs) > 0 {
		result := make([]*App, 0, len(appIDs))
		for _, id := range appIDs {
			a, ok := b.apps.Get(id)
			if !ok {
				return nil, ErrAppNotFound
			}
			result = append(result, a.toApp())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.apps.All, b.appsByStack.Get)

	result := make([]*App, 0, len(source))
	for _, a := range source {
		result = append(result, a.toApp())
	}

	return result, nil
}

// UpdateApp updates an app's name, type and any optional member that is set.
func (b *InMemoryBackend) UpdateApp(appID, name, appType string, opts AppOptions) error {
	if appType != "" && !isValidAppType(appType) {
		return ErrValidation
	}

	b.mu.Lock("UpdateApp")
	defer b.mu.Unlock()

	a, ok := b.apps.Get(appID)
	if !ok {
		return ErrAppNotFound
	}

	if name != "" {
		a.Name = name
	}

	if appType != "" {
		a.Type = appType
	}

	merged := mergeAppOptions(a.Options, opts)
	if isZeroAppOptions(merged) {
		a.Options = nil
	} else {
		a.Options = &merged
	}

	return nil
}

func isZeroAppOptions(o AppOptions) bool {
	return len(o.Attributes) == 0 && o.AppSource == nil && o.SslConfiguration == nil && o.EnableSsl == nil &&
		len(o.DataSources) == 0 && len(o.Environment) == 0 && len(o.Domains) == 0 &&
		o.Description == "" && o.Shortname == ""
}

func cloneAppOptions(o AppOptions) AppOptions {
	out := o
	out.Attributes = maps.Clone(o.Attributes)
	out.Domains = slices.Clone(o.Domains)
	out.DataSources = slices.Clone(o.DataSources)
	out.Environment = slices.Clone(o.Environment)

	if o.AppSource != nil {
		s := *o.AppSource
		out.AppSource = &s
	}

	if o.SslConfiguration != nil {
		s := *o.SslConfiguration
		out.SslConfiguration = &s
	}

	return out
}

// mergeAppOptions overlays the set members of upd on cur; lists and maps replace wholesale.
func mergeAppOptions(cur *AppOptions, upd AppOptions) AppOptions {
	var out AppOptions
	if cur != nil {
		out = cloneAppOptions(*cur)
	}

	u := cloneAppOptions(upd)
	if u.Attributes != nil {
		out.Attributes = u.Attributes
	}

	if u.AppSource != nil {
		out.AppSource = u.AppSource
	}

	if u.SslConfiguration != nil {
		out.SslConfiguration = u.SslConfiguration
	}

	if u.EnableSsl != nil {
		out.EnableSsl = u.EnableSsl
	}

	if u.DataSources != nil {
		out.DataSources = u.DataSources
	}

	if u.Environment != nil {
		out.Environment = u.Environment
	}

	if u.Domains != nil {
		out.Domains = u.Domains
	}

	if u.Description != "" {
		out.Description = u.Description
	}

	if u.Shortname != "" {
		out.Shortname = u.Shortname
	}

	return out
}

// DeleteApp deletes an app.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	if !b.apps.Delete(appID) {
		return ErrAppNotFound
	}

	return nil
}
