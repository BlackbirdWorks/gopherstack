package rum

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// CFNMetricDestination is one AppMonitorConfiguration.MetricDestinations entry of AWS::RUM::AppMonitor.
type CFNMetricDestination struct {
	Destination    string
	DestinationARN string
	IAMRoleARN     string
	Definitions    []WireMetricRequest
}

// CFNAppMonitor is the AWS::RUM::AppMonitor property set.
type CFNAppMonitor struct {
	Tags             map[string]string
	Config           *MonitorConfig
	SourceMaps       *SourceMaps
	CwLogEnabled     *bool
	Name             string
	Platform         string
	Domain           string
	CustomEvents     string
	PolicyDocument   string
	PolicyRevisionID string
	DomainList       []string
	Destinations     []CFNMetricDestination
}

func (s CFNAppMonitor) customEvents() *wireCustomEvents {
	if s.CustomEvents == "" {
		return nil
	}

	return &wireCustomEvents{Status: s.CustomEvents}
}

func (s CFNAppMonitor) sourceMaps() *wireSourceMaps {
	if s.SourceMaps == nil {
		return nil
	}

	return &wireSourceMaps{JavaScriptSourceMaps: s.SourceMaps}
}

// CFNCreate creates an app monitor with its metric destinations and resource policy, returning the monitor ID.
func (b *InMemoryBackend) CFNCreate(ctx context.Context, spec CFNAppMonitor) (string, error) {
	id, err := b.CreateAppMonitor(ctx, CreateMonitorInput{
		Config: spec.Config, CustomEvents: spec.customEvents(), SourceMaps: spec.sourceMaps(), Tags: spec.Tags,
		CwLogEnabled: spec.CwLogEnabled, Name: spec.Name, Platform: spec.Platform, Domain: spec.Domain,
		DomainList: spec.DomainList,
	})
	if err != nil {
		return "", err
	}

	if err = b.applyDestinations(ctx, spec.Name, nil, spec.Destinations); err == nil && spec.PolicyDocument != "" {
		_, _, err = b.PutResourcePolicy(ctx, spec.Name, spec.PolicyDocument, "")
	}

	if err != nil {
		_ = b.DeleteAppMonitor(ctx, spec.Name)

		return "", err
	}

	return id, nil
}

// CFNUpdate applies the mutable properties of AWS::RUM::AppMonitor to an existing monitor.
func (b *InMemoryBackend) CFNUpdate(ctx context.Context, spec CFNAppMonitor) error {
	err := b.UpdateAppMonitor(ctx, UpdateMonitorInput{
		Config: spec.Config, CustomEvents: spec.customEvents(), SourceMaps: spec.sourceMaps(),
		CwLogEnabled: spec.CwLogEnabled, Name: spec.Name, Domain: spec.Domain, DomainList: spec.DomainList,
	})
	if err != nil {
		return err
	}

	monitorARN := b.monitorARN(ctx, spec.Name)

	current, err := b.ListTagsForResource(ctx, monitorARN)
	if err != nil {
		return err
	}

	if removed := removedKeys(current, spec.Tags); len(removed) > 0 {
		if err = b.UntagResource(ctx, monitorARN, removed); err != nil {
			return err
		}
	}

	if len(spec.Tags) > 0 {
		if err = b.TagResource(ctx, monitorARN, spec.Tags); err != nil {
			return err
		}
	}

	if err = b.reconcileDestinations(ctx, spec); err != nil {
		return err
	}

	return b.reconcilePolicy(ctx, spec)
}

// CFNDelete removes an app monitor by name.
func (b *InMemoryBackend) CFNDelete(ctx context.Context, name string) error {
	return b.DeleteAppMonitor(ctx, name)
}

// CFNNotFound reports whether err says the app monitor does not exist.
func CFNNotFound(err error) bool {
	re, ok := errors.AsType[*restroute.Error](err)

	return ok && re.Code == "ResourceNotFoundException"
}

func (b *InMemoryBackend) monitorARN(ctx context.Context, name string) string {
	sc := b.scopeOf(ctx)

	return arn.Build(serviceName, sc.region, sc.account, monitorResourcePrefix+name)
}

func removedKeys(current, desired map[string]string) []string {
	var out []string

	for k := range current {
		if _, keep := desired[k]; !keep {
			out = append(out, k)
		}
	}

	slices.Sort(out)

	return out
}

func (b *InMemoryBackend) reconcilePolicy(ctx context.Context, spec CFNAppMonitor) error {
	doc, revision, err := b.GetResourcePolicy(ctx, spec.Name)

	switch {
	case spec.PolicyDocument != "" && (err != nil || doc != spec.PolicyDocument):
		_, _, perr := b.PutResourcePolicy(ctx, spec.Name, spec.PolicyDocument, spec.PolicyRevisionID)

		return perr
	case spec.PolicyDocument == "" && err == nil:
		_, derr := b.DeleteResourcePolicy(ctx, spec.Name, revision)

		return derr
	default:
		return nil
	}
}

func (b *InMemoryBackend) reconcileDestinations(ctx context.Context, spec CFNAppMonitor) error {
	sc := b.scopeOf(ctx)

	b.mu.RLock("reconcileDestinations")
	m, err := b.monitorByNameLocked(sc, spec.Name)

	var current []CFNMetricDestination
	if err == nil {
		current = b.destinationsLocked(m)
	}

	b.mu.RUnlock()

	if err != nil {
		return err
	}

	want := normalizeDestinations(spec.Destinations)
	if reflect.DeepEqual(current, want) {
		return nil
	}

	return b.applyDestinations(ctx, spec.Name, current, spec.Destinations)
}

func (b *InMemoryBackend) destinationsLocked(m *AppMonitor) []CFNMetricDestination {
	var out []CFNMetricDestination

	b.destinations.Range(func(d *Destination) bool {
		if d.AppID != m.ID {
			return true
		}

		cd := CFNMetricDestination{
			Destination: d.Destination, DestinationARN: d.DestinationARN, IAMRoleARN: d.IAMRoleARN,
		}

		for _, def := range b.defsByApp.Get(m.ID) {
			if def.Destination == d.Destination && def.DestinationARN == d.DestinationARN {
				cd.Definitions = append(cd.Definitions, WireMetricRequest{
					DimensionKeys: def.DimensionKeys, Name: def.Name, EventPattern: def.EventPattern,
					Namespace: def.Namespace, UnitLabel: def.UnitLabel, ValueKey: def.ValueKey,
				})
			}
		}

		out = append(out, cd)

		return true
	})

	return normalizeDestinations(out)
}

func normalizeDestinations(in []CFNMetricDestination) []CFNMetricDestination {
	out := make([]CFNMetricDestination, len(in))

	for i, d := range in {
		d.Definitions = slices.Clone(d.Definitions)

		for j := range d.Definitions {
			if len(d.Definitions[j].DimensionKeys) == 0 {
				d.Definitions[j].DimensionKeys = nil
			} else {
				d.Definitions[j].DimensionKeys = maps.Clone(d.Definitions[j].DimensionKeys)
			}
		}

		slices.SortFunc(d.Definitions, func(a, z WireMetricRequest) int { return cmp.Compare(a.Name, z.Name) })
		out[i] = d
	}

	slices.SortFunc(out, func(a, z CFNMetricDestination) int {
		return cmp.Or(cmp.Compare(a.Destination, z.Destination), cmp.Compare(a.DestinationARN, z.DestinationARN))
	})

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) applyDestinations(
	ctx context.Context, name string, current, desired []CFNMetricDestination,
) error {
	for _, d := range current {
		if err := b.DeleteRumMetricsDestination(ctx, name, d.Destination, d.DestinationARN); err != nil {
			return err
		}
	}

	for _, d := range desired {
		if err := b.PutRumMetricsDestination(ctx, name, d.Destination, d.DestinationARN, d.IAMRoleARN); err != nil {
			return err
		}

		if len(d.Definitions) == 0 {
			continue
		}

		_, failed, err := b.BatchCreateRumMetricDefinitions(ctx, name, d.Destination, d.DestinationARN, d.Definitions)
		if err != nil {
			return err
		}

		if len(failed) > 0 {
			return invalid(
				"metric definition %s was rejected: %s", failed[0].MetricDefinition.Name, failed[0].ErrorMessage,
			)
		}
	}

	return nil
}
