package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- CloudWatch Alarm Template Group operations ---

func (b *InMemoryBackend) findCWAlarmTemplateGroup(
	identifier string,
) (*storedCloudWatchAlarmTemplateGroup, bool) {
	for _, g := range b.cwAlarmTemplateGroups.All() {
		if g.ID == identifier || g.Arn == identifier || g.Name == identifier {
			return g, true
		}
	}

	return nil, false
}

// CreateCloudWatchAlarmTemplateGroup creates a new CW alarm template group.
func (b *InMemoryBackend) CreateCloudWatchAlarmTemplateGroup(
	name, description string, tags map[string]string,
) (*CloudWatchAlarmTemplateGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	id := newID()
	now := time.Now().UTC()
	g := &storedCloudWatchAlarmTemplateGroup{
		Tags:        copyTags(tags),
		Arn:         b.cwAlarmTemplateGroupARN(id),
		ID:          id,
		Name:        name,
		Description: description,
		CreatedAt:   now,
		ModifiedAt:  now,
	}
	b.mu.Lock("CreateCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	b.cwAlarmTemplateGroups.Put(g)

	return g.toGroup(), nil
}

// GetCloudWatchAlarmTemplateGroup returns a CW alarm template group by identifier.
func (b *InMemoryBackend) GetCloudWatchAlarmTemplateGroup(
	identifier string,
) (*CloudWatchAlarmTemplateGroup, error) {
	b.mu.RLock("GetCloudWatchAlarmTemplateGroup")
	defer b.mu.RUnlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return g.toGroup(), nil
}

// ListCloudWatchAlarmTemplateGroups returns all CW alarm template groups,
// each annotated with its live templateCount (see
// CloudWatchAlarmTemplateGroupSummary's doc comment).
func (b *InMemoryBackend) ListCloudWatchAlarmTemplateGroups(
	maxResults int,
	nextToken string,
) ([]*CloudWatchAlarmTemplateGroupSummary, string, error) {
	b.mu.RLock("ListCloudWatchAlarmTemplateGroups")
	defer b.mu.RUnlock()
	all := b.cwAlarmTemplateGroups.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*CloudWatchAlarmTemplateGroupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		result = append(result, &CloudWatchAlarmTemplateGroupSummary{
			CloudWatchAlarmTemplateGroup: *g.toGroup(),
			TemplateCount:                b.countCWAlarmTemplatesForGroup(g.ID),
		})
	}

	return result, pg.Next, nil
}

// countCWAlarmTemplatesForGroup returns the number of CloudWatch alarm
// templates belonging to groupID. Caller must already hold b.mu (Lock or
// RLock).
func (b *InMemoryBackend) countCWAlarmTemplatesForGroup(groupID string) int32 {
	var n int32

	for _, t := range b.cwAlarmTemplates.All() {
		if t.GroupID == groupID {
			n++
		}
	}

	return n
}

// UpdateCloudWatchAlarmTemplateGroup updates a CW alarm template group.
func (b *InMemoryBackend) UpdateCloudWatchAlarmTemplateGroup(
	identifier, name, description string,
) (*CloudWatchAlarmTemplateGroup, error) {
	b.mu.Lock("UpdateCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		g.Name = name
	}
	if description != "" {
		g.Description = description
	}
	g.ModifiedAt = time.Now().UTC()

	return g.toGroup(), nil
}

// DeleteCloudWatchAlarmTemplateGroup deletes a CW alarm template group.
func (b *InMemoryBackend) DeleteCloudWatchAlarmTemplateGroup(identifier string) error {
	b.mu.Lock("DeleteCloudWatchAlarmTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findCWAlarmTemplateGroup(identifier)
	if !ok {
		return fmt.Errorf(
			"%w: cloudwatch alarm template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	b.cwAlarmTemplateGroups.Delete(g.ID)
	delete(b.tags, g.Arn)

	return nil
}

// --- CloudWatch Alarm Template operations ---

func (b *InMemoryBackend) findCWAlarmTemplate(
	identifier string,
) (*storedCloudWatchAlarmTemplate, bool) {
	for _, t := range b.cwAlarmTemplates.All() {
		if t.ID == identifier || t.Arn == identifier || t.Name == identifier {
			return t, true
		}
	}

	return nil, false
}

// CreateCloudWatchAlarmTemplate creates a new CW alarm template.
func (b *InMemoryBackend) CreateCloudWatchAlarmTemplate(
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
	tags map[string]string,
) (*CloudWatchAlarmTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	groupID := groupIdentifier
	b.mu.Lock("CreateCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	if g, ok := b.findCWAlarmTemplateGroup(groupIdentifier); ok {
		groupID = g.ID
	}
	id := newID()
	now := time.Now().UTC()
	t := &storedCloudWatchAlarmTemplate{
		Tags: copyTags(
			tags,
		), Arn: b.cwAlarmTemplateARN(id), ID: id, Name: name, Description: description,
		GroupID: groupID, GroupIdentifier: groupIdentifier, MetricName: metricName, Namespace: namespace,
		Statistic: statistic, ComparisonOperator: comparisonOperator, TargetResourceType: targetResourceType,
		TreatMissingData: treatMissingData, Threshold: threshold,
		EvaluationPeriods: evaluationPeriods, DatapointsToAlarm: datapointsToAlarm, Period: period,
		CreatedAt: now, ModifiedAt: now,
	}
	b.cwAlarmTemplates.Put(t)

	return t.toTemplate(), nil
}

// GetCloudWatchAlarmTemplate returns a CW alarm template by identifier.
func (b *InMemoryBackend) GetCloudWatchAlarmTemplate(
	identifier string,
) (*CloudWatchAlarmTemplate, error) {
	b.mu.RLock("GetCloudWatchAlarmTemplate")
	defer b.mu.RUnlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return t.toTemplate(), nil
}

// ListCloudWatchAlarmTemplates returns all CW alarm templates.
func (b *InMemoryBackend) ListCloudWatchAlarmTemplates(
	maxResults int,
	nextToken string,
) ([]*CloudWatchAlarmTemplate, string, error) {
	b.mu.RLock("ListCloudWatchAlarmTemplates")
	defer b.mu.RUnlock()
	all := b.cwAlarmTemplates.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*CloudWatchAlarmTemplate, 0, len(pg.Data))
	for _, t := range pg.Data {
		result = append(result, t.toTemplate())
	}

	return result, pg.Next, nil
}

func (b *InMemoryBackend) updateCWTemplateFields(
	t *storedCloudWatchAlarmTemplate,
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
) {
	if name != "" {
		t.Name = name
	}
	if description != "" {
		t.Description = description
	}
	if groupIdentifier != "" {
		t.GroupIdentifier = groupIdentifier
		if g, ok := b.findCWAlarmTemplateGroup(groupIdentifier); ok {
			t.GroupID = g.ID
		} else {
			t.GroupID = groupIdentifier
		}
	}
	if metricName != "" {
		t.MetricName = metricName
	}
	if namespace != "" {
		t.Namespace = namespace
	}
	if statistic != "" {
		t.Statistic = statistic
	}
	if comparisonOperator != "" {
		t.ComparisonOperator = comparisonOperator
	}
	if targetResourceType != "" {
		t.TargetResourceType = targetResourceType
	}
	if treatMissingData != "" {
		t.TreatMissingData = treatMissingData
	}
	if threshold != 0 {
		t.Threshold = threshold
	}
	if evaluationPeriods != 0 {
		t.EvaluationPeriods = evaluationPeriods
	}
	if datapointsToAlarm != 0 {
		t.DatapointsToAlarm = datapointsToAlarm
	}
	if period != 0 {
		t.Period = period
	}
	t.ModifiedAt = time.Now().UTC()
}

// UpdateCloudWatchAlarmTemplate updates a CW alarm template.
func (b *InMemoryBackend) UpdateCloudWatchAlarmTemplate(
	identifier string,
	name string,
	description string,
	groupIdentifier string,
	metricName string,
	namespace string,
	statistic string,
	comparisonOperator string,
	targetResourceType string,
	treatMissingData string,
	threshold float64,
	evaluationPeriods, datapointsToAlarm, period int32,
) (*CloudWatchAlarmTemplate, error) {
	b.mu.Lock("UpdateCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: cloudwatch alarm template %s not found",
			ErrNotFound,
			identifier,
		)
	}
	b.updateCWTemplateFields(
		t,
		name,
		description,
		groupIdentifier,
		metricName,
		namespace,
		statistic,
		comparisonOperator,
		targetResourceType,
		treatMissingData,
		threshold,
		evaluationPeriods,
		datapointsToAlarm,
		period,
	)

	return t.toTemplate(), nil
}

// DeleteCloudWatchAlarmTemplate deletes a CW alarm template.
func (b *InMemoryBackend) DeleteCloudWatchAlarmTemplate(identifier string) error {
	b.mu.Lock("DeleteCloudWatchAlarmTemplate")
	defer b.mu.Unlock()
	t, ok := b.findCWAlarmTemplate(identifier)
	if !ok {
		return fmt.Errorf("%w: cloudwatch alarm template %s not found", ErrNotFound, identifier)
	}
	b.cwAlarmTemplates.Delete(t.ID)
	delete(b.tags, t.Arn)

	return nil
}
