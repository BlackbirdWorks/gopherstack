package kafkaconnect

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func workerConfigurationARN(region, accountID, name string) string {
	return arn.Build(
		"kafkaconnect", region, accountID, fmt.Sprintf("worker-configuration/%s/%s", name, uuid.NewString()),
	)
}

// CreateWorkerConfiguration creates a worker configuration, always at
// revision 1 and ACTIVE immediately -- UpdateWorkerConfiguration (which
// would create later revisions) is not part of the AWS API surface this
// backend implements; see PARITY.md.
func (b *InMemoryBackend) CreateWorkerConfiguration(
	accountID, region, name, description, propertiesFileContent string,
	tags map[string]string,
) (*WorkerConfiguration, error) {
	if name == "" || propertiesFileContent == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateWorkerConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.workerConfigurationByName(name); ok {
		return nil, ErrWorkerConfigNameInUse
	}

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	now := time.Now().UTC()

	w := &WorkerConfiguration{
		Name:         name,
		ARN:          workerConfigurationARN(region, accountID, name),
		Description:  description,
		State:        workerConfigurationStateActive,
		CreationTime: now,
		Tags:         t,
		LatestRevision: WorkerConfigRevision{
			Revision:              1,
			Description:           description,
			PropertiesFileContent: propertiesFileContent,
			CreationTime:          now,
		},
	}

	b.workerConfigurations.Put(w)

	return w.clone(), nil
}

// DescribeWorkerConfiguration returns the current information about a worker configuration.
func (b *InMemoryBackend) DescribeWorkerConfiguration(workerConfigurationArn string) (*WorkerConfiguration, error) {
	b.mu.RLock("DescribeWorkerConfiguration")
	defer b.mu.RUnlock()

	w, ok := b.workerConfigurations.Get(workerConfigurationArn)
	if !ok {
		return nil, ErrWorkerConfigNotFound
	}

	return w.clone(), nil
}

// ListWorkerConfigurations returns worker configurations matching namePrefix,
// paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListWorkerConfigurations(
	namePrefix, nextToken string,
	maxResults int,
) ([]*WorkerConfiguration, string, error) {
	b.mu.RLock("ListWorkerConfigurations")
	defer b.mu.RUnlock()

	all := b.workerConfigurations.All()

	matched := make([]*WorkerConfiguration, 0, len(all))

	for _, w := range all {
		if namePrefix != "" && !strings.HasPrefix(w.Name, namePrefix) {
			continue
		}

		matched = append(matched, w.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	pg := page.New(matched, nextToken, maxResults, defaultListLimit)

	return pg.Data, pg.Next, nil
}

// DeleteWorkerConfiguration deletes a worker configuration, returning a
// snapshot with State set to DELETING to mirror AWS's synchronous delete response.
func (b *InMemoryBackend) DeleteWorkerConfiguration(workerConfigurationArn string) (*WorkerConfiguration, error) {
	b.mu.Lock("DeleteWorkerConfiguration")
	defer b.mu.Unlock()

	w, ok := b.workerConfigurations.Get(workerConfigurationArn)
	if !ok {
		return nil, ErrWorkerConfigNotFound
	}

	out := w.clone()
	out.State = deletingState

	b.workerConfigurations.Delete(workerConfigurationArn)

	return out, nil
}
