package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedDataRepositoryTask struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	TaskID       string            `json:"taskId"`
	FileSystemID string            `json:"fileSystemId"`
	Type         string            `json:"type"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
	Paths        []string          `json:"paths,omitempty"`
}

func (t *storedDataRepositoryTask) toPublic() *DataRepositoryTask {
	return &DataRepositoryTask{
		CreationTime: epochTime(t.CreationTime),
		TaskID:       t.TaskID,
		FileSystemID: t.FileSystemID,
		Type:         t.Type,
		Lifecycle:    t.Lifecycle,
		ResourceARN:  t.ResourceARN,
		Paths:        t.Paths,
		Tags:         tagsMapToSlice(t.Tags),
	}
}

type createDataRepositoryTaskInput struct {
	FileSystemID string   `json:"FileSystemId"`
	Type         string   `json:"Type"`
	Paths        []string `json:"Paths,omitempty"`
	Tags         []Tag    `json:"Tags,omitempty"`
}

// CreateDataRepositoryTask creates a data repository task.
func (b *InMemoryBackend) CreateDataRepositoryTask(input *createDataRepositoryTaskInput) (*DataRepositoryTask, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataRepositoryTask")
	defer b.mu.Unlock()

	if !b.fileSystems.Has(input.FileSystemID) {
		return nil, ErrFileSystemNotFound
	}

	id := "task-" + uuid.New().String()[:17]
	arn := b.drtARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	t := &storedDataRepositoryTask{
		CreationTime: now,
		Tags:         tags,
		Paths:        input.Paths,
		TaskID:       id,
		FileSystemID: input.FileSystemID,
		Type:         input.Type,
		Lifecycle:    "EXECUTING",
		ResourceARN:  arn,
	}

	b.dataRepositoryTasks.Put(t)
	b.tags[arn] = tags

	return t.toPublic(), nil
}

// CancelDataRepositoryTask marks a task as cancelled.
func (b *InMemoryBackend) CancelDataRepositoryTask(taskID string) error {
	b.mu.Lock("CancelDataRepositoryTask")
	defer b.mu.Unlock()

	t, ok := b.dataRepositoryTasks.Get(taskID)
	if !ok {
		return ErrDataRepositoryTaskNotFound
	}

	t.Lifecycle = "CANCELING"

	return nil
}

// DescribeDataRepositoryTasks returns tasks, optionally filtered by ID.
func (b *InMemoryBackend) DescribeDataRepositoryTasks( //nolint:dupl // existing issue.
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*DataRepositoryTask, string, error) {
	b.mu.RLock("DescribeDataRepositoryTasks")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedDataRepositoryTask

	if len(ids) > 0 {
		for _, id := range ids {
			t, ok := b.dataRepositoryTasks.Get(id)
			if !ok {
				return nil, "", ErrDataRepositoryTaskNotFound
			}

			all = append(all, t)
		}
	} else {
		all = b.dataRepositoryTasks.All()

		sort.Slice(all, func(i, j int) bool { return all[i].TaskID < all[j].TaskID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].TaskID
	})

	result := make([]*DataRepositoryTask, end-start)
	for i, t := range all[start:end] {
		result[i] = t.toPublic()
	}

	return result, next, nil
}

func (b *InMemoryBackend) drtARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("task/%s", id))
}
