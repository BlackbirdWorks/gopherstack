package polly

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// StartSpeechSynthesisTask creates scheduled asynchronous task.
func (b *InMemoryBackend) StartSpeechSynthesisTask(
	options SynthesisOptions,
	outputBucket, roleArn, topicArn string,
) (*SpeechSynthesisTask, error) {
	if outputBucket == "" {
		return nil, fmt.Errorf("%w: OutputS3BucketName is required", ErrValidation)
	}
	if len(options.Text) > maxTaskTextLen {
		return nil, fmt.Errorf(
			"%w: text exceeds maximum length of %d characters", ErrTextLengthExceeded, maxTaskTextLen,
		)
	}

	normal, err := b.validateOptions(options)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	task := &SpeechSynthesisTask{
		CreationTime:       time.Now().UTC(),
		TaskID:             id,
		TaskStatus:         taskStatusScheduled,
		OutputURI:          fmt.Sprintf("s3://%s/%s.%s", outputBucket, id, taskExtension(normal.OutputFormat)),
		OutputS3BucketName: outputBucket,
		SNSRoleArn:         roleArn,
		SNSTopicArn:        topicArn,
		Options:            normal,
	}

	b.mu.Lock()
	b.tasks.Put(task)
	b.tags[b.taskARN(id)] = make(map[string]string)
	b.mu.Unlock()

	return cloneTask(task), nil
}

// GetSpeechSynthesisTask retrieves task and advances simulated lifecycle.
func (b *InMemoryBackend) GetSpeechSynthesisTask(taskID string) (*SpeechSynthesisTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.tasks.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("%w: task %q", ErrTaskNotFound, taskID)
	}

	advanceTask(task)

	return cloneTask(task), nil
}

// ListSpeechSynthesisTasks lists tasks and advances lifecycle consistently with AWS polling.
func (b *InMemoryBackend) ListSpeechSynthesisTasks(
	status, token string,
	maxResults int,
) ([]*SpeechSynthesisTask, string, error) {
	if status != "" && !slices.Contains(validTaskStatuses(), status) {
		return nil, "", fmt.Errorf("%w: invalid Status %q", ErrValidation, status)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Table.Snapshot returns tasks ordered by TaskID ascending, matching the
	// previous collections.SortedKeys(b.tasks) traversal order exactly.
	tasks := b.tasks.Snapshot()

	offset, err := parseToken(token, len(tasks))
	if err != nil {
		return nil, "", err
	}
	if maxResults <= 0 || maxResults > maxTaskPageSize {
		maxResults = maxTaskPageSize
	}

	out := make([]*SpeechSynthesisTask, 0, len(tasks))
	for _, task := range tasks[offset:] {
		advanceTask(task)
		if status == "" || task.TaskStatus == status {
			out = append(out, cloneTask(task))
		}
		if len(out) == maxResults {
			return out, encodeToken(offset + len(out)), nil
		}
	}

	return out, "", nil
}

func cloneTask(task *SpeechSynthesisTask) *SpeechSynthesisTask {
	copyTask := *task
	copyTask.Options.LexiconNames = slices.Clone(task.Options.LexiconNames)
	copyTask.Options.SpeechMarkTypes = slices.Clone(task.Options.SpeechMarkTypes)

	return &copyTask
}

func advanceTask(task *SpeechSynthesisTask) {
	switch task.TaskStatus {
	case taskStatusScheduled:
		task.TaskStatus = taskStatusProgress
	case taskStatusProgress:
		if strings.Contains(strings.ToLower(task.Options.Text), failedTaskMarker) {
			task.TaskStatus = taskStatusFailed
			task.TaskStatusReason = "Synthetic synthesis failure requested by text marker"
		} else {
			task.TaskStatus = taskStatusCompleted
		}
	}
	task.polls++
}

func taskExtension(format string) string {
	if format == outputFormatOGG || format == outputFormatOggOpus {
		return "ogg"
	}

	return format
}

// encodeToken returns an opaque base64 cursor for pagination.
func encodeToken(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(n)))
}

func parseToken(token string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw := token
	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		raw = string(decoded)
	}
	var offset int
	if _, err := fmt.Sscanf(raw, "%d", &offset); err != nil || offset < 0 || offset > total {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrInvalidNextToken)
	}

	return offset, nil
}

func validTaskStatuses() []string {
	return []string{taskStatusScheduled, taskStatusProgress, taskStatusCompleted, taskStatusFailed}
}
