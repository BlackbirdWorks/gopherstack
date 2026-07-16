package sqs

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an SQS backend.
type StorageBackend interface {
	CreateQueue(input *CreateQueueInput) (*CreateQueueOutput, error)
	DeleteQueue(input *DeleteQueueInput) error
	ListQueues(input *ListQueuesInput) (*ListQueuesOutput, error)
	GetQueueURL(input *GetQueueURLInput) (*GetQueueURLOutput, error)
	GetQueueAttributes(input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error)
	SetQueueAttributes(input *SetQueueAttributesInput) error
	SendMessage(input *SendMessageInput) (*SendMessageOutput, error)
	ReceiveMessage(input *ReceiveMessageInput) (*ReceiveMessageOutput, error)
	DeleteMessage(input *DeleteMessageInput) error
	ChangeMessageVisibility(input *ChangeMessageVisibilityInput) error
	SendMessageBatch(input *SendMessageBatchInput) (*SendMessageBatchOutput, error)
	DeleteMessageBatch(input *DeleteMessageBatchInput) (*DeleteMessageBatchOutput, error)
	PurgeQueue(input *PurgeQueueInput) error
	TagQueue(input *TagQueueInput) error
	UntagQueue(input *UntagQueueInput) error
	ListQueueTags(input *ListQueueTagsInput) (*ListQueueTagsOutput, error)
	ChangeMessageVisibilityBatch(
		input *ChangeMessageVisibilityBatchInput,
	) (*ChangeMessageVisibilityBatchOutput, error)
	ListDeadLetterSourceQueues(
		input *ListDeadLetterSourceQueuesInput,
	) (*ListDeadLetterSourceQueuesOutput, error)
	AddPermission(input *AddPermissionInput) error
	RemovePermission(input *RemovePermissionInput) error
	StartMessageMoveTask(input *StartMessageMoveTaskInput) (*StartMessageMoveTaskOutput, error)
	CancelMessageMoveTask(input *CancelMessageMoveTaskInput) (*CancelMessageMoveTaskOutput, error)
	ListMessageMoveTasks(input *ListMessageMoveTasksInput) (*ListMessageMoveTasksOutput, error)
	ListAll() []QueueInfo
}
