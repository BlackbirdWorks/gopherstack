package ecs

// Backend defines the interface for ECS control-plane operations.
// InMemoryBackend implements this interface; alternative backends (e.g. test
// doubles or future persistence-aware backends) can implement it too.
type Backend interface {
	// Clusters

	CreateCluster(input CreateClusterInput) (*Cluster, error)
	DescribeClusters(clusterNames []string) ([]Cluster, error)
	DeleteCluster(clusterName string) (*Cluster, error)

	// Task definitions

	RegisterTaskDefinition(input RegisterTaskDefinitionInput) (*TaskDefinition, error)
	DescribeTaskDefinition(family string) (*TaskDefinition, error)
	DeregisterTaskDefinition(taskDefinitionArn string) (*TaskDefinition, error)
	ListTaskDefinitions(familyPrefix string) ([]string, error)

	// Services

	CreateService(input CreateServiceInput) (*Service, error)
	DescribeServices(cluster string, serviceNames []string) ([]Service, error)
	UpdateService(input UpdateServiceInput) (*Service, error)
	DeleteService(cluster, serviceName string) (*Service, error)
	ListServices(cluster string) ([]string, error)

	// Tasks

	RunTask(input RunTaskInput) ([]Task, error)
	DescribeTasks(cluster string, taskArns []string) ([]Task, error)
	StopTask(cluster, taskArn, reason string) (*Task, error)
	ListTasks(cluster string) ([]string, error)

	// Container instances

	RegisterContainerInstance(cluster, ec2InstanceID string) (*ContainerInstance, error)
	DeregisterContainerInstance(cluster, containerInstance string, force bool) (*ContainerInstance, error)
	DescribeContainerInstances(cluster string, containerInstances []string) ([]ContainerInstance, error)
	ListContainerInstances(cluster string) ([]string, error)
	UpdateContainerInstancesState(
		cluster string,
		containerInstances []string,
		status string,
	) ([]ContainerInstance, error)

	// Task sets

	CreateTaskSet(input CreateTaskSetInput) (*TaskSet, error)
	DeleteTaskSet(cluster, service, taskSet string) (*TaskSet, error)
	DescribeTaskSets(cluster, service string, taskSets []string) ([]TaskSet, error)
	UpdateTaskSet(cluster, service, taskSet string, scale TaskSetScale) (*TaskSet, error)
	UpdateServicePrimaryTaskSet(cluster, service, primaryTaskSet string) (*TaskSet, error)

	// ECS Exec

	ExecuteCommand(cluster, task, container, command string, interactive bool) (*ExecuteCommandOutput, error)
}
