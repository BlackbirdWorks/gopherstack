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

	// Tasks

	RunTask(input RunTaskInput) ([]Task, error)
	DescribeTasks(cluster string, taskArns []string) ([]Task, error)
	StopTask(cluster, taskArn, reason string) (*Task, error)
	ListTasks(cluster string) ([]string, error)
}
