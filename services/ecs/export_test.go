package ecs

// ClusterFromTaskARNForTest exposes clusterFromTaskARN for unit tests.
func ClusterFromTaskARNForTest(taskARN string) string {
	return clusterFromTaskARN(taskARN)
}
