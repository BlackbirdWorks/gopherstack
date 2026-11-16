package ecs

import (
	"fmt"
	"slices"
)

// RunningTaskIPv4s returns the awsvpc private IPv4 addresses of the cluster's RUNNING tasks.
// A non-empty attrFilters keeps only tasks on container instances whose attributes match every pair.
func (b *InMemoryBackend) RunningTaskIPv4s(cluster string, attrFilters map[string]string) ([]string, error) {
	arns, err := b.ListTasksFiltered(ListTasksInput{Cluster: cluster})
	if err != nil {
		return nil, err
	}

	if len(arns) == 0 {
		return nil, nil
	}

	tasks, _, err := b.DescribeTasks(cluster, arns)
	if err != nil {
		return nil, fmt.Errorf("describe tasks: %w", err)
	}

	var matching map[string]bool

	if len(attrFilters) > 0 {
		if matching, err = b.instancesMatching(cluster, attrFilters); err != nil {
			return nil, err
		}
	}

	ips := make([]string, 0, len(tasks))

	for i := range tasks {
		if tasks[i].LastStatus != statusRunning {
			continue
		}

		if matching != nil && !matching[tasks[i].ContainerInstanceArn] {
			continue
		}

		if ip, ok := privateIPFromAttachments(tasks[i].Attachments); ok {
			ips = append(ips, ip)
		}
	}

	slices.Sort(ips)

	return slices.Compact(ips), nil
}

func (b *InMemoryBackend) instancesMatching(cluster string, filters map[string]string) (map[string]bool, error) {
	instances, _, err := b.DescribeContainerInstances(cluster, nil)
	if err != nil {
		return nil, err
	}

	out := make(map[string]bool, len(instances))

	for i := range instances {
		have := make(map[string]string, len(instances[i].Attributes))
		for _, a := range instances[i].Attributes {
			have[a.Name] = a.Value
		}

		ok := true

		for k, v := range filters {
			if got, present := have[k]; !present || got != v {
				ok = false

				break
			}
		}

		if ok {
			out[instances[i].ContainerInstanceArn] = true
		}
	}

	return out, nil
}
