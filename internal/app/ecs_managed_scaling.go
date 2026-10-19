package app

import (
	"math"
	"strings"

	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	ecsCPUUnitsPerVCPU = 1024
	asgInServiceState  = "InService"
)

// ecsASGController adapts Auto Scaling (and EC2 for instance sizes) to the ECS managed-scaling interfaces.
type ecsASGController struct {
	asg *autoscalingbackend.Handler
	ec2 *ec2backend.Handler
}

func splitASGRef(ref string) (string, string) {
	if !strings.HasPrefix(ref, "arn:") {
		return ref, ""
	}

	parts := strings.SplitN(ref, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return "", ""
	}

	_, n, _ := strings.Cut(parts[5], asgARNNameMarker)

	return n, parts[3]
}

func (c ecsASGController) AutoScalingGroupExists(ref string) bool {
	_, ok := c.DescribeGroup(ref)

	return ok
}

func (c ecsASGController) DescribeGroup(ref string) (ecsbackend.ASGInfo, bool) {
	name, region := splitASGRef(ref)
	if name == "" {
		return ecsbackend.ASGInfo{}, false
	}

	groups, err := c.asg.BackendFor(region).DescribeAutoScalingGroups([]string{name}, nil)
	if err != nil || len(groups) == 0 {
		return ecsbackend.ASGInfo{}, false
	}

	g := groups[0]
	info := ecsbackend.ASGInfo{
		Name: g.AutoScalingGroupName,
		Min:  int(g.MinSize), Max: int(g.MaxSize), Desired: int(g.DesiredCapacity),
	}

	for _, inst := range g.Instances {
		if inst.LifecycleState != asgInServiceState {
			continue
		}

		info.InServiceInstanceIDs = append(info.InServiceInstanceIDs, inst.InstanceID)

		if info.InstanceType == "" {
			info.InstanceType = inst.InstanceType
		}
	}

	if info.InstanceType == "" {
		info.InstanceType = c.groupInstanceType(region, &g)
	}

	return info, true
}

func (c ecsASGController) SetDesiredCapacity(ref string, desired int) error {
	name, region := splitASGRef(ref)

	return c.asg.BackendFor(region).
		SetDesiredCapacity(name, int32(min(max(desired, math.MinInt32), math.MaxInt32)), false)
}

func (c ecsASGController) SetInstanceProtection(ref string, ids []string, protected bool) error {
	name, region := splitASGRef(ref)

	return c.asg.BackendFor(region).SetInstanceProtection(name, ids, protected)
}

// ContainerInstanceResources sizes an EC2 instance from its type, found via Auto Scaling or EC2.
func (c ecsASGController) ContainerInstanceResources(instanceID string) (int, int, bool) {
	instanceType := ""

	for _, bk := range c.asg.RegionBackends() {
		details, err := bk.DescribeAutoScalingInstances([]string{instanceID})
		if err == nil && len(details) > 0 && details[0].InstanceType != "" {
			instanceType = details[0].InstanceType

			break
		}
	}

	if instanceType == "" && c.ec2 != nil {
		for _, bk := range c.ec2.RegionBackends() {
			if found := bk.DescribeInstances([]string{instanceID}, ""); len(found) > 0 {
				instanceType = found[0].InstanceType

				break
			}
		}
	}

	return c.InstanceTypeResources(instanceType)
}

// InstanceTypeResources sizes a catalogued EC2 instance type in ECS CPU units and MiB.
func (c ecsASGController) InstanceTypeResources(instanceType string) (int, int, bool) {
	vcpus, mem, ok := ec2backend.InstanceTypeResources(instanceType)
	if !ok {
		return 0, 0, false
	}

	return vcpus * ecsCPUUnitsPerVCPU, mem, true
}

// groupInstanceType resolves the type a group would launch from its launch configuration or template.
func (c ecsASGController) groupInstanceType(region string, g *autoscalingbackend.AutoScalingGroup) string {
	if g.LaunchConfigurationName != "" {
		lcs, err := c.asg.BackendFor(region).DescribeLaunchConfigurations([]string{g.LaunchConfigurationName})
		if err == nil && len(lcs) > 0 {
			return lcs[0].InstanceType
		}
	}

	lt := g.LaunchTemplate
	if lt == nil || c.ec2 == nil {
		return ""
	}

	idOrName := lt.LaunchTemplateID
	if idOrName == "" {
		idOrName = lt.LaunchTemplateName
	}

	if bk, ok := c.ec2.BackendFor(region).(*ec2backend.InMemoryBackend); ok {
		if tpl, err := bk.GetLaunchTemplate(idOrName, lt.Version); err == nil {
			return tpl.InstanceType
		}
	}

	return ""
}
