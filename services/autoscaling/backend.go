package autoscaling

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// completedProgress is the progress value for a successfully completed scaling activity.
const completedProgress = int32(100)

var (
	// ErrGroupNotFound is returned when the requested Auto Scaling group does not exist.
	ErrGroupNotFound = errors.New("AutoScalingGroupNotFound")
	// ErrGroupAlreadyExists is returned when an Auto Scaling group with that name already exists.
	ErrGroupAlreadyExists = errors.New("AlreadyExists")
	// ErrLaunchConfigurationNotFound is returned when a launch configuration does not exist.
	ErrLaunchConfigurationNotFound = errors.New("LaunchConfigurationNotFound")
	// ErrLaunchConfigurationAlreadyExists is returned when a launch configuration already exists.
	ErrLaunchConfigurationAlreadyExists = errors.New("AlreadyExists")
	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = errors.New("InvalidAction")
	// ErrInvalidParameter is returned when a request parameter is invalid.
	ErrInvalidParameter = errors.New("ValidationError")
)

// StorageBackend is the interface for the Autoscaling in-memory store.
type StorageBackend interface {
	CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DescribeAutoScalingGroups(names []string) ([]AutoScalingGroup, error)
	UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DeleteAutoScalingGroup(name string) error

	CreateLaunchConfiguration(input CreateLaunchConfigurationInput) (*LaunchConfiguration, error)
	DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error)
	DeleteLaunchConfiguration(name string) error

	DescribeScalingActivities(groupName string) ([]ScalingActivity, error)
}

// CreateAutoScalingGroupInput holds the input for CreateAutoScalingGroup.
type CreateAutoScalingGroupInput struct {
	AutoScalingGroupName    string
	LaunchConfigurationName string
	HealthCheckType         string
	AvailabilityZones       []string
	LoadBalancerNames       []string
	TargetGroupARNs         []string
	Tags                    []Tag
	MinSize                 int32
	MaxSize                 int32
	DesiredCapacity         int32
	DefaultCooldown         int32
	HealthCheckGracePeriod  int32
}

// UpdateAutoScalingGroupInput holds the input for UpdateAutoScalingGroup.
type UpdateAutoScalingGroupInput struct {
	MinSize                 *int32
	MaxSize                 *int32
	DesiredCapacity         *int32
	DefaultCooldown         *int32
	HealthCheckGracePeriod  *int32
	AutoScalingGroupName    string
	LaunchConfigurationName string
	HealthCheckType         string
	AvailabilityZones       []string
}

// CreateLaunchConfigurationInput holds the input for CreateLaunchConfiguration.
type CreateLaunchConfigurationInput struct {
	LaunchConfigurationName string
	ImageID                 string
	InstanceType            string
	KeyName                 string
	IAMInstanceProfile      string
	UserData                string
	KernelID                string
	RamdiskID               string
	SecurityGroups          []string
	BlockDeviceMappings     []BlockDeviceMapping
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	groups               map[string]*AutoScalingGroup
	launchConfigurations map[string]*LaunchConfiguration
	activities           map[string][]ScalingActivity
	mu                   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		groups:               make(map[string]*AutoScalingGroup),
		launchConfigurations: make(map[string]*LaunchConfiguration),
		activities:           make(map[string][]ScalingActivity),
		mu:                   lockmetrics.New("autoscaling"),
	}
}

// CreateAutoScalingGroup creates a new Auto Scaling group.
func (b *InMemoryBackend) CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("CreateAutoScalingGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[input.AutoScalingGroupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, input.AutoScalingGroupName)
	}

	if input.AutoScalingGroupName == "" {
		return nil, fmt.Errorf("%w: AutoScalingGroupName is required", ErrInvalidParameter)
	}

	desired := input.DesiredCapacity
	if desired == 0 {
		desired = input.MinSize
	}

	healthCheckType := input.HealthCheckType
	if healthCheckType == "" {
		healthCheckType = "EC2"
	}

	group := &AutoScalingGroup{
		AutoScalingGroupName: input.AutoScalingGroupName,
		AutoScalingGroupARN: "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:" +
			uuid.NewString() + ":autoScalingGroupName/" + input.AutoScalingGroupName,
		LaunchConfigurationName: input.LaunchConfigurationName,
		MinSize:                 input.MinSize,
		MaxSize:                 input.MaxSize,
		DesiredCapacity:         desired,
		DefaultCooldown:         input.DefaultCooldown,
		HealthCheckType:         healthCheckType,
		HealthCheckGracePeriod:  input.HealthCheckGracePeriod,
		AvailabilityZones:       input.AvailabilityZones,
		LoadBalancerNames:       input.LoadBalancerNames,
		TargetGroupARNs:         input.TargetGroupARNs,
		Tags:                    input.Tags,
		Instances:               []Instance{},
		CreatedTime:             time.Now(),
		Status:                  "Active",
	}

	b.groups[input.AutoScalingGroupName] = group

	b.activities[input.AutoScalingGroupName] = append(
		b.activities[input.AutoScalingGroupName],
		ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: input.AutoScalingGroupName,
			Description:          "Launching a new EC2 instance",
			StatusCode:           "Successful",
			StatusMessage:        "",
			Progress:             completedProgress,
			StartTime:            time.Now(),
			EndTime:              time.Now(),
		},
	)

	cp := *group

	return &cp, nil
}

// DescribeAutoScalingGroups returns Auto Scaling groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeAutoScalingGroups(names []string) ([]AutoScalingGroup, error) {
	b.mu.RLock("DescribeAutoScalingGroups")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		result := make([]AutoScalingGroup, 0, len(names))

		for _, name := range names {
			g, ok := b.groups[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, name)
			}

			cp := *g
			result = append(result, cp)
		}

		return result, nil
	}

	result := make([]AutoScalingGroup, 0, len(b.groups))
	for _, g := range b.groups {
		result = append(result, *g)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AutoScalingGroupName < result[j].AutoScalingGroupName
	})

	return result, nil
}

// UpdateAutoScalingGroup updates an existing Auto Scaling group.
func (b *InMemoryBackend) UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("UpdateAutoScalingGroup")
	defer b.mu.Unlock()

	g, ok := b.groups[input.AutoScalingGroupName]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	if input.MinSize != nil {
		g.MinSize = *input.MinSize
	}

	if input.MaxSize != nil {
		g.MaxSize = *input.MaxSize
	}

	if input.DesiredCapacity != nil {
		g.DesiredCapacity = *input.DesiredCapacity
	}

	if input.DefaultCooldown != nil {
		g.DefaultCooldown = *input.DefaultCooldown
	}

	if input.HealthCheckGracePeriod != nil {
		g.HealthCheckGracePeriod = *input.HealthCheckGracePeriod
	}

	if input.LaunchConfigurationName != "" {
		g.LaunchConfigurationName = input.LaunchConfigurationName
	}

	if input.HealthCheckType != "" {
		g.HealthCheckType = input.HealthCheckType
	}

	if len(input.AvailabilityZones) > 0 {
		g.AvailabilityZones = input.AvailabilityZones
	}

	cp := *g

	return &cp, nil
}

// DeleteAutoScalingGroup removes an Auto Scaling group by name.
func (b *InMemoryBackend) DeleteAutoScalingGroup(name string) error {
	b.mu.Lock("DeleteAutoScalingGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, name)
	}

	delete(b.groups, name)
	delete(b.activities, name)

	return nil
}

// CreateLaunchConfiguration creates a new launch configuration.
func (b *InMemoryBackend) CreateLaunchConfiguration(
	input CreateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	b.mu.Lock("CreateLaunchConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.launchConfigurations[input.LaunchConfigurationName]; exists {
		return nil, fmt.Errorf(
			"%w: launch configuration %q already exists",
			ErrLaunchConfigurationAlreadyExists,
			input.LaunchConfigurationName,
		)
	}

	if input.LaunchConfigurationName == "" {
		return nil, fmt.Errorf("%w: LaunchConfigurationName is required", ErrInvalidParameter)
	}

	lc := &LaunchConfiguration{
		LaunchConfigurationName: input.LaunchConfigurationName,
		LaunchConfigurationARN: "arn:aws:autoscaling:us-east-1:000000000000:launchConfiguration:" +
			uuid.NewString() + ":launchConfigurationName/" + input.LaunchConfigurationName,
		ImageID:             input.ImageID,
		InstanceType:        input.InstanceType,
		KeyName:             input.KeyName,
		IAMInstanceProfile:  input.IAMInstanceProfile,
		UserData:            input.UserData,
		KernelID:            input.KernelID,
		RamdiskID:           input.RamdiskID,
		SecurityGroups:      input.SecurityGroups,
		BlockDeviceMappings: input.BlockDeviceMappings,
		CreatedTime:         time.Now(),
	}

	b.launchConfigurations[input.LaunchConfigurationName] = lc

	cp := *lc

	return &cp, nil
}

// DescribeLaunchConfigurations returns launch configurations, optionally filtered by name.
func (b *InMemoryBackend) DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error) {
	b.mu.RLock("DescribeLaunchConfigurations")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		result := make([]LaunchConfiguration, 0, len(names))

		for _, name := range names {
			lc, ok := b.launchConfigurations[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
			}

			cp := *lc
			result = append(result, cp)
		}

		return result, nil
	}

	result := make([]LaunchConfiguration, 0, len(b.launchConfigurations))
	for _, lc := range b.launchConfigurations {
		result = append(result, *lc)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LaunchConfigurationName < result[j].LaunchConfigurationName
	})

	return result, nil
}

// DeleteLaunchConfiguration removes a launch configuration by name.
func (b *InMemoryBackend) DeleteLaunchConfiguration(name string) error {
	b.mu.Lock("DeleteLaunchConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.launchConfigurations[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
	}

	delete(b.launchConfigurations, name)

	return nil
}

// DescribeScalingActivities returns scaling activities for the given group.
func (b *InMemoryBackend) DescribeScalingActivities(groupName string) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	if groupName != "" {
		if _, ok := b.groups[groupName]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		acts := b.activities[groupName]
		result := make([]ScalingActivity, len(acts))
		copy(result, acts)

		return result, nil
	}

	result := make([]ScalingActivity, 0)
	for _, acts := range b.activities {
		result = append(result, acts...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}
