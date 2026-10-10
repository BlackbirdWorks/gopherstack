package autoscaling

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const launchInProgressPercent = int32(50)

func instanceIDSet(g *AutoScalingGroup) map[string]bool {
	ids := make(map[string]bool, len(g.Instances))
	for _, inst := range g.Instances {
		ids[inst.InstanceID] = true
	}

	return ids
}

// recordCapacityActivities records one scaling activity per instance launched or removed since
// before, with the cause text AWS gives for the trigger. Must be called with b.mu held.
func (b *InMemoryBackend) recordCapacityActivities(
	g *AutoScalingGroup, before map[string]bool, oldDesired int32, trigger string,
) {
	now := time.Now()
	stamp := now.UTC().Format(time.RFC3339)

	var launched []string

	for _, inst := range g.Instances {
		if before[inst.InstanceID] {
			continue
		}

		launched = append(launched, inst.InstanceID)

		b.appendCapacityActivity(g.AutoScalingGroupName, launchingActivityPrefix+": "+inst.InstanceID,
			fmt.Sprintf("At %s %s changing the desired capacity from %d to %d. At %s an instance was started in "+
				"response to a difference between desired and actual capacity, increasing the capacity from %d to %d.",
				stamp, trigger, oldDesired, g.DesiredCapacity, stamp, len(before), len(g.Instances)), now)
	}

	b.beginLaunchDwellLocked(g, launched)

	after := instanceIDSet(g)
	for id := range before {
		if after[id] {
			continue
		}

		b.appendCapacityActivity(g.AutoScalingGroupName, "Terminating EC2 instance: "+id,
			fmt.Sprintf("At %s %s changing the desired capacity from %d to %d. At %s an instance was taken out of "+
				"service in response to a difference between desired and actual capacity, shrinking the capacity "+
				"from %d to %d.",
				stamp, trigger, oldDesired, g.DesiredCapacity, stamp, len(before), len(g.Instances)), now)
	}
}

func (b *InMemoryBackend) appendCapacityActivity(group, description, cause string, now time.Time) {
	act := ScalingActivity{
		ActivityID:           uuid.NewString(),
		AutoScalingGroupName: group,
		Description:          description,
		Cause:                cause,
		StatusCode:           statusCodeSuccessful,
		Progress:             completedProgress,
		StartTime:            now,
		EndTime:              now,
	}

	if b.launchDelay.Load() > 0 && strings.HasPrefix(description, launchingActivityPrefix) {
		act.StatusCode, act.Progress, act.EndTime = statusInProgress, launchInProgressPercent, time.Time{}
	}

	b.activities[group] = append(b.activities[group], act)
}
