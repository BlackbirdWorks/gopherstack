package autoscaling_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_DetachInstancesNoInstances covers DetachInstances.
func TestAutoscalingHandler_DetachInstancesNoInstances(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create LC and ASG.
	postAutoscalingForm(t, h, "Action=CreateLaunchConfiguration&Version=2011-01-01"+
		"&LaunchConfigurationName=di-lc&ImageId=ami-12345&InstanceType=t3.micro")
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=di-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=di-lc")
	require.Equal(t, http.StatusOK, rec.Code)

	// DetachInstances (no instances, should still succeed).
	rec = postAutoscalingForm(t, h, "Action=DetachInstances&Version=2011-01-01"+
		"&AutoScalingGroupName=di-asg"+
		"&ShouldDecrementDesiredCapacity=true")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// xmlLaunchInstancesResponse parses the LaunchInstances response, which returns
// InstanceCollection entries (grouped by AZ/InstanceType with a list of instance
// IDs) rather than a flat per-instance list.
type xmlLaunchInstancesResponse struct {
	XMLName xml.Name `xml:"LaunchInstancesResponse"`
	Result  struct {
		AutoScalingGroupName string `xml:"AutoScalingGroupName"`
		ClientToken          string `xml:"ClientToken"`
		Instances            struct {
			Members []struct {
				AvailabilityZone string `xml:"AvailabilityZone"`
				InstanceIDs      struct {
					Members []string `xml:"member"`
				} `xml:"InstanceIds"`
			} `xml:"member"`
		} `xml:"Instances"`
	} `xml:"LaunchInstancesResult"`
}

// TestAutoscalingHandler_LaunchInstancesWireShapeAndIndex verifies that LaunchInstances (a) reads the
// real RequestedCapacity field (not the DesiredCapacity typo it used to read), (b)
// returns the AWS InstanceCollection shape instead of a flat per-instance list, and
// (c) that launched instances are indexed so a subsequent
// TerminateInstanceInAutoScalingGroup can find them by ID (previously LaunchInstances
// never updated the instance index at all).
func TestAutoscalingHandler_LaunchInstancesWireShapeAndIndex(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "launch-instances-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"10"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "LaunchInstances", url.Values{
		"AutoScalingGroupName": {asgName},
		"ClientToken":          {"tok-1"},
		"RequestedCapacity":    {"2"},
	})
	require.Equal(t, 200, code, body)

	var parsed xmlLaunchInstancesResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	assert.Equal(t, asgName, parsed.Result.AutoScalingGroupName)
	assert.Equal(t, "tok-1", parsed.Result.ClientToken)

	var instanceIDs []string
	for _, m := range parsed.Result.Instances.Members {
		instanceIDs = append(instanceIDs, m.InstanceIDs.Members...)
	}

	require.Len(t, instanceIDs, 2, "RequestedCapacity=2 must launch exactly 2 instances; got body: %s", body)

	code, body = doAS(t, h, "TerminateInstanceInAutoScalingGroup", url.Values{
		"InstanceId":                     {instanceIDs[0]},
		"ShouldDecrementDesiredCapacity": {"true"},
	})
	assert.Equal(t, 200, code,
		"an instance launched via LaunchInstances must be indexed and terminable by ID; got: %s", body)
}

func TestAutoscalingHandler_AttachInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_instances_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=attach-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachInstances&Version=2011-01-01" +
				"&AutoScalingGroupName=attach-asg" +
				"&InstanceIds.member.1=i-abc123&InstanceIds.member.2=i-def456",
			wantStatus: http.StatusOK,
		},
		{
			name:       "attach_instances_group_not_found",
			body:       "Action=AttachInstances&Version=2011-01-01&AutoScalingGroupName=no-such&InstanceIds.member.1=i-abc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_TerminateInstanceInAutoScalingGroup(t *testing.T) {
	t.Parallel()

	const terminateAction = "Action=TerminateInstanceInAutoScalingGroup&Version=2011-01-01"

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "terminate_instance_success",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=term-asg&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			// i-fake not in any group → 400.
			body:       terminateAction + "&InstanceId=i-fake&ShouldDecrementDesiredCapacity=false",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminate_instance_not_found",
			body:       terminateAction + "&InstanceId=i-unknown&ShouldDecrementDesiredCapacity=true",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DescribeAutoScalingInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "describe_instances_empty",
			body:       "Action=DescribeAutoScalingInstances&Version=2011-01-01",
			wantStatus: http.StatusOK,
		},
		{
			name: "describe_instances_with_group",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inst-asg&MinSize=1&MaxSize=3&DesiredCapacity=1",
				)
			},
			body:       "Action=DescribeAutoScalingInstances&Version=2011-01-01",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "InService")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}
