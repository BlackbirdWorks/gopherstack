package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	asgARNNameMarker = "autoScalingGroupName/"
	arnPartCount     = 6 // arn:partition:service:region:account:resource
)

// wireECSAutoScaling lets ECS validate capacity-provider Auto Scaling groups against Auto Scaling.
func wireECSAutoScaling(byName map[string]service.Registerable) {
	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	if !ok {
		return
	}

	asgH, ok := byName["Autoscaling"].(*autoscalingbackend.Handler)
	if !ok {
		return
	}

	ecsBk, ok := ecsH.Backend.(*ecsbackend.InMemoryBackend)
	if !ok {
		return
	}

	ec2H, _ := byName["EC2"].(*ec2backend.Handler)
	ctl := ecsASGController{asg: asgH, ec2: ec2H}

	ecsBk.SetAutoScalingGroupController(ctl)
	ecsBk.SetInstanceResourceResolver(ctl)
}
