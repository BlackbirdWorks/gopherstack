package main

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	asgARNNameMarker = "autoScalingGroupName/"
	arnPartCount     = 6 // arn:partition:service:region:account:resource
)

// ecsASGResolver adapts Auto Scaling to ecs.AutoScalingGroupResolver.
type ecsASGResolver struct {
	handler *autoscalingbackend.Handler
}

func (r ecsASGResolver) AutoScalingGroupExists(arnOrName string) bool {
	name, region := arnOrName, ""

	if strings.HasPrefix(arnOrName, "arn:") {
		parts := strings.SplitN(arnOrName, ":", arnPartCount)
		if len(parts) < arnPartCount {
			return false
		}

		region = parts[3]

		_, n, found := strings.Cut(parts[5], asgARNNameMarker)
		if !found {
			return false
		}

		name = n
	}

	groups, err := r.handler.BackendFor(region).DescribeAutoScalingGroups([]string{name}, nil)

	return err == nil && len(groups) > 0
}

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

	ecsBk.SetAutoScalingGroupResolver(ecsASGResolver{handler: asgH})
}
