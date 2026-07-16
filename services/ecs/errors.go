package ecs

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var errServiceDeploymentAlreadyStopped = awserr.New(
	"ServiceDeploymentAlreadyStoppedException", awserr.ErrInvalidParameter,
)

// errNoLifecycleHook is returned by ContinueServiceDeployment: this backend
// never pauses a deployment at a lifecycle hook (blue/green PAUSE stages
// aren't modeled — every deployment either runs to completion or trips the
// circuit breaker), so there is never a paused hookId to act on. Matching AWS
// behavior for "act on a hook that isn't currently paused" as closely as
// possible without modeling the full lifecycle-hook state machine.
var errNoLifecycleHook = awserr.New("ClientException", awserr.ErrInvalidParameter)
