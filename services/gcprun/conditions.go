package gcprun

import (
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const condReady = "Ready"

func newCondition(
	typ string, state runpb.Condition_State, msg string, at *timestamppb.Timestamp,
) *runpb.Condition {
	return &runpb.Condition{Type: typ, State: state, Message: msg, LastTransitionTime: at}
}

func setCondition(conds []*runpb.Condition, c *runpb.Condition) []*runpb.Condition {
	for i, old := range conds {
		if old.GetType() == c.GetType() {
			conds[i] = c

			return conds
		}
	}

	return append(conds, c)
}
