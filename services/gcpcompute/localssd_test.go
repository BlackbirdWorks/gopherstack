package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
)

func TestLocalSSDByMachineFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		machine string
		scratch int
		status  int
	}{
		{name: "n1 three", machine: "n1-standard-4", scratch: 3},
		{name: "n2 two", machine: "n2-standard-8", scratch: 2},
		{name: "n2d two", machine: "n2d-standard-8", scratch: 2},
		{name: "n2d large", machine: "n2d-standard-96", scratch: 24},
		{name: "n2d three", machine: "n2d-standard-8", scratch: 3, status: http.StatusBadRequest},
		{name: "e2 none allowed", machine: e2Medium, scratch: 1, status: http.StatusBadRequest},
		{name: "t2d none allowed", machine: "t2d-standard-4", scratch: 1, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := vmSpec("ssd")
			spec.MachineType = s(tt.machine)

			for range tt.scratch {
				spec.Disks = append(spec.Disks, &computepb.AttachedDisk{Type: s("SCRATCH")})
			}

			op, err := e.instances(t).Insert(t.Context(),
				&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: spec})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			e.done(t, op, err)
		})
	}
}
