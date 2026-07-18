package athena

import "encoding/json"

type cancelCapacityReservationInput struct {
	Name string `json:"Name"`
}

type createCapacityReservationInput struct {
	Name       string `json:"Name"`
	Tags       []Tag  `json:"Tags"`
	TargetDpus int32  `json:"TargetDpus"`
}

type deleteCapacityReservationInput struct {
	Name string `json:"Name"`
}

type capacityReservationNameInput struct {
	Name string `json:"Name"`
}

type updateCapacityReservationInput struct {
	Name       string `json:"Name"`
	TargetDpus int32  `json:"TargetDpus"`
}

type putCapacityAssignmentInput struct {
	CapacityReservationName string               `json:"CapacityReservationName"`
	CapacityAssignments     []CapacityAssignment `json:"CapacityAssignments"`
}

type getCapacityAssignmentInput struct {
	CapacityReservationName string `json:"CapacityReservationName"`
}

func (h *Handler) capacityReservationOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateCapacityReservation": func(b []byte) (any, error) {
			var input createCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateCapacityReservation(
				input.Name, input.TargetDpus, tagsFromSlice(input.Tags),
			)
		},
		"CancelCapacityReservation": func(b []byte) (any, error) {
			var input cancelCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CancelCapacityReservation(input.Name)
		},
		"DeleteCapacityReservation": func(b []byte) (any, error) {
			var input deleteCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteCapacityReservation(input.Name)
		},
	}
}

func (h *Handler) capacityExtraOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetCapacityReservation": func(b []byte) (any, error) {
			var input capacityReservationNameInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			cr, err := h.Backend.GetCapacityReservation(input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityReservation": cr}, nil
		},
		"ListCapacityReservations": func(_ []byte) (any, error) {
			list, err := h.Backend.ListCapacityReservations()
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityReservations": list}, nil
		},
		"UpdateCapacityReservation": func(b []byte) (any, error) {
			var input updateCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateCapacityReservation(input.Name, input.TargetDpus)
		},
		"PutCapacityAssignmentConfiguration": func(b []byte) (any, error) {
			var input putCapacityAssignmentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.PutCapacityAssignmentConfiguration(
				input.CapacityReservationName, input.CapacityAssignments,
			)
		},
		"GetCapacityAssignmentConfiguration": func(b []byte) (any, error) {
			var input getCapacityAssignmentInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			cfg, err := h.Backend.GetCapacityAssignmentConfiguration(input.CapacityReservationName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CapacityAssignmentConfiguration": cfg}, nil
		},
	}
}
