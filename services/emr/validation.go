package emr

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultActionOnFailure = "TERMINATE_CLUSTER"

var (
	instanceTypeRe = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9-]+$`)
)

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func validateInstanceType(t string) error {
	if t != "" && !instanceTypeRe.MatchString(t) {
		return fmt.Errorf("%w: invalid instance type %q", ErrValidation, t)
	}

	return nil
}

func validateInstanceTypes(in RunJobFlowInstances) error {
	for _, g := range in.InstanceGroups {
		if err := validateInstanceType(g.InstanceType); err != nil {
			return err
		}
	}

	return nil
}

func validateStepSpecs(specs []StepSpec) error {
	valid := []string{"TERMINATE_JOB_FLOW", defaultActionOnFailure, "CANCEL_AND_WAIT", "CONTINUE"}

	for _, s := range specs {
		if s.ActionOnFailure != "" && !slices.Contains(valid, s.ActionOnFailure) {
			return fmt.Errorf("%w: invalid ActionOnFailure %q", ErrValidation, s.ActionOnFailure)
		}
	}

	return nil
}

// checkMarker rejects a non-empty Marker or NextToken this backend did not issue.
func checkMarker(body []byte) error {
	var in struct {
		Marker    string `json:"Marker"`
		NextToken string `json:"NextToken"`
	}

	if json.Unmarshal(body, &in) == nil {
		for _, tok := range []string{in.Marker, in.NextToken} {
			if page.ValidateToken(tok) != nil {
				return fmt.Errorf("%w: Invalid marker", ErrValidation)
			}
		}
	}

	return nil
}

func notValidErr(kind, id string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, kind+" id '"+id+"' is not valid.")
}
