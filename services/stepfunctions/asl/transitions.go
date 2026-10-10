package asl

import (
	"fmt"
	"sort"
)

func validateTransitions(states map[string]*State) error {
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		st := states[name]
		if st == nil {
			continue
		}

		if err := validateStateTransitions(name, st, states); err != nil {
			return err
		}

		for _, sub := range nestedStateMachines(st) {
			if err := validateTransitions(sub); err != nil {
				return err
			}
		}
	}

	return nil
}

const initialTargetCap = 8

func validateStateTransitions(name string, st *State, states map[string]*State) error {
	targetSeeds := [...]string{st.Next, st.Default}

	targets := make([]string, 0, initialTargetCap)
	targets = append(targets, targetSeeds[:]...)
	for _, c := range st.Catch {
		targets = append(targets, c.Next)
	}

	for _, r := range st.Choices {
		targets = append(targets, r.Next)
	}

	for _, t := range targets {
		if _, ok := states[t]; t != "" && !ok {
			return fmt.Errorf("%w: state %q transitions to missing state %q", ErrParseError, name, t)
		}
	}

	switch st.Type {
	case stateTypeTask, stateTypePass, stateTypeWait, stateTypeParallel, StateTypeMap:
		if st.Next == "" && !st.End {
			return fmt.Errorf("%w: state %q must have either Next or End", ErrParseError, name)
		}
	}

	return nil
}
