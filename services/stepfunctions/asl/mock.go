package asl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// Mock config errors.
var (
	ErrMockConfigInvalid    = errors.New("invalid mock configuration")
	ErrMockTestCaseNotFound = errors.New("mock test case not found")
)

var errMockBadKey = errors.New("invalid")

const errMockNoResponseForCall = "no mocked response for invocation"

// MockConfig is a parsed Step Functions Local / LocalStack mock configuration file.
type MockConfig struct {
	stateMachines map[string]map[string]map[string]string
	responses     map[string][]mockStep
}

type mockStep struct {
	throw *mockThrow
	ret   json.RawMessage
	lo    int
	hi    int
}

type mockThrow struct {
	Error string `json:"Error"`
	Cause string `json:"Cause"`
}

type mockFileStep struct {
	Throw  *mockThrow      `json:"Throw"`
	Return json.RawMessage `json:"Return"`
}

type mockFile struct {
	StateMachines map[string]struct {
		TestCases map[string]map[string]string `json:"TestCases"`
	} `json:"StateMachines"`
	MockedResponses map[string]map[string]mockFileStep `json:"MockedResponses"`
}

// LoadMockConfig reads and validates a mock configuration file.
func LoadMockConfig(path string) (*MockConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMockConfigInvalid, err)
	}

	return ParseMockConfig(data)
}

// ParseMockConfig validates mock configuration JSON.
func ParseMockConfig(data []byte) (*MockConfig, error) {
	var f mockFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMockConfigInvalid, err)
	}

	cfg := &MockConfig{
		stateMachines: map[string]map[string]map[string]string{},
		responses:     map[string][]mockStep{},
	}

	for name, resp := range f.MockedResponses {
		steps, err := parseMockSteps(name, resp)
		if err != nil {
			return nil, err
		}

		cfg.responses[name] = steps
	}

	for smName, sm := range f.StateMachines {
		for tcName, states := range sm.TestCases {
			for stateName, respName := range states {
				if _, ok := cfg.responses[respName]; !ok {
					return nil, fmt.Errorf(
						"%w: %s/%s/%s references undefined mocked response %q",
						ErrMockConfigInvalid, smName, tcName, stateName, respName,
					)
				}
			}
		}

		cfg.stateMachines[smName] = sm.TestCases
	}

	return cfg, nil
}

func parseMockSteps(name string, resp map[string]mockFileStep) ([]mockStep, error) {
	steps := make([]mockStep, 0, len(resp))

	for key, s := range resp {
		lo, hi, err := parseMockRange(key)
		if err != nil {
			return nil, fmt.Errorf("%w: mocked response %q: %w", ErrMockConfigInvalid, name, err)
		}

		if (s.Return == nil) == (s.Throw == nil) {
			return nil, fmt.Errorf(
				"%w: mocked response %q key %q needs exactly one of Return or Throw",
				ErrMockConfigInvalid, name, key,
			)
		}

		if s.Throw != nil && s.Throw.Error == "" {
			return nil, fmt.Errorf("%w: mocked response %q key %q: Throw needs Error", ErrMockConfigInvalid, name, key)
		}

		steps = append(steps, mockStep{lo: lo, hi: hi, ret: s.Return, throw: s.Throw})
	}

	sort.Slice(steps, func(i, j int) bool { return steps[i].lo < steps[j].lo })

	for i := 1; i < len(steps); i++ {
		if steps[i].lo <= steps[i-1].hi {
			return nil, fmt.Errorf(
				"%w: mocked response %q has overlapping invocation ranges", ErrMockConfigInvalid, name,
			)
		}
	}

	return steps, nil
}

func parseMockRange(key string) (int, int, error) {
	loStr, hiStr, isRange := strings.Cut(key, "-")

	lo, err := strconv.Atoi(loStr)
	if err != nil || lo < 0 {
		return 0, 0, fmt.Errorf("%w: invocation key %q", errMockBadKey, key)
	}

	if !isRange {
		return lo, lo, nil
	}

	hi, err := strconv.Atoi(hiStr)
	if err != nil || hi < lo {
		return 0, 0, fmt.Errorf("%w: invocation range %q", errMockBadKey, key)
	}

	return lo, hi, nil
}

// MockRun is the per-execution view of one test case, counting invocations per state.
type MockRun struct {
	states map[string]*mockState
}

type mockState struct {
	steps []mockStep
	calls atomic.Int64
}

// TestCase returns a MockRun for the named state machine and test case.
func (c *MockConfig) TestCase(smName, testCase string) (*MockRun, error) {
	states, ok := c.stateMachines[smName][testCase]
	if !ok {
		return nil, fmt.Errorf("%w: %q for state machine %q", ErrMockTestCaseNotFound, testCase, smName)
	}

	run := &MockRun{states: make(map[string]*mockState, len(states))}
	for stateName, respName := range states {
		run.states[stateName] = &mockState{steps: c.responses[respName]}
	}

	return run, nil
}

// invoke returns the mocked outcome for the next invocation of stateName.
func (r *MockRun) invoke(stateName string) (any, bool, error) {
	if r == nil {
		return nil, false, nil
	}

	st, ok := r.states[stateName]
	if !ok {
		return nil, false, nil
	}

	idx := int(st.calls.Add(1) - 1)

	for _, s := range st.steps {
		if idx < s.lo || idx > s.hi {
			continue
		}

		if s.throw != nil {
			return nil, true, &FailError{ErrCode: s.throw.Error, Cause: s.throw.Cause}
		}

		var out any
		if uerr := json.Unmarshal(s.ret, &out); uerr != nil {
			return nil, true, &FailError{ErrCode: errCodeStatesRuntime, Cause: uerr.Error()}
		}

		return out, true, nil
	}

	return nil, true, &FailError{
		ErrCode: errCodeStatesRuntime,
		Cause:   fmt.Sprintf("%s %d of state %q", errMockNoResponseForCall, idx, stateName),
	}
}

// SetMockRun makes Task states named in the run return mocked responses.
func (e *Executor) SetMockRun(r *MockRun) { e.mock = r }
