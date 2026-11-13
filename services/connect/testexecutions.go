package connect

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelTestExecution = "TestCaseExecution"
	execInitiated      = "INITIATED"
	execInProgress     = "IN_PROGRESS"
	execPassed         = "PASSED"
	execFailed         = "FAILED"
	execStopped        = "STOPPED"
	maxExecListPage    = 100
)

type testExecutionRec struct {
	Start      awstime.EpochTime  `json:"start"`
	End        *awstime.EpochTime `json:"end,omitempty"`
	Tags       map[string]string  `json:"tags,omitempty"`
	InstanceID string             `json:"instanceId"`
	ID         string             `json:"id"`
	TestCaseID string             `json:"testCaseId"`
	Status     string             `json:"status"`
	Records    []ExecutionRecord  `json:"records,omitempty"`
}

// TestCaseExecution is a test case execution (types.TestCaseExecution).
type TestCaseExecution struct {
	StartTime               awstime.EpochTime  `json:"StartTime"`
	EndTime                 *awstime.EpochTime `json:"EndTime,omitempty"`
	Tags                    map[string]string  `json:"Tags,omitempty"`
	TestCaseExecutionID     string             `json:"TestCaseExecutionId"`
	TestCaseExecutionStatus string             `json:"TestCaseExecutionStatus"`
	TestCaseID              string             `json:"TestCaseId"`
}

// StartTestCaseExecutionInput is the StartTestCaseExecution request.
type StartTestCaseExecutionInput struct {
	ClientToken string
	TestCaseID  string `json:"-" path:"TestCaseId"`
	InstanceID  string `json:"-" path:"InstanceId"`
}

// StopTestCaseExecutionInput is the StopTestCaseExecution request.
type StopTestCaseExecutionInput struct {
	ClientToken string
	execRef
}

type execRef struct {
	ExecutionID string `json:"-" path:"TestCaseExecutionId"`
	TestCaseID  string `json:"-" path:"TestCaseId"`
	InstanceID  string `json:"-" path:"InstanceId"`
}

// ListTestCaseExecutionsInput is the ListTestCaseExecutions request.
type ListTestCaseExecutionsInput struct {
	TestCaseID   string `json:"-" query:"testCaseId"`
	TestCaseName string `json:"-" query:"testCaseName"`
	Status       string `json:"-" query:"status"`
	NextToken    string `json:"-" query:"nextToken"`
	InstanceID   string `json:"-"                      path:"InstanceId"`
	StartTime    int64  `json:"-" query:"startTime"`
	EndTime      int64  `json:"-" query:"endTime"`
	MaxResults   int32  `json:"-" query:"maxResults"`
}

// ListTestCaseExecutionRecordsInput is the ListTestCaseExecutionRecords request.
type ListTestCaseExecutionRecordsInput struct {
	execRef
	Status     string `json:"-" query:"status"`
	NextToken  string `json:"-" query:"nextToken"`
	MaxResults int32  `json:"-" query:"maxResults"`
}

// ExecutionRecord is one observation of a test case execution.
type ExecutionRecord struct {
	Timestamp     awstime.EpochTime `json:"Timestamp"`
	ObservationID string            `json:"ObservationId"`
	Record        string            `json:"Record"`
	Status        string            `json:"Status"`
}

type startExecutionOut struct {
	Status              string `json:"Status"`
	TestCaseExecutionID string `json:"TestCaseExecutionId"`
	TestCaseID          string `json:"TestCaseId"`
}

type executionSummaryOut struct {
	StartTime awstime.EpochTime  `json:"StartTime"`
	EndTime   *awstime.EpochTime `json:"EndTime,omitempty"`
	Status    string             `json:"Status"`
}

func (r *testExecutionRec) view() TestCaseExecution {
	return TestCaseExecution{
		StartTime: r.Start, EndTime: r.End, Tags: cloneTags(r.Tags), TestCaseExecutionID: r.ID,
		TestCaseExecutionStatus: r.Status, TestCaseID: r.TestCaseID,
	}
}

func (r *testExecutionRec) finished() bool {
	return r.Status == execPassed || r.Status == execFailed || r.Status == execStopped
}

func checkExecutionStatus(field, v string) error {
	if v == "" {
		return nil
	}

	return oneOf(field, v, execInitiated, execPassed, execFailed, execInProgress, execStopped)
}

// StartTestCaseExecution starts an execution of a published test case.
func (b *Backend) StartTestCaseExecution(in *StartTestCaseExecutionInput) (*startExecutionOut, error) {
	return writeInstance(b, "StartTestCaseExecution", in.InstanceID, func(inst string) (*startExecutionOut, error) {
		tc, err := b.testCases.get(inst, in.TestCaseID)
		if err != nil {
			return nil, err
		}

		if tc.Published == nil {
			return nil, invalidRequestErr("test case %q has no published version to execute", in.TestCaseID)
		}

		rec := &testExecutionRec{
			InstanceID: inst, ID: newID(), TestCaseID: tc.ID, Status: execInitiated,
			Start: awstime.NewEpochTime(b.now()), Tags: cloneTags(tc.Tags),
		}
		b.testExecutions.put(rec)

		return &startExecutionOut{Status: rec.Status, TestCaseExecutionID: rec.ID, TestCaseID: rec.TestCaseID}, nil
	})
}

func (b *Backend) executionLocked(inst string, ref *execRef) (*testExecutionRec, error) {
	r, err := b.testExecutions.get(inst, ref.ExecutionID)
	if err != nil || r.TestCaseID != ref.TestCaseID {
		return nil, notFoundErr(labelTestExecution, ref.ExecutionID)
	}

	return r, nil
}

// StopTestCaseExecution stops a running test case execution.
func (b *Backend) StopTestCaseExecution(in *StopTestCaseExecutionInput) (*empty, error) {
	return writeInstance(b, "StopTestCaseExecution", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.executionLocked(inst, &in.execRef)
		if err != nil {
			return nil, err
		}

		if r.finished() {
			return nil, invalidRequestErr("test case execution %q already ended with status %s", r.ID, r.Status)
		}

		t := awstime.NewEpochTime(b.now())
		r.Status, r.End = execStopped, &t
		r.Records = append(r.Records, ExecutionRecord{
			Timestamp:     t,
			ObservationID: newID(),
			Record:        "execution stopped by StopTestCaseExecution",
			Status:        execStopped,
		})

		return &empty{}, nil
	})
}

func (b *Backend) executionMatches(inst string, in *ListTestCaseExecutionsInput, r *testExecutionRec) bool {
	if in.TestCaseID != "" && r.TestCaseID != in.TestCaseID {
		return false
	}

	if in.Status != "" && r.Status != in.Status {
		return false
	}

	if in.StartTime != 0 && r.Start.UnixMilli() < in.StartTime {
		return false
	}

	if in.EndTime != 0 && r.Start.UnixMilli() > in.EndTime {
		return false
	}

	if in.TestCaseName != "" {
		tc, err := b.testCases.get(inst, r.TestCaseID)

		return err == nil && strings.EqualFold(tc.Name, in.TestCaseName)
	}

	return true
}

// ListTestCaseExecutions lists executions, newest first, optionally filtered.
func (b *Backend) ListTestCaseExecutions(in *ListTestCaseExecutionsInput) (*map[string]any, error) {
	if err := checkExecutionStatus("status", in.Status); err != nil {
		return nil, err
	}

	if in.MaxResults > maxExecListPage {
		return nil, invalidParamErr("maxResults must be 1-%d", maxExecListPage)
	}

	all, err := readInstance(
		b,
		"ListTestCaseExecutions",
		in.InstanceID,
		func(inst string) ([]TestCaseExecution, error) {
			recs := slices.DeleteFunc(b.testExecutions.in(inst), func(r *testExecutionRec) bool {
				return !b.executionMatches(inst, in, r)
			})
			slices.SortFunc(recs, func(a, c *testExecutionRec) int {
				if d := c.Start.Compare(a.Start.Time); d != 0 {
					return d
				}

				return strings.Compare(a.ID, c.ID)
			})

			out := make([]TestCaseExecution, 0, len(recs))
			for _, r := range recs {
				out = append(out, r.view())
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, err
	}

	items, next, err := paged(all, in.NextToken, in.MaxResults)

	return listBody("TestCaseExecutions", orEmpty(items), next), err
}

// ListTestCaseExecutionRecords lists the observations recorded for an execution.
func (b *Backend) ListTestCaseExecutionRecords(in *ListTestCaseExecutionRecordsInput) (*map[string]any, error) {
	if err := checkExecutionStatus("status", in.Status); err != nil {
		return nil, err
	}

	if in.MaxResults > maxExecListPage {
		return nil, invalidParamErr("maxResults must be 1-%d", maxExecListPage)
	}

	all, err := readInstance(
		b,
		"ListTestCaseExecutionRecords",
		in.InstanceID,
		func(inst string) ([]ExecutionRecord, error) {
			r, gerr := b.executionLocked(inst, &in.execRef)
			if gerr != nil {
				return nil, gerr
			}

			return slices.DeleteFunc(slices.Clone(r.Records), func(x ExecutionRecord) bool {
				return in.Status != "" && x.Status != in.Status
			}), nil
		},
	)
	if err != nil {
		return nil, err
	}

	items, next, err := paged(all, in.NextToken, in.MaxResults)

	return listBody("ExecutionRecords", orEmpty(items), next), err
}

// GetTestCaseExecutionSummary returns the times and status of an execution.
func (b *Backend) GetTestCaseExecutionSummary(in *execRef) (*executionSummaryOut, error) {
	return readInstance(
		b,
		"GetTestCaseExecutionSummary",
		in.InstanceID,
		func(inst string) (*executionSummaryOut, error) {
			r, err := b.executionLocked(inst, in)
			if err != nil {
				return nil, err
			}

			return &executionSummaryOut{StartTime: r.Start, EndTime: r.End, Status: r.Status}, nil
		},
	)
}
