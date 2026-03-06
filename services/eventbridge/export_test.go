package eventbridge

import "time"

// MatchPatternForTest exposes the internal matchPattern function for external tests.
func MatchPatternForTest(pattern, event string) bool {
	return matchPattern(pattern, event)
}

// ScheduleForTest wraps a scheduleExpression for testing.
type ScheduleForTest struct {
	expr scheduleExpression
}

// ParseScheduleExpressionForTest exposes parseScheduleExpression for external tests.
func ParseScheduleExpressionForTest(expr string) (*ScheduleForTest, error) {
	s, err := parseScheduleExpression(expr)
	if err != nil {
		return nil, err
	}

	return &ScheduleForTest{expr: s}, nil
}

// NextAfterForTest exposes NextAfter for external tests.
func (s *ScheduleForTest) NextAfterForTest(t time.Time) time.Time {
	return s.expr.NextAfter(t)
}
