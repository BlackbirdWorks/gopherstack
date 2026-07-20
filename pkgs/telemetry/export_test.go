package telemetry

//nolint:gochecknoglobals // These variables are exported for testing purposes only.
var (
	ProcessLockHeldMetrics          = processLockHeldMetrics
	ProcessLockWaitersMetrics       = processLockWaitersMetrics
	FillMissingPercentiles          = fillMissingPercentiles
	CalculatePercentilesFromBuckets = calculatePercentilesFromBuckets
	EstimatePercentiles             = estimatePercentiles
)

// Types exported for tests.
type DashboardTest = Dashboard
type DeadlockInfoTest = DeadlockInfo
