package appstream

import "fmt"

// usageReportSchedule is the only real UsageReportSchedule enum value
// (aws-sdk-go-v2 appstream@v1.64.5 types/enums.go: UsageReportScheduleDaily
// = "DAILY" is the sole member).
const usageReportSchedule = "DAILY"

type storedUsageReportSubscription struct {
	S3BucketName string `json:"s3BucketName"`
	Schedule     string `json:"schedule"`
}

func (u *storedUsageReportSubscription) toUsageReportSubscription() *UsageReportSubscription {
	return &UsageReportSubscription{
		S3BucketName: u.S3BucketName,
		Schedule:     u.Schedule,
	}
}

// CreateUsageReportSubscription creates a usage report subscription.
//
// Real CreateUsageReportSubscriptionInput takes no parameters
// (aws-sdk-go-v2 appstream@v1.64.5 api_op_CreateUsageReportSubscription.go)
// -- AWS derives both the schedule (always DAILY) and the S3 bucket
// server-side rather than accepting them from the caller.
func (b *InMemoryBackend) CreateUsageReportSubscription() (*UsageReportSubscription, error) {
	b.mu.Lock("CreateUsageReportSubscription")
	defer b.mu.Unlock()

	if b.usageReport != nil {
		return nil, ErrAlreadyExists
	}

	b.usageReport = &storedUsageReportSubscription{
		S3BucketName: fmt.Sprintf("appstream-logs-%s-%s", b.region, b.accountID),
		Schedule:     usageReportSchedule,
	}

	return b.usageReport.toUsageReportSubscription(), nil
}

// DeleteUsageReportSubscription removes the usage report subscription.
func (b *InMemoryBackend) DeleteUsageReportSubscription() error {
	b.mu.Lock("DeleteUsageReportSubscription")
	defer b.mu.Unlock()

	if b.usageReport == nil {
		return ErrNotFound
	}

	b.usageReport = nil

	return nil
}

// DescribeUsageReportSubscriptions returns the usage report subscription.
func (b *InMemoryBackend) DescribeUsageReportSubscriptions() ([]*UsageReportSubscription, error) {
	b.mu.RLock("DescribeUsageReportSubscriptions")
	defer b.mu.RUnlock()

	if b.usageReport == nil {
		return []*UsageReportSubscription{}, nil
	}

	return []*UsageReportSubscription{b.usageReport.toUsageReportSubscription()}, nil
}
