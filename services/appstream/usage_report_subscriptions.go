package appstream

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
func (b *InMemoryBackend) CreateUsageReportSubscription(schedule, s3Bucket string) (*UsageReportSubscription, error) {
	b.mu.Lock("CreateUsageReportSubscription")
	defer b.mu.Unlock()

	if b.usageReport != nil {
		return nil, ErrAlreadyExists
	}

	sched := schedule
	if sched == "" {
		sched = "DAILY"
	}

	b.usageReport = &storedUsageReportSubscription{
		S3BucketName: s3Bucket,
		Schedule:     sched,
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
