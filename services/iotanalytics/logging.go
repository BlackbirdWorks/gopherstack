package iotanalytics

import "fmt"

// DescribeLoggingOptions returns the current IoT Analytics logging options.
func (b *InMemoryBackend) DescribeLoggingOptions() (*LoggingOptions, error) {
	b.mu.RLock("DescribeLoggingOptions")
	defer b.mu.RUnlock()

	if b.loggingOptions == nil {
		return nil, ErrLoggingOptionsNotFound
	}

	opts := *b.loggingOptions

	return &opts, nil
}

// PutLoggingOptions sets the IoT Analytics logging options.
// Validates: level must be "ERROR"; roleArn is required when enabled is true.
func (b *InMemoryBackend) PutLoggingOptions(options *LoggingOptions) error {
	if options.Level != "ERROR" {
		return fmt.Errorf("%w: loggingOptions.level must be ERROR", ErrValidation)
	}

	if options.Enabled && options.RoleARN == "" {
		return fmt.Errorf("%w: loggingOptions.roleArn is required when enabled is true", ErrValidation)
	}

	b.mu.Lock("PutLoggingOptions")
	defer b.mu.Unlock()

	opts := *options
	b.loggingOptions = &opts

	return nil
}
