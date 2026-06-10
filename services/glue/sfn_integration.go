package glue

import "context"

// SFNStartJobRun implements the Step Functions Glue StartJobRun service integration.
func (b *InMemoryBackend) SFNStartJobRun(
	_ context.Context,
	jobName string,
	arguments map[string]string,
) (string, error) {
	run, err := b.StartJobRun(jobName, arguments)
	if err != nil {
		return "", err
	}

	return run.ID, nil
}
