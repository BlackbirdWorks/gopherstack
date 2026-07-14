package iam

import (
	"sort"
	"time"
)

// accessAdvisorJob represents a pending/completed access advisor job.
type accessAdvisorJob struct {
	JobID     string    `json:"jobID,omitempty"`
	EntityARN string    `json:"entityARN,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	Status    string    `json:"status,omitempty"` // IN_PROGRESS or COMPLETED
}

// GenerateServiceLastAccessedDetailsForEntity creates a new access-advisor job for the given entity ARN.
func (b *InMemoryBackend) GenerateServiceLastAccessedDetailsForEntity(entityARN string) string {
	jobID := "sladjob-" + newID("")

	c := b.comp()
	c.mu.Lock()
	c.accessAdvisorJobs[jobID] = &accessAdvisorJob{
		JobID:     jobID,
		EntityARN: entityARN,
		CreatedAt: time.Now().UTC(),
		Status:    jobStatusCompleted, // Immediately complete in the mock
	}
	c.mu.Unlock()

	return jobID
}

// GetServiceLastAccessedDetails returns the access details for a given job ID.
// Returns job status and the list of service access details.
func (b *InMemoryBackend) GetServiceLastAccessedDetails(jobID string) (string, []ServiceLastAccessedDetail, error) {
	c := b.comp()
	c.mu.Lock()
	job, exists := c.accessAdvisorJobs[jobID]
	c.mu.Unlock()

	if !exists {
		// Return COMPLETED with empty list if job not found (permissive mock behavior).
		return jobStatusCompleted, []ServiceLastAccessedDetail{}, nil
	}

	c.mu.Lock()
	entityDetails := c.serviceLastAccessed[job.EntityARN]
	c.mu.Unlock()

	result := make([]ServiceLastAccessedDetail, 0, len(entityDetails))

	for _, d := range entityDetails {
		result = append(result, d)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceNamespace < result[j].ServiceNamespace
	})

	return jobStatusCompleted, result, nil
}

// RecordServiceAccess records that an entity accessed a service.
func (b *InMemoryBackend) RecordServiceAccess(entityARN, serviceNamespace, serviceName string) {
	c := b.comp()
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.serviceLastAccessed[entityARN] == nil {
		c.serviceLastAccessed[entityARN] = make(map[string]ServiceLastAccessedDetail)
	}

	c.serviceLastAccessed[entityARN][serviceNamespace] = ServiceLastAccessedDetail{
		ServiceName:                serviceName,
		ServiceNamespace:           serviceNamespace,
		LastAuthenticated:          time.Now().UTC(),
		LastAuthenticatedArn:       entityARN,
		TotalAuthenticatedEntities: 1,
	}
}
