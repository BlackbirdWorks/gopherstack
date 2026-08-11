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

	b.mu.Lock("GenerateServiceLastAccessedDetailsForEntity")
	defer b.mu.Unlock()

	b.comp().accessAdvisorJobs[jobID] = &accessAdvisorJob{
		JobID:     jobID,
		EntityARN: entityARN,
		CreatedAt: time.Now().UTC(),
		Status:    jobStatusCompleted, // Immediately complete in the mock
	}

	return jobID
}

// GetServiceLastAccessedDetails returns the access details for a given job ID.
// Returns job status and the list of service access details.
func (b *InMemoryBackend) GetServiceLastAccessedDetails(jobID string) (string, []ServiceLastAccessedDetail, error) {
	b.mu.RLock("GetServiceLastAccessedDetails")
	defer b.mu.RUnlock()

	c := b.comp()

	job, exists := c.accessAdvisorJobs[jobID]
	if !exists {
		// Return COMPLETED with empty list if job not found (permissive mock behavior).
		return jobStatusCompleted, []ServiceLastAccessedDetail{}, nil
	}

	entityDetails := c.serviceLastAccessed[job.EntityARN]

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
	b.mu.Lock("RecordServiceAccess")
	defer b.mu.Unlock()

	c := b.comp()
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
