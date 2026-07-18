package workmail

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// GetMailboxDetails returns mailbox quota and usage.
func (b *InMemoryBackend) GetMailboxDetails(orgID, userID string) (*MailboxDetails, error) {
	b.mu.RLock("GetMailboxDetails")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return nil, fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	quota := defaultMailboxQuota
	if q, ok := b.mailboxQuotas[orgID][u.UserID]; ok {
		quota = q
	}

	return &MailboxDetails{MailboxQuota: quota, MailboxSize: 0}, nil
}

// UpdateMailboxQuota updates the mailbox quota for a user.
func (b *InMemoryBackend) UpdateMailboxQuota(orgID, userID string, quota int32) error {
	b.mu.Lock("UpdateMailboxQuota")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	b.mailboxQuotas[orgID][u.UserID] = quota

	return nil
}

// --- Mailbox Permissions ---

// PutMailboxPermissions creates or updates mailbox permissions.
func (b *InMemoryBackend) PutMailboxPermissions(
	orgID, entityID, granteeID string,
	perms []string,
) error {
	b.mu.Lock("PutMailboxPermissions")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	granteeType := memberTypeUser
	if b.findGroup(orgID, granteeID) != nil {
		granteeType = memberTypeGroup
	}

	b.permissions.Put(&Permission{
		GranteeID:   granteeID,
		GranteeType: granteeType,
		Permissions: perms,
		orgID:       orgID,
		entityID:    actualID,
	})

	return nil
}

// DeleteMailboxPermissions removes mailbox permissions.
func (b *InMemoryBackend) DeleteMailboxPermissions(orgID, entityID, granteeID string) error {
	b.mu.Lock("DeleteMailboxPermissions")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	if !b.permissions.Delete(permissionKey(orgID, actualID, granteeID)) {
		return fmt.Errorf("%w: permission for grantee %q not found", ErrNotFound, granteeID)
	}

	return nil
}

// ListMailboxPermissions returns mailbox permissions for an entity.
func (b *InMemoryBackend) ListMailboxPermissions(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]*Permission, string, error) {
	b.mu.RLock("ListMailboxPermissions")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return nil, "", fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	perms := slices.Clone(b.permissionsByOrgEntity.Get(permissionOrgEntityKey(orgID, actualID)))
	sort.Slice(perms, func(i, j int) bool { return perms[i].GranteeID < perms[j].GranteeID })

	items, next := paginate(perms, maxResults, nextToken)

	return items, next, nil
}

// --- Mailbox Export Jobs ---

// StartMailboxExportJob starts a mailbox export job.
func (b *InMemoryBackend) StartMailboxExportJob(
	orgID, entityID, description, roleARN, kmsKeyARN, s3BucketName, s3Prefix string,
) (*MailboxExportJob, error) {
	b.mu.Lock("StartMailboxExportJob")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job := &MailboxExportJob{
		JobID:        newID(),
		EntityID:     entityID,
		Description:  description,
		RoleARN:      roleARN,
		KmsKeyARN:    kmsKeyARN,
		S3BucketName: s3BucketName,
		S3Prefix:     s3Prefix,
		S3Path:       s3Prefix + "/export.zip",
		State:        "RUNNING",
		StartTime:    time.Now(),
		orgID:        orgID,
	}
	b.exportJobs.Put(job)

	return job, nil
}

// CancelMailboxExportJob cancels a running mailbox export job.
func (b *InMemoryBackend) CancelMailboxExportJob(orgID, jobID string) error {
	b.mu.Lock("CancelMailboxExportJob")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job, ok := b.exportJobs.Get(orgKey(orgID, jobID))
	if !ok {
		return fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}
	job.State = "CANCELLED"
	job.EndTime = time.Now()

	return nil
}

// DescribeMailboxExportJob returns details of a mailbox export job.
func (b *InMemoryBackend) DescribeMailboxExportJob(orgID, jobID string) (*MailboxExportJob, error) {
	b.mu.RLock("DescribeMailboxExportJob")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job, ok := b.exportJobs.Get(orgKey(orgID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}

	return job, nil
}

// ListMailboxExportJobs lists mailbox export jobs for an org.
func (b *InMemoryBackend) ListMailboxExportJobs(
	orgID string, maxResults int32, nextToken string,
) ([]*MailboxExportJob, string, error) {
	b.mu.RLock("ListMailboxExportJobs")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.exportJobsByOrg.Get(orgID)
	jobs := make([]*MailboxExportJob, 0, len(byOrg))
	jobs = append(jobs, byOrg...)
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].JobID < jobs[k].JobID })
	page, next := paginate(jobs, maxResults, nextToken)

	return page, next, nil
}
