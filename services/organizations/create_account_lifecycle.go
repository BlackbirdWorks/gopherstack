package organizations

import (
	"fmt"
	"time"
)

type createParams struct {
	name, email, roleName, iamUserAccessToBilling string
	tags                                          []Tag
	govCloud                                      bool
}

type provisionReq struct{ accountID, roleName string }

type pendingCreate struct {
	dueAt    time.Time
	statusID string
	params   createParams
}

// SetLifecycleDelay sets how long CreateAccount/CreateGovCloudAccount stay
// IN_PROGRESS before settling. Zero settles synchronously.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.createDelay = d
}

func (b *InMemoryBackend) createAccountLocked(p createParams) *CreateAccountStatus {
	now := time.Now()

	b.statusCounter++
	status := &CreateAccountStatus{
		ID:                 fmt.Sprintf("car-%012d", b.statusCounter),
		AccountName:        p.name,
		State:              createAccountStateInProgress,
		RequestedTimestamp: epochSeconds(now),
	}
	b.createStatuses.Put(status)

	if b.createDelay > 0 {
		b.pendingCreates = append(b.pendingCreates, &pendingCreate{
			dueAt: now.Add(b.createDelay), statusID: status.ID, params: p,
		})
	} else {
		b.completeCreateLocked(status, p, now)
	}

	cp := *status

	return &cp
}

// completeCreateLocked settles status: a duplicate email yields FAILED with
// no account, as in AWS.
func (b *InMemoryBackend) completeCreateLocked(status *CreateAccountStatus, p createParams, now time.Time) {
	status.CompletedTimestamp = epochSeconds(now)

	if _, exists := b.emailToAccountID[p.email]; exists {
		status.State = createAccountStateFailed
		status.FailureReason = createAccountFailureEmailExists

		return
	}

	b.accountCounter++
	acctID := newAccountID(b.accountCounter)

	b.accounts.Put(&Account{
		ID:                     acctID,
		ARN:                    b.accountARN(b.org.ID, acctID),
		Name:                   p.name,
		Email:                  p.email,
		Status:                 accountStatusActive,
		JoinedMethod:           joinedMethodCreated,
		JoinedAt:               now,
		RoleName:               p.roleName,
		IamUserAccessToBilling: p.iamUserAccessToBilling,
	})
	b.accountParent[acctID] = b.root.ID
	b.addAccountChild(b.root.ID, acctID)
	b.attachDefaultSCPLocked(acctID)
	b.setTagsLocked(acctID, p.tags)

	if b.emailToAccountID == nil {
		b.emailToAccountID = make(map[string]string)
	}

	b.emailToAccountID[p.email] = acctID
	b.provisionQueue = append(b.provisionQueue, provisionReq{accountID: acctID, roleName: p.roleName})

	status.AccountID = acctID
	status.State = createAccountStateSucceeded

	if p.govCloud {
		status.GovCloudAccountID = newGovCloudAccountID(b.accountCounter)
	}
}

// settlePendingCreates completes every pending create whose delay elapsed.
func (b *InMemoryBackend) settlePendingCreates() {
	b.settleDue()
	b.flushProvisions()
}

func (b *InMemoryBackend) settleDue() {
	now := time.Now()

	b.mu.RLock("settlePendingCreates")
	idle := len(b.pendingCreates) == 0 || now.Before(b.pendingCreates[0].dueAt)
	b.mu.RUnlock()

	if idle {
		return
	}

	b.mu.Lock("settlePendingCreates")
	defer b.mu.Unlock()

	for len(b.pendingCreates) > 0 && !now.Before(b.pendingCreates[0].dueAt) {
		pc := b.pendingCreates[0]
		b.pendingCreates = b.pendingCreates[1:]

		if status, ok := b.createStatuses.Get(pc.statusID); ok && b.org != nil {
			b.completeCreateLocked(status, pc.params, now)
		}
	}
}

// failInterruptedCreatesLocked fails IN_PROGRESS statuses that can no longer
// settle (their pending work is not persisted).
func (b *InMemoryBackend) failInterruptedCreatesLocked(now time.Time) {
	b.pendingCreates = nil

	for _, s := range b.createStatuses.All() {
		if s.State == createAccountStateInProgress {
			s.State = createAccountStateFailed
			s.FailureReason = "INTERNAL_FAILURE"
			s.CompletedTimestamp = epochSeconds(now)
		}
	}
}
