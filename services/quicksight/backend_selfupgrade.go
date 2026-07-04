package quicksight

import (
	"sort"
	"strings"
	"time"
)

const (
	selfUpgradeStatusAutoApproval  = "AUTO_APPROVAL"
	selfUpgradeStatusAdminApproval = "ADMIN_APPROVAL"

	selfUpgradeActionApprove = "APPROVE"
	selfUpgradeActionDeny    = "DENY"
	selfUpgradeActionVerify  = "VERIFY"

	selfUpgradeRequestStatusApproved = "APPROVED"
	selfUpgradeRequestStatusDenied   = "DENIED"
)

// storedSelfUpgradeRequest is the persisted representation of one namespace
// self-upgrade request. There is no CreateSelfUpgradeRequest operation in the
// QuickSight API (requests are created by end users self-requesting a role
// upgrade through the console); requests only ever enter backend state via
// seedSelfUpgradeRequest, used by tests to populate fixtures for
// ListSelfUpgrades/UpdateSelfUpgrade.
type storedSelfUpgradeRequest struct {
	CreationTime            time.Time `json:"creationTime"`
	LastUpdateAttemptTime   time.Time `json:"lastUpdateAttemptTime,omitzero"`
	LastUpdateFailureReason string    `json:"lastUpdateFailureReason,omitempty"`
	OriginalRole            string    `json:"originalRole"`
	RequestNote             string    `json:"requestNote,omitempty"`
	RequestStatus           string    `json:"requestStatus"`
	RequestedRole           string    `json:"requestedRole"`
	UpgradeRequestID        string    `json:"upgradeRequestId"`
}

func (r *storedSelfUpgradeRequest) toSelfUpgradeRequestDetail() *SelfUpgradeRequestDetail {
	return &SelfUpgradeRequestDetail{
		CreationTime:            r.CreationTime.Unix(),
		LastUpdateAttemptTime:   unixOrZero(r.LastUpdateAttemptTime),
		LastUpdateFailureReason: r.LastUpdateFailureReason,
		OriginalRole:            r.OriginalRole,
		RequestNote:             r.RequestNote,
		RequestStatus:           r.RequestStatus,
		RequestedRole:           r.RequestedRole,
		UpgradeRequestID:        r.UpgradeRequestID,
	}
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.Unix()
}

func selfUpgradeConfigKey(accountID, namespace string) string {
	return accountID + "/" + namespace
}

func selfUpgradeRequestKey(accountID, namespace, upgradeRequestID string) string {
	return accountID + "/" + namespace + "/" + upgradeRequestID
}

// seedSelfUpgradeRequest inserts r directly into backend state, bypassing the
// (real) AWS API. Exported for tests only (via export_test.go's
// SeedSelfUpgradeRequest) since QuickSight has no CreateSelfUpgradeRequest
// operation.
func (b *InMemoryBackend) seedSelfUpgradeRequest(accountID, namespace string, r *SelfUpgradeRequestDetail) {
	b.mu.Lock("seedSelfUpgradeRequest")
	defer b.mu.Unlock()

	var lastAttempt time.Time
	if r.LastUpdateAttemptTime != 0 {
		lastAttempt = time.Unix(r.LastUpdateAttemptTime, 0).UTC()
	}

	b.selfUpgradeRequests[selfUpgradeRequestKey(accountID, namespace, r.UpgradeRequestID)] = &storedSelfUpgradeRequest{
		CreationTime:            time.Unix(r.CreationTime, 0).UTC(),
		LastUpdateAttemptTime:   lastAttempt,
		LastUpdateFailureReason: r.LastUpdateFailureReason,
		OriginalRole:            r.OriginalRole,
		RequestNote:             r.RequestNote,
		RequestStatus:           r.RequestStatus,
		RequestedRole:           r.RequestedRole,
		UpgradeRequestID:        r.UpgradeRequestID,
	}
}

// ---- Self-upgrade configuration ----

// DescribeSelfUpgradeConfiguration returns namespace's self-upgrade status,
// defaulting to ADMIN_APPROVAL (the more conservative option) when never
// configured, mirroring DescribeQuickSightQSearchConfiguration's
// default-when-unset convention.
func (b *InMemoryBackend) DescribeSelfUpgradeConfiguration(accountID, namespace string) (string, error) {
	b.mu.RLock("DescribeSelfUpgradeConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return "", ErrNamespaceNotFound
	}

	if status, ok := b.selfUpgradeConfig[selfUpgradeConfigKey(accountID, namespace)]; ok {
		return status, nil
	}

	return selfUpgradeStatusAdminApproval, nil
}

func isValidSelfUpgradeStatus(status string) bool {
	switch status {
	case selfUpgradeStatusAutoApproval, selfUpgradeStatusAdminApproval:
		return true
	}

	return false
}

// UpdateSelfUpgradeConfiguration sets namespace's self-upgrade status.
func (b *InMemoryBackend) UpdateSelfUpgradeConfiguration(accountID, namespace, status string) (string, error) {
	if !isValidSelfUpgradeStatus(status) {
		return "", ErrValidation
	}

	b.mu.Lock("UpdateSelfUpgradeConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return "", ErrNamespaceNotFound
	}

	b.selfUpgradeConfig[selfUpgradeConfigKey(accountID, namespace)] = status

	return status, nil
}

// ---- Self-upgrade requests ----

func (b *InMemoryBackend) ListSelfUpgrades(
	accountID, namespace string,
	maxResults int32,
	nextToken string,
) ([]*SelfUpgradeRequestDetail, string, error) {
	b.mu.RLock("ListSelfUpgrades")
	defer b.mu.RUnlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, "", ErrNamespaceNotFound
	}

	prefix := accountID + "/" + namespace + "/"
	var all []*storedSelfUpgradeRequest
	for k, r := range b.selfUpgradeRequests {
		if strings.HasPrefix(k, prefix) {
			all = append(all, r)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].UpgradeRequestID < all[j].UpgradeRequestID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, r := range all {
			if r.UpgradeRequestID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].UpgradeRequestID
	} else {
		end = len(all)
	}

	result := make([]*SelfUpgradeRequestDetail, 0, end-start)
	for _, r := range all[start:end] {
		result = append(result, r.toSelfUpgradeRequestDetail())
	}

	return result, next, nil
}

func isValidSelfUpgradeAction(action string) bool {
	switch action {
	case selfUpgradeActionApprove, selfUpgradeActionDeny, selfUpgradeActionVerify:
		return true
	}

	return false
}

// UpdateSelfUpgrade approves, denies, or (re-)verifies an existing self-upgrade
// request. VERIFY re-confirms an already-decided request without changing its
// RequestStatus (there is no distinct "VERIFIED" status in the QuickSight API;
// only PENDING/APPROVED/DENIED/UPDATE_FAILED/VERIFY_FAILED exist), but always
// refreshes LastUpdateAttemptTime.
func (b *InMemoryBackend) UpdateSelfUpgrade(
	accountID, namespace, action, upgradeRequestID string,
) (*SelfUpgradeRequestDetail, error) {
	if upgradeRequestID == "" || !isValidSelfUpgradeAction(action) {
		return nil, ErrValidation
	}

	b.mu.Lock("UpdateSelfUpgrade")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	r, ok := b.selfUpgradeRequests[selfUpgradeRequestKey(accountID, namespace, upgradeRequestID)]
	if !ok {
		return nil, ErrSelfUpgradeRequestNotFound
	}

	switch action {
	case selfUpgradeActionApprove:
		r.RequestStatus = selfUpgradeRequestStatusApproved
	case selfUpgradeActionDeny:
		r.RequestStatus = selfUpgradeRequestStatusDenied
	}
	r.LastUpdateAttemptTime = time.Now().UTC()

	return r.toSelfUpgradeRequestDetail(), nil
}
