package emr

// sessions.go implements StartSession/GetSession/ListSessions/
// TerminateSession/GetSessionEndpoint -- EMR's interactive (Spark Connect)
// session family, added to the SDK after this backend's prior parity audit.
//
// Session state model. The real SessionState enum
// (aws-sdk-go-v2/service/emr/types/enums.go) has eight values: SUBMITTED,
// STARTING, STARTED, IDLE, BUSY, TERMINATING, TERMINATED, FAILED. Real
// StartSession's own doc says a session "enters the SUBMITTED state" when
// first created, and this backend honors that exactly. It deliberately does
// NOT simulate the SUBMITTED -> STARTING -> STARTED -> IDLE provisioning
// sequence a real Spark Connect session goes through to become usable:
// unlike a step's PENDING -> COMPLETED promotion (effectiveStepStatus in
// clusters.go), which fakes near-instant completion of work gopherstack
// never actually runs, reaching IDLE/STARTED here would require simulating
// readiness of a real Spark driver this emulator has no model for at all --
// there is no equivalent "the job trivially succeeds" story for a session,
// only "pretend a real engine finished booting", which is exactly the kind
// of fabricated progression the parity campaign flags. A session therefore
// stays SUBMITTED until it is explicitly terminated (directly, see
// TerminateSession below) or its cluster is terminated (terminateSingle's
// cascade in clusters.go). IDLE/STARTED/BUSY/STARTING are consequently
// unreachable in this backend; FAILED is also never produced (no simulated
// failure injection for sessions).
//
// TerminateSession resolves synchronously: State goes directly to
// TERMINATED, skipping the real API's intermediate TERMINATING step. This
// matches terminateSingle's own cluster-termination model (WAITING straight
// to TERMINATED, no TERMINATING), keeping the two termination paths
// consistent, and guarantees GetSession/ListSessions never show a
// TERMINATING session a real client's poll loop could get stuck on.

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// sessionCanStart reports whether a cluster in the given state may host a
// new session, per real StartSession's doc ("The cluster must be in the
// RUNNING or WAITING state").
func sessionCanStart(state string) bool {
	return state == StateWaiting || state == StateRunning
}

// findSessionIndex returns the index of the session with the given ID
// within cluster.sessions, or -1 if not found. Caller must hold at least a
// read lock.
func findSessionIndex(cluster *Cluster, sessionID string) int {
	for i := range cluster.sessions {
		if cluster.sessions[i].ID == sessionID {
			return i
		}
	}

	return -1
}

// cloneLogTypes deep-copies a log-component-to-log-types map.
func cloneLogTypes(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}

	out := make(map[string][]string, len(m))
	for k, v := range m {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}

	return out
}

// cloneSessionMonitoringConfiguration deep-copies a
// *SessionMonitoringConfiguration, including its nested logging configs.
func cloneSessionMonitoringConfiguration(mc *SessionMonitoringConfiguration) *SessionMonitoringConfiguration {
	if mc == nil {
		return nil
	}

	cp := *mc

	if mc.CloudWatchLoggingConfiguration != nil {
		cwCp := *mc.CloudWatchLoggingConfiguration
		cwCp.LogTypes = cloneLogTypes(mc.CloudWatchLoggingConfiguration.LogTypes)
		cp.CloudWatchLoggingConfiguration = &cwCp
	}

	cp.ManagedLoggingConfiguration = clonePtr(mc.ManagedLoggingConfiguration)

	if mc.S3LoggingConfiguration != nil {
		s3Cp := *mc.S3LoggingConfiguration
		s3Cp.LogTypes = cloneLogTypes(mc.S3LoggingConfiguration.LogTypes)
		cp.S3LoggingConfiguration = &s3Cp
	}

	return &cp
}

// clone returns a deep copy of the Session.
func (s Session) clone() Session {
	cp := s

	if s.Tags != nil {
		cp.Tags = make([]Tag, len(s.Tags))
		copy(cp.Tags, s.Tags)
	}

	cp.EngineConfigurations = cloneConfigurations(s.EngineConfigurations)
	cp.MonitoringConfiguration = cloneSessionMonitoringConfiguration(s.MonitoringConfiguration)

	return cp
}

// sessionARN builds the session's ARN as a sub-resource of its owning
// cluster, matching the hierarchical "cluster/<id>/..." resource pattern
// buildNewCluster/CreateStudio already use for this account's other
// elasticmapreduce ARNs.
func sessionARN(region, accountID, clusterID, sessionID string) string {
	return arn.Build("elasticmapreduce", region, accountID, "cluster/"+clusterID+"/session/"+sessionID)
}

// StartSession creates and starts a new interactive session on a cluster.
// The referenced cluster must exist, be in a state that can host a session
// (see sessionCanStart), and have been launched with SessionEnabled=true.
func (b *InMemoryBackend) StartSession(ctx context.Context, params StartSessionParams) (*Session, error) {
	if params.ClusterID == "" {
		return nil, fmt.Errorf("%w: ClusterId is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, params.ClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, params.ClusterID)
	}

	if !sessionCanStart(cluster.Status.State) {
		return nil, fmt.Errorf("%w: cluster %s is not in a state that can host a session (state %s)",
			errSessionClusterNotReady, params.ClusterID, cluster.Status.State)
	}

	if !cluster.SessionEnabled {
		return nil, fmt.Errorf("%w: cluster %s was not launched with SessionEnabled",
			errSessionsNotEnabled, params.ClusterID)
	}

	id := b.nextSessionID()
	now := awstime.Epoch(time.Now())

	tagsCopy := make([]Tag, len(params.Tags))
	copy(tagsCopy, params.Tags)

	session := Session{
		ID:                          id,
		ClusterID:                   params.ClusterID,
		ARN:                         sessionARN(region, b.accountID, params.ClusterID, id),
		AccountID:                   b.accountID,
		State:                       SessionStateSubmitted,
		Name:                        params.Name,
		ExecutionRoleArn:            params.ExecutionRoleArn,
		ReleaseLabel:                cluster.ReleaseLabel,
		EngineConfigurations:        cloneConfigurations(params.EngineConfigurations),
		MonitoringConfiguration:     cloneSessionMonitoringConfiguration(params.MonitoringConfiguration),
		Tags:                        tagsCopy,
		SessionIdleTimeoutInMinutes: params.SessionIdleTimeoutInMinutes,
		CreatedAt:                   now,
		UpdatedAt:                   now,
	}

	cluster.sessions = append(cluster.sessions, session)

	cp := session.clone()

	return &cp, nil
}

// GetSession returns a single session by cluster ID and session ID.
func (b *InMemoryBackend) GetSession(ctx context.Context, clusterID, sessionID string) (*Session, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	idx := findSessionIndex(cluster, sessionID)
	if idx < 0 {
		return nil, fmt.Errorf("%w: session %s not found", ErrNotFound, sessionID)
	}

	cp := cluster.sessions[idx].clone()

	return &cp, nil
}

// ListSessions returns the sessions on a cluster, optionally filtered by
// state, newest first (matching real ListSessions' doc: "Newer sessions are
// returned first").
func (b *InMemoryBackend) ListSessions(
	ctx context.Context,
	clusterID string,
	states []string,
	marker string,
) ([]Session, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	stateSet := buildStateSet(states)
	list := make([]Session, 0, len(cluster.sessions))

	for _, s := range cluster.sessions {
		if stateSet != nil && !stateSet[s.State] {
			continue
		}

		list = append(list, s.clone())
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt != list[j].CreatedAt {
			return list[i].CreatedAt > list[j].CreatedAt
		}

		return list[i].ID > list[j].ID
	})

	p := page.New(list, marker, listSessionsPageSize, listSessionsPageSize)

	return p.Data, p.Next, nil
}

// terminateSessionInPlace transitions a session directly to TERMINATED (a
// no-op if it is already TERMINATED or FAILED). See this file's package doc
// comment for why there is no intermediate TERMINATING step.
func terminateSessionInPlace(s *Session, now time.Time) {
	if s.State == SessionStateTerminated || s.State == SessionStateFailed {
		return
	}

	nowEpoch := awstime.Epoch(now)
	s.State = SessionStateTerminated
	s.EndedAt = nowEpoch
	s.UpdatedAt = nowEpoch
}

// TerminateSession terminates an active session on a cluster.
func (b *InMemoryBackend) TerminateSession(ctx context.Context, clusterID, sessionID string) (*Session, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	idx := findSessionIndex(cluster, sessionID)
	if idx < 0 {
		return nil, fmt.Errorf("%w: session %s not found", ErrNotFound, sessionID)
	}

	terminateSessionInPlace(&cluster.sessions[idx], time.Now())
	cp := cluster.sessions[idx].clone()

	return &cp, nil
}

// terminateClusterSessions cascades a cluster termination to every
// non-terminal session running on it -- a Spark Connect session cannot
// continue once its underlying cluster is gone. Called from terminateSingle
// (clusters.go) while b.mu is already held. Sessions are embedded directly
// on Cluster (see the sessions field in models.go), so no separate cleanup
// is needed when the janitor later sweeps this cluster's row entirely; this
// only handles the (cluster still exists, but TERMINATED) window in
// between.
func terminateClusterSessions(cluster *Cluster, now time.Time) {
	for i := range cluster.sessions {
		terminateSessionInPlace(&cluster.sessions[i], now)
	}
}

// sessionEndpointURL builds the synthetic Spark Connect endpoint URL for a
// session, following the same "https://<id>.<region>.<fake-service>.amazonaws.com"
// pattern GetPresignedURL (persistent_app_ui.go) already uses for other
// synthesized EMR endpoint URLs in this backend.
func sessionEndpointURL(sessionID, region string) string {
	return fmt.Sprintf("https://%s.%s.spark-connect-emr.amazonaws.com", sessionID, region)
}

// GetSessionEndpoint returns the Spark Connect endpoint URL, a synthesized
// auth token, and VPC-peering credentials for a session, verifying the
// cluster and session both exist.
func (b *InMemoryBackend) GetSessionEndpoint(
	ctx context.Context,
	clusterID, sessionID string,
) (*SessionEndpointResult, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetSessionEndpoint")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if findSessionIndex(cluster, sessionID) < 0 {
		return nil, fmt.Errorf("%w: session %s not found", ErrNotFound, sessionID)
	}

	return &SessionEndpointResult{
		Endpoint:  sessionEndpointURL(sessionID, region),
		AuthToken: "authtok-" + sessionID,
		Credentials: map[string]any{
			"UsernamePassword": map[string]string{
				"Username": "session-" + sessionID,
				"Password": "fake-password-" + sessionID,
			},
		},
		Expiry: time.Now().Add(sessionCredentialExpiry),
	}, nil
}
