package gcppubsub

import (
	"context"
	"errors"
	"io"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	pullMaxWait       = 10 * time.Second
	defaultPullMax    = 100
	streamOwnerIDSize = 12
)

type subscriberServer struct {
	pubsubpb.UnimplementedSubscriberServer
	b *InMemoryBackend
}

func (s *subscriberServer) CreateSubscription(
	_ context.Context,
	in *pubsubpb.Subscription,
) (*pubsubpb.Subscription, error) {
	return s.b.CreateSubscription(in)
}

func (s *subscriberServer) GetSubscription(
	_ context.Context,
	in *pubsubpb.GetSubscriptionRequest,
) (*pubsubpb.Subscription, error) {
	return s.b.GetSubscription(in.GetSubscription())
}

func (s *subscriberServer) UpdateSubscription(
	_ context.Context,
	in *pubsubpb.UpdateSubscriptionRequest,
) (*pubsubpb.Subscription, error) {
	return s.b.UpdateSubscription(in)
}

func (s *subscriberServer) ListSubscriptions(
	_ context.Context,
	in *pubsubpb.ListSubscriptionsRequest,
) (*pubsubpb.ListSubscriptionsResponse, error) {
	subs, next, err := s.b.ListSubscriptions(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListSubscriptionsResponse{Subscriptions: subs, NextPageToken: next}, nil
}

func (s *subscriberServer) DeleteSubscription(
	_ context.Context,
	in *pubsubpb.DeleteSubscriptionRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteSubscription(in.GetSubscription())
}

func (s *subscriberServer) ModifyPushConfig(
	_ context.Context,
	in *pubsubpb.ModifyPushConfigRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.ModifyPushConfig(in.GetSubscription(), in.GetPushConfig())
}

func (s *subscriberServer) GetSnapshot(_ context.Context, in *pubsubpb.GetSnapshotRequest) (*pubsubpb.Snapshot, error) {
	return s.b.GetSnapshot(in.GetSnapshot())
}

func (s *subscriberServer) ListSnapshots(
	_ context.Context,
	in *pubsubpb.ListSnapshotsRequest,
) (*pubsubpb.ListSnapshotsResponse, error) {
	snaps, next, err := s.b.ListSnapshots(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListSnapshotsResponse{Snapshots: snaps, NextPageToken: next}, nil
}

func (s *subscriberServer) CreateSnapshot(
	_ context.Context,
	in *pubsubpb.CreateSnapshotRequest,
) (*pubsubpb.Snapshot, error) {
	return s.b.CreateSnapshot(in)
}

func (s *subscriberServer) UpdateSnapshot(
	_ context.Context,
	in *pubsubpb.UpdateSnapshotRequest,
) (*pubsubpb.Snapshot, error) {
	return s.b.UpdateSnapshot(in)
}

func (s *subscriberServer) DeleteSnapshot(
	_ context.Context,
	in *pubsubpb.DeleteSnapshotRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteSnapshot(in.GetSnapshot())
}

func (s *subscriberServer) Seek(_ context.Context, in *pubsubpb.SeekRequest) (*pubsubpb.SeekResponse, error) {
	return &pubsubpb.SeekResponse{}, s.b.Seek(in)
}

func (s *subscriberServer) Acknowledge(_ context.Context, in *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	if len(in.GetAckIds()) == 0 {
		return nil, errInvalid("ack_ids must not be empty")
	}

	_, invalid, err := s.b.Acknowledge(in.GetSubscription(), in.GetAckIds())
	if err != nil {
		return nil, err
	}

	if len(invalid) > 0 && s.b.strictAcks(in.GetSubscription()) {
		return nil, errInvalid("Some acknowledge IDs are invalid or expired: %v", invalid)
	}

	return &emptypb.Empty{}, nil
}

func (s *subscriberServer) ModifyAckDeadline(
	_ context.Context,
	in *pubsubpb.ModifyAckDeadlineRequest,
) (*emptypb.Empty, error) {
	if len(in.GetAckIds()) == 0 {
		return nil, errInvalid("ack_ids must not be empty")
	}

	if d := in.GetAckDeadlineSeconds(); d < 0 || d > maxAckDeadline {
		return nil, errInvalid("ack_deadline_seconds must be between 0 and %d", maxAckDeadline)
	}

	_, invalid, err := s.b.ModifyAckDeadline(in.GetSubscription(), in.GetAckIds(), []int32{in.GetAckDeadlineSeconds()})
	if err != nil {
		return nil, err
	}

	if len(invalid) > 0 && s.b.strictAcks(in.GetSubscription()) {
		return nil, errInvalid("Some acknowledge IDs are invalid or expired: %v", invalid)
	}

	return &emptypb.Empty{}, nil
}

func (s *subscriberServer) Pull(ctx context.Context, in *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	maxMsgs := int(in.GetMaxMessages())
	if maxMsgs < 0 {
		return nil, errInvalid("max_messages must be positive")
	}

	if maxMsgs == 0 {
		maxMsgs = defaultPullMax
	}

	maxMsgs = min(maxMsgs, maxPublishBatch)

	immediate := in.GetReturnImmediately() //nolint:staticcheck // deprecated but still honoured by clients
	timer := time.NewTimer(pullMaxWait)
	defer timer.Stop()

	for {
		res, err := s.b.pull(in.GetSubscription(), maxMsgs)
		if err != nil {
			return nil, err
		}

		if len(res.msgs) > 0 || immediate {
			return &pubsubpb.PullResponse{ReceivedMessages: res.msgs}, nil
		}

		var due <-chan time.Time

		var wakeTimer *time.Timer

		if !res.nextWake.IsZero() {
			wakeTimer = time.NewTimer(max(time.Until(res.nextWake), time.Millisecond))
			due = wakeTimer.C
		}

		select {
		case <-ctx.Done():
			stopTimer(wakeTimer)

			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
			stopTimer(wakeTimer)

			return &pubsubpb.PullResponse{}, nil
		case <-res.wake:
		case <-due:
		}

		stopTimer(wakeTimer)
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

type streamState struct {
	owner       string
	deadline    time.Duration
	maxMsgs     int64
	maxBytes    int64
	exactlyOnce bool
}

func (st *streamState) absorb(req *pubsubpb.StreamingPullRequest) {
	if d := req.GetStreamAckDeadlineSeconds(); d > 0 {
		st.deadline = time.Duration(min(max(d, minAckDeadline), maxAckDeadline)) * time.Second
	}

	if req.GetMaxOutstandingMessages() != 0 || req.GetMaxOutstandingBytes() != 0 || req.GetClientId() != "" {
		st.maxMsgs = req.GetMaxOutstandingMessages()
		st.maxBytes = req.GetMaxOutstandingBytes()
	}
}

func (s *subscriberServer) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return streamEnd(err)
	}

	sub := first.GetSubscription()

	props, err := s.b.subProps(sub)
	if err != nil {
		return err
	}

	if first.GetStreamAckDeadlineSeconds() == 0 {
		return errInvalid("stream_ack_deadline_seconds must be set on the first StreamingPull request")
	}

	st := &streamState{
		owner:       awsid.NewN("stream-", streamOwnerIDSize),
		deadline:    props.ackDeadline,
		exactlyOnce: props.exactlyOnce,
	}
	st.absorb(first)

	reqs := make(chan *pubsubpb.StreamingPullRequest)
	recvErr := make(chan error, 1)

	go func() {
		for {
			req, rerr := stream.Recv()
			if rerr != nil {
				recvErr <- rerr

				return
			}

			select {
			case reqs <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	wireProps := &pubsubpb.StreamingPullResponse_SubscriptionProperties{
		ExactlyOnceDeliveryEnabled: props.exactlyOnce, MessageOrderingEnabled: props.ordering,
	}
	if err = stream.Send(&pubsubpb.StreamingPullResponse{SubscriptionProperties: wireProps}); err != nil {
		return err
	}

	if err = s.streamAcks(stream, sub, st, first); err != nil {
		return err
	}

	return s.streamLoop(ctx, stream, sub, st, reqs, recvErr)
}

func (s *subscriberServer) streamLoop(
	ctx context.Context, stream pubsubpb.Subscriber_StreamingPullServer, sub string, st *streamState,
	reqs <-chan *pubsubpb.StreamingPullRequest, recvErr <-chan error,
) error {
	for {
		res, err := s.b.streamLease(sub, st.owner, st.deadline, st.maxMsgs, st.maxBytes)
		if err != nil {
			return err
		}

		if len(res.msgs) > 0 {
			if err = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: res.msgs}); err != nil {
				return err
			}

			continue
		}

		var due <-chan time.Time

		var wakeTimer *time.Timer

		if !res.nextWake.IsZero() {
			wakeTimer = time.NewTimer(max(time.Until(res.nextWake), time.Millisecond))
			due = wakeTimer.C
		}

		select {
		case <-ctx.Done():
			stopTimer(wakeTimer)

			return nil
		case rerr := <-recvErr:
			stopTimer(wakeTimer)

			return streamEnd(rerr)
		case req := <-reqs:
			stopTimer(wakeTimer)
			st.absorb(req)

			if err = s.streamAcks(stream, sub, st, req); err != nil {
				return err
			}

			continue
		case <-res.wake:
		case <-due:
		}

		stopTimer(wakeTimer)
	}
}

func streamEnd(err error) error {
	if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
		return nil
	}

	return err
}

func (s *subscriberServer) streamAcks(
	stream pubsubpb.Subscriber_StreamingPullServer, sub string, st *streamState, req *pubsubpb.StreamingPullRequest,
) error {
	resp := &pubsubpb.StreamingPullResponse{}

	if ids := req.GetAckIds(); len(ids) > 0 {
		acked, invalid, err := s.b.Acknowledge(sub, ids)
		if err != nil {
			return err
		}

		if st.exactlyOnce {
			resp.AcknowledgeConfirmation = &pubsubpb.StreamingPullResponse_AcknowledgeConfirmation{
				AckIds: acked, InvalidAckIds: invalid,
			}
		}
	}

	if ids := req.GetModifyDeadlineAckIds(); len(ids) > 0 {
		done, invalid, err := s.b.ModifyAckDeadline(sub, ids, req.GetModifyDeadlineSeconds())
		if err != nil {
			return err
		}

		if st.exactlyOnce {
			resp.ModifyAckDeadlineConfirmation = &pubsubpb.StreamingPullResponse_ModifyAckDeadlineConfirmation{
				AckIds: done, InvalidAckIds: invalid,
			}
		}
	}

	if resp.GetAcknowledgeConfirmation() == nil && resp.GetModifyAckDeadlineConfirmation() == nil {
		return nil
	}

	return stream.Send(resp)
}
