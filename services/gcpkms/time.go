package gcpkms

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func tsOf(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t.UTC()) }
