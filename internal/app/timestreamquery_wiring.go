package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// tsqNotifier publishes scheduled query notifications to SNS.
type tsqNotifier struct{ sns *snsbackend.InMemoryBackend }

func (n tsqNotifier) Publish(ctx context.Context, topicARN, message string) error {
	_, err := n.sns.PublishContext(ctx, topicARN, message, "", "", nil)

	return err
}

// wireTimestreamQueryIntegrations gives Timestream Query its SNS notifier and the S3 store used for
// scheduled query error reports and UNLOAD output.
func wireTimestreamQueryIntegrations(byName map[string]service.Registerable) {
	h, ok := byName["TimestreamQuery"].(*timestreamquerybackend.Handler)
	if !ok {
		return
	}

	bk, ok := h.Backend.(*timestreamquerybackend.InMemoryBackend)
	if !ok {
		return
	}

	if snsH, sok := byName["SNS"].(*snsbackend.Handler); sok {
		if snsBk, bok := snsH.Backend.(*snsbackend.InMemoryBackend); bok {
			bk.SetNotifier(tsqNotifier{sns: snsBk})
		}
	}

	if s3H, sok := byName["S3"].(*s3backend.S3Handler); sok {
		if s3Bk, bok := s3H.Backend.(*s3backend.InMemoryBackend); bok {
			bk.SetObjectStore(s3Bk)
		}
	}
}
