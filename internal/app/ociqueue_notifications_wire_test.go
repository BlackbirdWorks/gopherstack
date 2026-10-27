package app

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ocinotificationsbackend "github.com/blackbirdworks/gopherstack/services/ocinotifications"
	ociqueuebackend "github.com/blackbirdworks/gopherstack/services/ociqueue"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestWireOCIQueueKeyChecker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		withVault bool
		wantErr   bool
	}{
		{"unknown key rejected when vault runs", true, true},
		{"any key OCID accepted standalone", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			queueH := ociqueuebackend.NewHandler(ociqueuebackend.NewInMemoryBackend())
			byName := map[string]service.Registerable{"OCIQueue": queueH}

			if tt.withVault {
				byName["OCIVault"] = ocivaultbackend.NewHandler(ocivaultbackend.NewInMemoryBackend())
			}

			wireOCIQueueNotifications(byName)

			key := "ocid1.key.oc1..ghost"
			_, err := queueH.Backend.CreateQueue(ociqueuebackend.CreateQueueIn{
				Name: "q", Compartment: "ocid1.compartment.oc1..c", KMSKeyID: &key,
			})
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestWireOCINotificationsFunctions(t *testing.T) {
	t.Parallel()

	notifyH := ocinotificationsbackend.NewHandler(ocinotificationsbackend.NewInMemoryBackend())
	fnH := ocifunctionsbackend.NewHandler(ocifunctionsbackend.NewInMemoryBackend())

	wireOCIQueueNotifications(map[string]service.Registerable{"OCINotifications": notifyH, "OCIFunctions": fnH})

	b := notifyH.Backend
	b.SetTransition(0)

	topic, err := b.CreateTopic(
		ocinotificationsbackend.CreateTopicIn{Name: "t", Compartment: "ocid1.compartment.oc1..c"},
		"http://x",
	)
	require.NoError(t, err)

	_, err = b.CreateSubscription(ocinotificationsbackend.CreateSubscriptionIn{
		TopicID: topic.TopicID, Compartment: "ocid1.compartment.oc1..c", Protocol: "ORACLE_FUNCTIONS",
		Endpoint: "ocid1.fnfunc.oc1..ghost",
	}, "http://x")
	require.NoError(t, err)

	body := "payload"
	_, err = b.PublishMessage(topic.TopicID, "", ocinotificationsbackend.PublishIn{Body: &body})
	require.NoError(t, err)

	b.DeliverDue(t.Context(), func(f func()) { f() })

	ds := b.Deliveries(topic.TopicID, "")
	require.Len(t, ds, 1)
	assert.Equal(t, "PENDING", ds[0].Status, "an unknown function is retried, not delivered")
	assert.Equal(t, 1, ds[0].Attempts)
	assert.NotEmpty(t, ds[0].LastError)
	assert.NotEqual(t, http.StatusOK, ds[0].LastStatus)
}
