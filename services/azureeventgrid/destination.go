package azureeventgrid

import (
	"fmt"
	"strings"
)

const (
	propEndpointURL = "endpointUrl"
	propResourceID  = "resourceId"
	propQueueName   = "queueName"
	propContainer   = "blobContainerName"
	propQueueTTL    = "queueMessageTimeToLiveInSeconds"
)

func (d Destination) str(key string) string {
	s, _ := d.Properties[key].(string)

	return s
}

// resourceSegment returns the segment following the (case-insensitive) key in an ARM resource ID.
func resourceSegment(resourceID, key string) string {
	segs := strings.Split(resourceID, "/")
	for i := 0; i+1 < len(segs); i++ {
		if strings.EqualFold(segs[i], key) {
			return segs[i+1]
		}
	}

	return ""
}

func invalidSpec(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, args...))
}

func validateDestination(d Destination) error {
	switch d.EndpointType {
	case EndpointWebHook:
		return validateWebhook(d)
	case EndpointServiceBusQ:
		return requireSegment(d, "queues", "ServiceBusQueue destination needs a queue resourceId")
	case EndpointServiceBusT:
		return requireSegment(d, "topics", "ServiceBusTopic destination needs a topic resourceId")
	case EndpointEventHub:
		return requireSegment(d, "eventhubs", "EventHub destination needs an event hub resourceId")
	case EndpointStorageQueue:
		if d.str(propQueueName) == "" {
			return invalidSpec("StorageQueue destination needs a queueName")
		}

		return requireSegment(d, "storageAccounts", "StorageQueue destination needs a storage account resourceId")
	}

	return invalidSpec("endpoint type %q is not supported", d.EndpointType)
}

func validateWebhook(d Destination) error {
	if _, err := parseWebhookURL(d.str(propEndpointURL)); err != nil {
		return invalidSpec("WebHook destination needs an http(s) endpointUrl")
	}

	return nil
}

func requireSegment(d Destination, key, msg string) error {
	if resourceSegment(d.str(propResourceID), key) == "" {
		return invalidSpec("%s", msg)
	}

	return nil
}

func validateDeadLetter(d *Destination) error {
	if d == nil {
		return nil
	}

	if d.EndpointType != EndpointStorageBlob {
		return invalidSpec("dead-letter endpoint type %q is not supported", d.EndpointType)
	}

	if resourceSegment(d.str(propResourceID), "storageAccounts") == "" || d.str(propContainer) == "" {
		return invalidSpec(
			"StorageBlob dead-letter destination needs a storage account resourceId and blobContainerName",
		)
	}

	return nil
}
