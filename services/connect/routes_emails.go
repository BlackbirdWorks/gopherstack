package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func emailRoutes() []restroute.Route {
	const (
		list  = "/email-addresses/{InstanceId}"
		one   = list + "/{EmailAddressId}"
		queue = "/queues/{InstanceId}/{QueueId}"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: list, Op: "CreateEmailAddress"},
		{Method: http.MethodGet, Path: one, Op: "DescribeEmailAddress"},
		{Method: http.MethodPost, Path: one, Op: "UpdateEmailAddressMetadata"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteEmailAddress"},
		{Method: http.MethodPost, Path: one + "/associate-alias", Op: "AssociateEmailAddressAlias"},
		{Method: http.MethodPost, Path: one + "/disassociate-alias", Op: "DisassociateEmailAddressAlias"},
		{Method: http.MethodPost, Path: queue + "/associate-email-addresses", Op: "AssociateQueueEmailAddresses"},
		{Method: http.MethodPost, Path: queue + "/disassociate-email-addresses", Op: "DisassociateQueueEmailAddresses"},
		{Method: http.MethodGet, Path: queue + "/email-addresses", Op: "ListQueueEmailAddresses"},
	}
}
