package ec2

func registerAcceptAndAdvancedOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["AcceptAddressTransfer"] = h.handleAcceptAddressTransfer
	ops["AcceptCapacityReservationBillingOwnership"] = h.handleAcceptCapacityReservationBillingOwnership
	ops["AcceptReservedInstancesExchangeQuote"] = h.handleAcceptReservedInstancesExchangeQuote
	ops["AcceptTransitGatewayMulticastDomainAssociations"] = h.handleAcceptTransitGatewayMulticastDomainAssociations
	ops["AcceptTransitGatewayPeeringAttachment"] = h.handleAcceptTransitGatewayPeeringAttachment
	ops["AcceptTransitGatewayVpcAttachment"] = h.handleAcceptTransitGatewayVpcAttachment
	ops["AcceptVpcEndpointConnections"] = h.handleAcceptVpcEndpointConnections
	ops["AcceptVpcPeeringConnection"] = h.handleAcceptVpcPeeringConnection
	ops["AdvertiseByoipCidr"] = h.handleAdvertiseByoipCidr
	ops["AllocateHosts"] = h.handleAllocateHosts
	ops["DescribeCapacityReservations"] = h.handleDescribeCapacityReservations
	ops["DescribeByoipCidrs"] = h.handleDescribeByoipCidrs
	ops["DescribeHosts"] = h.handleDescribeHosts
}
