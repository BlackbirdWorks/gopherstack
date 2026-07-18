package route53

import (
	"errors"
)

// Errors returned by the backend.
var (
	ErrHostedZoneNotFound    = errors.New("NoSuchHostedZone")
	ErrInvalidInput          = errors.New("InvalidInput")
	ErrInvalidAction         = errors.New("InvalidChangeBatch")
	ErrHealthCheckNotFound   = errors.New("NoSuchHealthCheck")
	ErrKeySigningKeyNotFound = errors.New("NoSuchKeySigningKey")
	// ErrCidrCollectionNotFound wire code intentionally carries the "Exception"
	// suffix — that is the literal AWS error code for this shape (unlike most
	// Route 53 NoSuch* errors), confirmed against aws-sdk-go-v2 route53 model.
	ErrCidrCollectionNotFound          = errors.New("NoSuchCidrCollectionException")
	ErrQueryLoggingConfigNotFound      = errors.New("NoSuchQueryLoggingConfig")
	ErrDelegationSetNotFound           = errors.New("NoSuchDelegationSet")
	ErrTrafficPolicyNotFound           = errors.New("NoSuchTrafficPolicy")
	ErrTrafficPolicyInstNotFound       = errors.New("NoSuchTrafficPolicyInstance")
	ErrChangeNotFound                  = errors.New("NoSuchChange")
	ErrNoSuchGeoLocation               = errors.New("NoSuchGeoLocation")
	ErrQueryLoggingConfigAlreadyExists = errors.New("QueryLoggingConfigAlreadyExists")
	ErrPublicZoneVPCAssociation        = errors.New("PublicZoneVPCAssociation")
	ErrVPCAssociationAuthorizationNF   = errors.New("VPCAssociationAuthorizationNotFound")
	ErrKeySigningKeyWithActiveStatusNF = errors.New("KeySigningKeyWithActiveStatusNotFound")
	ErrInvalidARecord                  = errors.New("invalid A record value")
	ErrInvalidAAAARecord               = errors.New("invalid AAAA record value")
	ErrInvalidMXRecord                 = errors.New("invalid MX record value")
	ErrInvalidSRVRecord                = errors.New("invalid SRV record value")
	ErrInvalidCAARecord                = errors.New("invalid CAA record value")
	ErrInvalidTXTRecord                = errors.New("invalid TXT record value")
	ErrInvalidCNAMERecord              = errors.New("invalid CNAME record value")
	ErrInvalidNSRecord                 = errors.New("invalid NS record value")
	ErrInvalidPTRRecord                = errors.New("invalid PTR record value")
	ErrInvalidNAPTRRecord              = errors.New("invalid NAPTR record value")
	ErrInvalidDSRecord                 = errors.New("invalid DS record value")
	ErrInvalidSPFRecord                = errors.New("invalid SPF record value")
	ErrTrafficPolicyInUse              = errors.New("TrafficPolicyInUse")
	// ErrInvalidKeySigningKeyStatus is returned when a KSK operation is attempted
	// while the key is in a status that does not permit it (e.g. deleting an
	// ACTIVE key). This is the real AWS wire error code — Route 53 has no
	// "KeySigningKeyNotInactive" error.
	ErrInvalidKeySigningKeyStatus = errors.New("InvalidKeySigningKeyStatus")
	ErrTrafficPolicyAlreadyExists = errors.New("TrafficPolicyAlreadyExists")
	ErrHostedZoneNotEmpty         = errors.New("HostedZoneNotEmpty")
	ErrLastVPCAssociation         = errors.New("LastVPCAssociation")
	// ErrHostedZoneAlreadyExists is returned when CreateHostedZone reuses a
	// CallerReference already associated with a hosted zone that has different
	// Name/Comment/PrivateZone values (AWS: HostedZoneAlreadyExists, 409).
	ErrHostedZoneAlreadyExists = errors.New("HostedZoneAlreadyExists")
	// ErrHealthCheckAlreadyExists is returned when CreateHealthCheck reuses a
	// CallerReference already associated with a health check that has a
	// different configuration (AWS: HealthCheckAlreadyExists, 409).
	ErrHealthCheckAlreadyExists = errors.New("HealthCheckAlreadyExists")
	// ErrKeySigningKeyAlreadyExists is returned when CreateKeySigningKey is
	// called with a name that already exists in the hosted zone (AWS:
	// KeySigningKeyAlreadyExists, 409).
	ErrKeySigningKeyAlreadyExists = errors.New("KeySigningKeyAlreadyExists")
	// ErrVPCAssociationNotFound is returned by DisassociateVPCFromHostedZone
	// when the given VPC is not associated with the hosted zone (AWS:
	// VPCAssociationNotFound, 404).
	ErrVPCAssociationNotFound = errors.New("VPCAssociationNotFound")
	// ErrTrafficPolicyInstanceAlreadyExists is returned when
	// CreateTrafficPolicyInstance targets a (hostedZoneID, name) pair that
	// already has a traffic policy instance (AWS:
	// TrafficPolicyInstanceAlreadyExists, 409).
	ErrTrafficPolicyInstanceAlreadyExists = errors.New("TrafficPolicyInstanceAlreadyExists")
	// ErrCidrCollectionAlreadyExists is returned when CreateCidrCollection is
	// called with a name that is already in use (AWS:
	// CidrCollectionAlreadyExistsException, 400).
	ErrCidrCollectionAlreadyExists = errors.New("CidrCollectionAlreadyExistsException")
	// ErrHealthCheckVersionMismatch is returned when UpdateHealthCheck is
	// called with a HealthCheckVersion that does not match the health
	// check's current version (AWS: HealthCheckVersionMismatch, 409).
	ErrHealthCheckVersionMismatch = errors.New("HealthCheckVersionMismatch")
	// ErrCidrCollectionVersionMismatch is returned when ChangeCidrCollection
	// is called with a CollectionVersion that does not match the
	// collection's current version (AWS:
	// CidrCollectionVersionMismatchException, 409).
	ErrCidrCollectionVersionMismatch = errors.New("CidrCollectionVersionMismatchException")
	// ErrCidrCollectionInUse is returned when DeleteCidrCollection is called
	// on a non-empty collection — real AWS requires the collection to be
	// emptied of locations/blocks before it can be deleted (AWS:
	// CidrCollectionInUseException, 400).
	ErrCidrCollectionInUse = errors.New("CidrCollectionInUseException")
	// ErrDelegationSetInUse is returned when DeleteReusableDelegationSet is
	// called on a delegation set that is still associated with one or more
	// hosted zones (AWS: DelegationSetInUse, 400).
	ErrDelegationSetInUse = errors.New("DelegationSetInUse")
)
