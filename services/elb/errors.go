package elb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrLoadBalancerNotFound is returned when the requested load balancer does not exist.
	ErrLoadBalancerNotFound = awserr.New("LoadBalancerNotFound", awserr.ErrNotFound)

	// ErrLoadBalancerAlreadyExists is returned when a load balancer with that name already exists.
	ErrLoadBalancerAlreadyExists = awserr.New("DuplicateLoadBalancerName", awserr.ErrAlreadyExists)

	// ErrInvalidParameter is returned when a request parameter is invalid or missing.
	ErrInvalidParameter = awserr.New("ValidationError", awserr.ErrInvalidParameter)

	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = awserr.New("InvalidAction", awserr.ErrInvalidParameter)

	// ErrPolicyNotFound is returned when a policy does not exist.
	ErrPolicyNotFound = awserr.New("PolicyNotFound", awserr.ErrNotFound)

	// ErrPolicyAlreadyExists is returned when a policy with that name already exists.
	ErrPolicyAlreadyExists = awserr.New("DuplicatePolicyName", awserr.ErrAlreadyExists)

	// ErrListenerNotFound is returned when a listener on the requested port does not exist.
	ErrListenerNotFound = awserr.New("ListenerNotFound", awserr.ErrNotFound)

	// ErrInvalidInstance is returned when a specified instance is not registered with the LB.
	ErrInvalidInstance = awserr.New("InvalidInstance", awserr.ErrInvalidParameter)

	// ErrDuplicateListener is returned when a listener already exists on the requested port.
	ErrDuplicateListener = awserr.New("DuplicateListener", awserr.ErrAlreadyExists)

	// ErrInvalidConfiguration is returned when an operation is not valid for the LB's configuration.
	ErrInvalidConfiguration = awserr.New("InvalidConfigurationRequest", awserr.ErrInvalidParameter)

	// ErrTooManyLoadBalancers is returned when creating a load balancer would exceed the
	// per-region account limit (AWS: TooManyAccessPointsException / "TooManyLoadBalancers").
	ErrTooManyLoadBalancers = awserr.New("TooManyLoadBalancers", awserr.ErrInvalidParameter)

	// ErrTooManyTags is returned when tagging a load balancer would exceed the per-resource
	// tag limit (AWS: TooManyTagsException / "TooManyTags").
	ErrTooManyTags = awserr.New("TooManyTags", awserr.ErrInvalidParameter)

	// ErrDuplicateTagKeys is returned when a single AddTags/CreateLoadBalancer request
	// specifies the same tag key more than once (AWS: DuplicateTagKeysException).
	ErrDuplicateTagKeys = awserr.New("DuplicateTagKeys", awserr.ErrInvalidParameter)

	// ErrInvalidScheme is returned when the Scheme parameter is not 'internet-facing' or
	// 'internal' (AWS: InvalidSchemeException).
	ErrInvalidScheme = awserr.New("InvalidScheme", awserr.ErrInvalidParameter)

	// ErrPolicyTypeNotFound is returned when the requested policy type name does not exist
	// (AWS: PolicyTypeNotFoundException). Distinct from ErrPolicyNotFound, which is for
	// policy *instances*, not policy *types*.
	ErrPolicyTypeNotFound = awserr.New("PolicyTypeNotFound", awserr.ErrNotFound)

	// ErrUnsupportedProtocol is returned when a listener specifies a protocol other than
	// HTTP, HTTPS, TCP, or SSL (AWS: UnsupportedProtocolException).
	ErrUnsupportedProtocol = awserr.New("UnsupportedProtocol", awserr.ErrInvalidParameter)
)
