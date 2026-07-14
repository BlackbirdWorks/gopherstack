package s3control

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// ErrNotFound is returned when public access block config is not found.
var ErrNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)

// ErrAlreadyExists is returned when a resource already exists.
var ErrAlreadyExists = awserr.New("BucketAlreadyExists", awserr.ErrAlreadyExists)

// ErrValidation is returned when a required parameter is missing or invalid.
var ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)

// errBucketNotFound is returned when an Outposts bucket is not found.
var errBucketNotFound = awserr.New("NoSuchBucket", awserr.ErrNotFound)

// errJobNotFound is returned when a batch job is not found.
var errJobNotFound = awserr.New("NoSuchJob", awserr.ErrNotFound)

// errMRAPNotFound is returned when a Multi-Region Access Point is not found.
var errMRAPNotFound = awserr.New("NoSuchMultiRegionAccessPoint", awserr.ErrNotFound)

// errObjectLambdaAPNotFound is returned when an Object Lambda AP is not found.
var errObjectLambdaAPNotFound = awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)

// errReplicationNotFound is returned when no replication config is found for a bucket.
var errReplicationNotFound = awserr.New("ReplicationConfigurationNotFoundError", awserr.ErrNotFound)

// errStorageLensConfigNotFound is returned when a Storage Lens configuration is not found.
var errStorageLensConfigNotFound = awserr.New("NoSuchConfiguration", awserr.ErrNotFound)

// errStorageLensGroupNotFound is returned when a Storage Lens group is not found.
var errStorageLensGroupNotFound = awserr.New("NoSuchStorageLensGroup", awserr.ErrNotFound)

// errAccessPointNotFound is returned when an access point is not found.
var errAccessPointNotFound = awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)

// errAPPABNotFound is returned when no per-AP public access block configuration exists.
var errAPPABNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)
