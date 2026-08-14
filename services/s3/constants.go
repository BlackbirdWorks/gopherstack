package s3

const (
	errAccessDenied    = "AccessDenied"
	xmlNamespaceS3     = "http://s3.amazonaws.com/doc/2006-03-01/"
	gopherstackName    = "gopherstack"
	errMalformedXML    = "MalformedXML"
	errMalformedXMLMsg = "The XML you provided was not well-formed or did not validate against" +
		" our published schema."
	errInvalidArgument        = "InvalidArgument"
	errNoSuchConfig           = "NoSuchConfiguration"
	opCompleteMultipartUpload = "CompleteMultipartUpload"
	xmlElemCode               = "Code"
	xmlElemMessage            = "Message"
	errAuthQueryParams        = "AuthorizationQueryParametersError"
	sqlOpAND                  = "AND"
	sqlValTrue                = "true"
	sqlValFalse               = "false"
	aclPrivate                = "private"
	aclPublicRead             = "public-read"
	aclPublicReadWrite        = "public-read-write"
	aclAuthenticatedRead      = "authenticated-read"
	aclLogDeliveryWrite       = "log-delivery-write"
	errCodeInternalError      = "InternalError"
	csvFileHeaderInfoUse      = "USE"
	errNoSuchKey              = "NoSuchKey"
	errInvalidRequest         = "InvalidRequest"
	errSignatureMismatch      = "SignatureDoesNotMatch"
	actionGetObjectLower      = "s3:getobject"
	errMalformedPolicy        = "MalformedPolicy"

	// maxAnnotationsPerObject is the documented per-object annotation cap
	// (s3@v1.106.5 api_op_PutObjectAnnotation.go doc comment: "Each object can
	// have up to 1,000 annotations.").
	maxAnnotationsPerObject = 1000
	// maxAnnotationNameBytes is PutObjectAnnotationInput.AnnotationName's
	// documented max length ("Maximum length of 512 bytes.").
	maxAnnotationNameBytes = 512
	// defaultMaxAnnotationResults is ListObjectAnnotations' page size when the
	// caller doesn't specify MaxAnnotationResults, capped at the same 1,000
	// ceiling as maxAnnotationsPerObject.
	defaultMaxAnnotationResults = 1000
)
