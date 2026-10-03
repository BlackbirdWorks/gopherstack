package schemas

var queryErrors = []any{
	&smithytraits.AWSQueryError{ErrorCode: "InvalidParameterInput",
		HTTPStatusCode: 400},
	&smithytraits.AWSQueryError{ErrorCode: "LimitExceeded", HTTPStatusCode: 400},
}
