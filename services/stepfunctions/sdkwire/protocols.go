package sdkwire

import (
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/aws/smithy-go/transport/http/protocol/awsjson"
	"github.com/aws/smithy-go/transport/http/protocol/awsquery"
	"github.com/aws/smithy-go/transport/http/protocol/ec2query"
	"github.com/aws/smithy-go/transport/http/protocol/restjson1"
	"github.com/aws/smithy-go/transport/http/protocol/restxml"
	"github.com/aws/smithy-go/transport/http/protocol/rpcv2"
)

func queryProtocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return awsquery.New(s) }

func awsjson11Protocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return awsjson.New11(s) }

func restjson1Protocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return restjson1.New(s) }

func ec2queryProtocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return ec2query.New(s) }

func restxmlProtocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return restxml.New(s) }

func rpcv2CBORProtocol(s *smithy.ServiceSchema) smithyhttp.ClientProtocol { return rpcv2.NewCBOR(s) }
