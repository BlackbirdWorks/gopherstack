package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceCreator_Lambda_NilBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		physID     string
		wantPhysID string
		isDelete   bool
	}{
		{
			name:      "create_returns_stub",
			logicalID: "MyFunction",
			props: map[string]any{
				"FunctionName": "my-fn",
				"Runtime":      "python3.12",
				"Handler":      "index.handler",
			},
			wantPhysID: "MyFunction-stub",
		},
		{
			name:     "delete_nil_path",
			physID:   "my-fn",
			isDelete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newServiceBackends() leaves Lambda=nil, so Lambda functions use stub path.
			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			if tt.isDelete {
				err := rc.Delete(t.Context(), "AWS::Lambda::Function", tt.physID, nil)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Lambda::Function",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)
		})
	}
}

func TestResourceCreator_EC2Resources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildProps   func() map[string]any
		name         string
		logicalID    string
		resourceType string
		wantNotEmpty bool
	}{
		{
			name:         "ec2_vpc",
			logicalID:    "MyVPC",
			resourceType: "AWS::EC2::VPC",
			buildProps:   func() map[string]any { return map[string]any{"CidrBlock": "10.0.0.0/16"} },
			wantNotEmpty: true,
		},
		{
			name:         "ec2_internet_gateway",
			logicalID:    "MyIGW",
			resourceType: "AWS::EC2::InternetGateway",
			buildProps:   func() map[string]any { return map[string]any{} },
			wantNotEmpty: true,
		},
		{
			name:         "ec2_security_group",
			logicalID:    "MySecurityGroup",
			resourceType: "AWS::EC2::SecurityGroup",
			buildProps: func() map[string]any {
				return map[string]any{
					"GroupName":        "cfn-sg",
					"GroupDescription": "CloudFormation test SG",
				}
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)
			props := tt.buildProps()

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, props, nil, nil)
			require.NoError(t, err)

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			err = rc.Delete(t.Context(), tt.resourceType, physID, props)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_EC2SubnetAndRouteTable(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create VPC first.
	vpcID, err := rc.Create(t.Context(), "MyVPC", "AWS::EC2::VPC",
		map[string]any{"CidrBlock": "10.0.0.0/16"}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, vpcID)

	// Create Subnet.
	subnetID, err := rc.Create(t.Context(), "MySubnet", "AWS::EC2::Subnet",
		map[string]any{
			"VpcId":            vpcID,
			"CidrBlock":        "10.0.1.0/24",
			"AvailabilityZone": "us-east-1a",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, subnetID)

	// Create RouteTable.
	rtID, err := rc.Create(t.Context(), "MyRouteTable", "AWS::EC2::RouteTable",
		map[string]any{"VpcId": vpcID}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, rtID)

	// Create Route.
	routePhysID, err := rc.Create(t.Context(), "MyRoute", "AWS::EC2::Route",
		map[string]any{
			"RouteTableId":         rtID,
			"DestinationCidrBlock": "0.0.0.0/0",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, routePhysID)

	// Delete in reverse order.
	err = rc.Delete(t.Context(), "AWS::EC2::Route", routePhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::RouteTable", rtID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::Subnet", subnetID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::VPC", vpcID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaESM_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create a Lambda function first.
	fnARN, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-test-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fnARN)

	// Create a Kinesis stream for ESM.
	streamARN, err := rc.Create(t.Context(), "MyStream", "AWS::Kinesis::Stream",
		map[string]any{"Name": "cfn-esm-stream", "ShardCount": float64(1)},
		nil, nil)
	require.NoError(t, err)

	// Create EventSourceMapping.
	esmID, err := rc.Create(t.Context(), "MyESM", "AWS::Lambda::EventSourceMapping",
		map[string]any{
			"FunctionName":     "cfn-test-fn",
			"EventSourceArn":   streamARN,
			"StartingPosition": "TRIM_HORIZON",
			"BatchSize":        float64(10),
			"Enabled":          true,
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, esmID)

	err = rc.Delete(t.Context(), "AWS::Lambda::EventSourceMapping", esmID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaAlias_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create function.
	_, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-alias-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)

	// Create alias pointing to $LATEST.
	aliasARN, err := rc.Create(t.Context(), "MyAlias", "AWS::Lambda::Alias",
		map[string]any{
			"FunctionName":    "cfn-alias-fn",
			"Name":            "prod",
			"FunctionVersion": "$LATEST",
			"Description":     "Production alias",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, aliasARN)
	assert.Contains(t, aliasARN, "prod")

	err = rc.Delete(t.Context(), "AWS::Lambda::Alias", aliasARN, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaVersion_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create function.
	_, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-version-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)

	// Publish a version.
	versionARN, err := rc.Create(t.Context(), "MyVersion", "AWS::Lambda::Version",
		map[string]any{
			"FunctionName": "cfn-version-fn",
			"Description":  "v1",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, versionARN)

	err = rc.Delete(t.Context(), "AWS::Lambda::Version", versionARN, nil)
	require.NoError(t, err)
}

func TestResourceCreator_APIGatewaySubResources(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create REST API.
	apiID, err := rc.Create(t.Context(), "MyAPI", "AWS::ApiGateway::RestApi",
		map[string]any{"Name": "cfn-api", "Description": "CFN test API"}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, apiID)

	// Find root resource.
	resources, _, err := backends.APIGateway.Backend.GetResources(apiID, "", 100)
	require.NoError(t, err)

	var rootID string
	for _, r := range resources {
		if r.Path == "/" {
			rootID = r.ID

			break
		}
	}
	require.NotEmpty(t, rootID, "root resource should exist")

	// Create sub-resource.
	resourcePhysID, err := rc.Create(t.Context(), "MyResource", "AWS::ApiGateway::Resource",
		map[string]any{
			"RestApiId": apiID,
			"ParentId":  rootID,
			"PathPart":  "items",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, resourcePhysID)

	// Create method.
	methodPhysID, err := rc.Create(t.Context(), "MyMethod", "AWS::ApiGateway::Method",
		map[string]any{
			"RestApiId":         apiID,
			"ResourceId":        resourcePhysID,
			"HttpMethod":        "GET",
			"AuthorizationType": "NONE",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, methodPhysID)

	// Create deployment.
	deployPhysID, err := rc.Create(t.Context(), "MyDeployment", "AWS::ApiGateway::Deployment",
		map[string]any{
			"RestApiId":   apiID,
			"StageName":   "prod",
			"Description": "Production deployment",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, deployPhysID)

	// Create stage.
	stagePhysID, err := rc.Create(t.Context(), "MyStage", "AWS::ApiGateway::Stage",
		map[string]any{
			"RestApiId": apiID,
			"StageName": "prod",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, stagePhysID)

	// Delete.
	err = rc.Delete(t.Context(), "AWS::ApiGateway::Stage", stagePhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Deployment", deployPhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Method", methodPhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Resource", resourcePhysID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaPermission_RealBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "success_with_function_name",
			logicalID: "MyPermission",
			props: map[string]any{
				"FunctionName": "cfn-perm-fn",
				"Action":       "lambda:InvokeFunction",
				"Principal":    "apigateway.amazonaws.com",
			},
			wantContains: "cfn-perm-fn",
		},
		{
			name:      "error_missing_function_name",
			logicalID: "BadPermission",
			props:     map[string]any{},
			wantErr:   cloudformation.ErrFunctionNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newLambdaServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Lambda::Permission",
				tt.props,
				nil,
				nil,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Lambda::Permission", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestEC2_DeleteSubnet_NotFound(t *testing.T) {
	t.Parallel()

	bk := ec2backend.NewInMemoryBackend("000000000000", "us-east-1")
	err := bk.DeleteSubnet("subnet-notexist")
	require.ErrorIs(t, err, ec2backend.ErrSubnetNotFound)
}

func TestResourceCreator_ECSServiceCreateDelete(t *testing.T) {
	t.Parallel()

	backends := newAdditionalServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create cluster.
	clusterARN, err := rc.Create(ctx, "MyCluster", "AWS::ECS::Cluster",
		map[string]any{"ClusterName": "unit-ecs-cluster"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, clusterARN)

	// Create task definition.
	tdARN, err := rc.Create(ctx, "MyTD", "AWS::ECS::TaskDefinition",
		map[string]any{
			"Family":      "unit-ecs-family",
			"NetworkMode": "bridge",
			"ContainerDefinitions": []any{
				map[string]any{"Name": "app", "Image": "nginx:latest"},
			},
		}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, tdARN)

	physIDs := map[string]string{
		"MyCluster": clusterARN,
		"MyTD":      tdARN,
	}

	// Create service.
	svcARN, err := rc.Create(ctx, "MySvc", "AWS::ECS::Service",
		map[string]any{
			"ServiceName":    "unit-ecs-service",
			"Cluster":        clusterARN,
			"TaskDefinition": tdARN,
			"DesiredCount":   float64(0),
		}, nil, physIDs)
	require.NoError(t, err)
	require.NotEmpty(t, svcARN)

	// Delete service.
	err = rc.Delete(ctx, "AWS::ECS::Service", svcARN, nil)
	require.NoError(t, err)
}

// TestResourceCreator_EC2Instance_CreateDelete verifies that EC2 instances
// are created in the backend and terminated on delete.
func TestResourceCreator_EC2Instance_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newMoreTypesServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"ImageId":      "ami-0c55b159cbfafe1f0",
		"InstanceType": "t3.micro",
	}

	physID, err := rc.Create(t.Context(), "MyInstance", "AWS::EC2::Instance", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
	assert.Contains(t, physID, "i-", "EC2 instance physical ID should look like an instance ID")

	err = rc.Delete(t.Context(), "AWS::EC2::Instance", physID, nil)
	require.NoError(t, err)
}
