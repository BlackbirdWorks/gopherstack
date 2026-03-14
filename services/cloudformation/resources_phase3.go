package cloudformation

import (
	"fmt"
	"strconv"
	"strings"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
)

// eksNodegroupDefaultDesiredSize is the default desired node count for an EKS nodegroup.
const eksNodegroupDefaultDesiredSize int32 = 2

// eksNodegroupDefaultMaxSize is the default max node count for an EKS nodegroup.
const eksNodegroupDefaultMaxSize int32 = 5

func (rc *ResourceCreator) createEKSCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	version := strProp(props, "Version", params, physicalIDs)
	roleARN := strProp(props, "RoleArn", params, physicalIDs)

	cluster, err := rc.backends.EKS.Backend.CreateCluster(name, version, roleARN, nil)
	if err != nil {
		return "", fmt.Errorf("create EKS cluster %s: %w", name, err)
	}

	return cluster.ARN, nil
}

func (rc *ResourceCreator) deleteEKSCluster(arn string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	_, err := rc.backends.EKS.Backend.DeleteCluster(name)

	return err
}

func (rc *ResourceCreator) createEKSNodegroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	nodegroupName := strProp(props, "NodegroupName", params, physicalIDs)
	if nodegroupName == "" {
		nodegroupName = logicalID
	}

	nodeRole := strProp(props, "NodeRole", params, physicalIDs)

	var instanceTypes []string
	if itRaw, ok := props["InstanceTypes"].([]any); ok {
		for _, v := range itRaw {
			if s, ok2 := v.(string); ok2 {
				instanceTypes = append(instanceTypes, s)
			}
		}
	}

	ng, err := rc.backends.EKS.Backend.CreateNodegroup(
		clusterName, nodegroupName, nodeRole,
		"AL2_x86_64", "ON_DEMAND", "",
		instanceTypes, eksNodegroupDefaultDesiredSize, 1, eksNodegroupDefaultMaxSize, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create EKS nodegroup %s: %w", nodegroupName, err)
	}

	return ng.ARN, nil
}

func (rc *ResourceCreator) deleteEKSNodegroup(arn string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	// ARN format: arn:aws:eks:{region}:{account}:nodegroup/{cluster}/{nodegroup}/{uuid}
	parts := strings.Split(arn, "/")
	const eksNodegroupARNMinParts = 3
	if len(parts) < eksNodegroupARNMinParts {
		return nil
	}

	clusterName := parts[len(parts)-eksNodegroupARNMinParts]
	nodegroupName := parts[len(parts)-2]

	_, err := rc.backends.EKS.Backend.DeleteNodegroup(clusterName, nodegroupName)

	return err
}

// ---- EFS ----

func (rc *ResourceCreator) createEFSFileSystem(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EFS == nil {
		return logicalID + "-stub", nil
	}

	performanceMode := strProp(props, "PerformanceMode", params, physicalIDs)
	throughputMode := strProp(props, "ThroughputMode", params, physicalIDs)

	var encrypted bool
	if v, ok := props["Encrypted"].(bool); ok {
		encrypted = v
	}

	token := logicalID + "-token"

	fs, err := rc.backends.EFS.Backend.CreateFileSystem(token, performanceMode, throughputMode, encrypted, nil)
	if err != nil {
		return "", fmt.Errorf("create EFS file system: %w", err)
	}

	return fs.FileSystemID, nil
}

func (rc *ResourceCreator) deleteEFSFileSystem(id string) error {
	if rc.backends.EFS == nil {
		return nil
	}

	return rc.backends.EFS.Backend.DeleteFileSystem(id)
}

func (rc *ResourceCreator) createEFSMountTarget(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EFS == nil {
		return logicalID + "-stub", nil
	}

	fileSystemID := strProp(props, "FileSystemId", params, physicalIDs)
	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	mt, err := rc.backends.EFS.Backend.CreateMountTarget(fileSystemID, subnetID, "")
	if err != nil {
		return "", fmt.Errorf("create EFS mount target: %w", err)
	}

	return mt.MountTargetID, nil
}

func (rc *ResourceCreator) deleteEFSMountTarget(id string) error {
	if rc.backends.EFS == nil {
		return nil
	}

	return rc.backends.EFS.Backend.DeleteMountTarget(id)
}

// ---- Batch ----

func (rc *ResourceCreator) createBatchComputeEnvironment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ComputeEnvironmentName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	ceType := strProp(props, "Type", params, physicalIDs)
	if ceType == "" {
		ceType = "MANAGED"
	}

	ce, err := rc.backends.Batch.Backend.CreateComputeEnvironment(name, ceType, "ENABLED", nil)
	if err != nil {
		return "", fmt.Errorf("create Batch compute environment %s: %w", name, err)
	}

	return ce.ComputeEnvironmentArn, nil
}

func (rc *ResourceCreator) deleteBatchComputeEnvironment(arnOrName string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	return rc.backends.Batch.Backend.DeleteComputeEnvironment(arnOrName)
}

func (rc *ResourceCreator) createBatchJobQueue(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "JobQueueName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var priority int32 = 1
	if pStr := strProp(props, "Priority", params, physicalIDs); pStr != "" {
		if p, err := strconv.ParseInt(pStr, 10, 32); err == nil {
			priority = int32(p)
		}
	} else if pRaw, ok := props["Priority"].(float64); ok {
		priority = int32(pRaw)
	}

	var ceOrder []batchbackend.ComputeEnvironmentOrder
	if rawList, ok := props["ComputeEnvironmentOrder"].([]any); ok {
		for i, item := range rawList {
			if m, ok2 := item.(map[string]any); ok2 {
				ceOrder = append(ceOrder, batchbackend.ComputeEnvironmentOrder{
					ComputeEnvironment: resolve(m["ComputeEnvironment"], params, physicalIDs),
					Order:              int32(i),
				})
			}
		}
	}

	jq, err := rc.backends.Batch.Backend.CreateJobQueue(name, priority, "ENABLED", ceOrder, nil)
	if err != nil {
		return "", fmt.Errorf("create Batch job queue %s: %w", name, err)
	}

	return jq.JobQueueArn, nil
}

func (rc *ResourceCreator) deleteBatchJobQueue(arnOrName string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	return rc.backends.Batch.Backend.DeleteJobQueue(arnOrName)
}

func (rc *ResourceCreator) createBatchJobDefinition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Batch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "JobDefinitionName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	defType := strProp(props, "Type", params, physicalIDs)
	if defType == "" {
		defType = "container"
	}

	jd, err := rc.backends.Batch.Backend.RegisterJobDefinition(name, defType, nil)
	if err != nil {
		return "", fmt.Errorf("create Batch job definition %s: %w", name, err)
	}

	return jd.JobDefinitionArn, nil
}

func (rc *ResourceCreator) deleteBatchJobDefinition(arnOrNameRev string) error {
	if rc.backends.Batch == nil {
		return nil
	}

	return rc.backends.Batch.Backend.DeregisterJobDefinition(arnOrNameRev)
}

// ---- CloudFront ----

func (rc *ResourceCreator) createCloudFrontDistribution(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	comment := logicalID
	var enabled = true

	if cfg, ok := props["DistributionConfig"].(map[string]any); ok {
		if c := resolve(cfg["Comment"], params, physicalIDs); c != "" {
			comment = c
		}

		if e, ok2 := cfg["Enabled"].(bool); ok2 {
			enabled = e
		}
	}

	dist, err := rc.backends.CloudFront.Backend.CreateDistribution(logicalID, comment, enabled, nil)
	if err != nil {
		return "", fmt.Errorf("create CloudFront distribution: %w", err)
	}

	return dist.ARN, nil
}

func (rc *ResourceCreator) deleteCloudFrontDistribution(arn string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	return rc.backends.CloudFront.Backend.DeleteDistribution(id)
}

// ---- AutoScaling ----

func (rc *ResourceCreator) createAutoScalingGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "AutoScalingGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	lcName := strProp(props, "LaunchConfigurationName", params, physicalIDs)

	var minSize, maxSize, desired int32 = 1, 1, 1

	if v, ok := props["MinSize"].(float64); ok {
		minSize = int32(v)
	} else if s := strProp(props, "MinSize", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			minSize = int32(n)
		}
	}

	if v, ok := props["MaxSize"].(float64); ok {
		maxSize = int32(v)
	} else if s := strProp(props, "MaxSize", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			maxSize = int32(n)
		}
	}

	if v, ok := props["DesiredCapacity"].(float64); ok {
		desired = int32(v)
	}

	_, err := rc.backends.Autoscaling.Backend.CreateAutoScalingGroup(autoscalingbackend.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: lcName,
		MinSize:                 minSize,
		MaxSize:                 maxSize,
		DesiredCapacity:         desired,
	})
	if err != nil {
		return "", fmt.Errorf("create AutoScaling group %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteAutoScalingGroup(name string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	return rc.backends.Autoscaling.Backend.DeleteAutoScalingGroup(name)
}

func (rc *ResourceCreator) createLaunchConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "LaunchConfigurationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	imageID := strProp(props, "ImageId", params, physicalIDs)
	instanceType := strProp(props, "InstanceType", params, physicalIDs)

	_, err := rc.backends.Autoscaling.Backend.CreateLaunchConfiguration(
		autoscalingbackend.CreateLaunchConfigurationInput{
			LaunchConfigurationName: name,
			ImageID:                 imageID,
			InstanceType:            instanceType,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create LaunchConfiguration %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteLaunchConfiguration(name string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	return rc.backends.Autoscaling.Backend.DeleteLaunchConfiguration(name)
}

// ---- API Gateway V2 ----

func (rc *ResourceCreator) createAPIGatewayV2API(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	protocolType := strProp(props, "ProtocolType", params, physicalIDs)
	if protocolType == "" {
		protocolType = "HTTP"
	}

	api, err := rc.backends.APIGatewayV2.Backend.CreateAPI(apigatewayv2backend.CreateAPIInput{
		Name:         name,
		ProtocolType: protocolType,
		Description:  strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 API %s: %w", name, err)
	}

	return api.APIID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2API(apiID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAPI(apiID)
}

func (rc *ResourceCreator) createAPIGatewayV2Stage(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	stageName := strProp(props, "StageName", params, physicalIDs)
	if stageName == "" {
		stageName = logicalID
	}

	_, err := rc.backends.APIGatewayV2.Backend.CreateStage(apiID, apigatewayv2backend.CreateStageInput{
		StageName: stageName,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 stage %s: %w", stageName, err)
	}

	return apiID + "/" + stageName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Stage(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	// physicalID format: {apiID}/{stageName}
	idx := strings.LastIndex(physicalID, "/")
	if idx < 0 {
		return nil
	}

	apiID := physicalID[:idx]
	stageName := physicalID[idx+1:]

	return rc.backends.APIGatewayV2.Backend.DeleteStage(apiID, stageName)
}

// ---- CodeBuild ----

func (rc *ResourceCreator) createCodeBuildProject(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeBuild == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	serviceRole := strProp(props, "ServiceRole", params, physicalIDs)

	var source codebuildbackend.ProjectSource
	if s, ok := props["Source"].(map[string]any); ok {
		source.Type = resolve(s["Type"], params, physicalIDs)
		source.Location = resolve(s["Location"], params, physicalIDs)
	}
	if source.Type == "" {
		source.Type = "NO_SOURCE"
	}

	var artifacts codebuildbackend.ProjectArtifacts
	if a, ok := props["Artifacts"].(map[string]any); ok {
		artifacts.Type = resolve(a["Type"], params, physicalIDs)
	}
	if artifacts.Type == "" {
		artifacts.Type = "NO_ARTIFACTS"
	}

	var env codebuildbackend.ProjectEnvironment
	if e, ok := props["Environment"].(map[string]any); ok {
		env.Type = resolve(e["Type"], params, physicalIDs)
		env.Image = resolve(e["Image"], params, physicalIDs)
		env.ComputeType = resolve(e["ComputeType"], params, physicalIDs)
	}
	if env.Type == "" {
		env.Type = "LINUX_CONTAINER"
	}
	if env.ComputeType == "" {
		env.ComputeType = "BUILD_GENERAL1_SMALL"
	}

	project, err := rc.backends.CodeBuild.Backend.CreateProject(
		name,
		description,
		source,
		artifacts,
		env,
		serviceRole,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create CodeBuild project %s: %w", name, err)
	}

	return project.Arn, nil
}

func (rc *ResourceCreator) deleteCodeBuildProject(arn string) error {
	if rc.backends.CodeBuild == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.CodeBuild.Backend.DeleteProject(name)
}

// ---- Glue ----

func (rc *ResourceCreator) createGlueDatabase(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID
	var description string

	if di, ok := props["DatabaseInput"].(map[string]any); ok {
		if n := resolve(di["Name"], params, physicalIDs); n != "" {
			name = n
		}

		description = resolve(di["Description"], params, physicalIDs)
	}

	db, err := rc.backends.Glue.Backend.CreateDatabase(gluebackend.DatabaseInput{
		Name:        name,
		Description: description,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue database %s: %w", name, err)
	}

	return db.ARN, nil
}

func (rc *ResourceCreator) deleteGlueDatabase(arn string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.Glue.Backend.DeleteDatabase(name)
}

func (rc *ResourceCreator) createGlueJob(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	role := strProp(props, "Role", params, physicalIDs)

	var cmd gluebackend.JobCommand
	if c, ok := props["Command"].(map[string]any); ok {
		cmd.Name = resolve(c["Name"], params, physicalIDs)
		cmd.ScriptLocation = resolve(c["ScriptLocation"], params, physicalIDs)
		cmd.PythonVersion = resolve(c["PythonVersion"], params, physicalIDs)
	}

	job, err := rc.backends.Glue.Backend.CreateJob(gluebackend.Job{
		Name:    name,
		Role:    role,
		Command: cmd,
	})
	if err != nil {
		return "", fmt.Errorf("create Glue job %s: %w", name, err)
	}

	return job.ARN, nil
}

func (rc *ResourceCreator) deleteGlueJob(arn string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.Glue.Backend.DeleteJob(name)
}

// ---- DocDB ----

func (rc *ResourceCreator) createDocDBCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	engine := strProp(props, "Engine", params, physicalIDs)
	masterUser := strProp(props, "MasterUsername", params, physicalIDs)
	dbName := strProp(props, "DBClusterParameterGroupName", params, physicalIDs)

	cluster, err := rc.backends.DocDB.Backend.CreateDBCluster(id, engine, masterUser, dbName, "", 0)
	if err != nil {
		return "", fmt.Errorf("create DocDB cluster %s: %w", id, err)
	}

	return cluster.DBClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteDocDBCluster(arn string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.DocDB.Backend.DeleteDBCluster(id)

	return err
}

func (rc *ResourceCreator) createDocDBInstance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DocDB == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBInstanceIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	clusterID := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	instanceClass := strProp(props, "DBInstanceClass", params, physicalIDs)
	engine := strProp(props, "Engine", params, physicalIDs)

	instance, err := rc.backends.DocDB.Backend.CreateDBInstance(id, clusterID, instanceClass, engine)
	if err != nil {
		return "", fmt.Errorf("create DocDB instance %s: %w", id, err)
	}

	return instance.DBInstanceIdentifier, nil
}

func (rc *ResourceCreator) deleteDocDBInstance(arn string) error {
	if rc.backends.DocDB == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.DocDB.Backend.DeleteDBInstance(id)

	return err
}

// ---- Neptune ----

func (rc *ResourceCreator) createNeptuneCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	paramGroupName := strProp(props, "DBClusterParameterGroupName", params, physicalIDs)

	cluster, err := rc.backends.Neptune.Backend.CreateDBCluster(id, paramGroupName, 0)
	if err != nil {
		return "", fmt.Errorf("create Neptune cluster %s: %w", id, err)
	}

	return cluster.DBClusterIdentifier, nil
}

func (rc *ResourceCreator) deleteNeptuneCluster(arn string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.Neptune.Backend.DeleteDBCluster(id)

	return err
}

func (rc *ResourceCreator) createNeptuneInstance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Neptune == nil {
		return logicalID + "-stub", nil
	}

	id := strProp(props, "DBInstanceIdentifier", params, physicalIDs)
	if id == "" {
		id = strings.ToLower(logicalID)
	}

	clusterID := strProp(props, "DBClusterIdentifier", params, physicalIDs)
	instanceClass := strProp(props, "DBInstanceClass", params, physicalIDs)

	instance, err := rc.backends.Neptune.Backend.CreateDBInstance(id, clusterID, instanceClass)
	if err != nil {
		return "", fmt.Errorf("create Neptune instance %s: %w", id, err)
	}

	return instance.DBInstanceIdentifier, nil
}

func (rc *ResourceCreator) deleteNeptuneInstance(arn string) error {
	if rc.backends.Neptune == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	_, err := rc.backends.Neptune.Backend.DeleteDBInstance(id)

	return err
}

// ---- MSK (Kafka) ----

func (rc *ResourceCreator) createMSKCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Kafka == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ClusterName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	kafkaVersion := strProp(props, "KafkaVersion", params, physicalIDs)
	if kafkaVersion == "" {
		kafkaVersion = "3.4.0"
	}

	var numBrokers int32 = 3
	if n, ok := props["NumberOfBrokerNodes"].(float64); ok {
		numBrokers = int32(n)
	}

	var brokerInfo kafkabackend.BrokerNodeGroupInfo
	if b, ok := props["BrokerNodeGroupInfo"].(map[string]any); ok {
		brokerInfo.InstanceType = resolve(b["InstanceType"], params, physicalIDs)
	}
	if brokerInfo.InstanceType == "" {
		brokerInfo.InstanceType = "kafka.m5.large"
	}

	cluster, err := rc.backends.Kafka.Backend.CreateCluster(name, kafkaVersion, numBrokers, brokerInfo, nil)
	if err != nil {
		return "", fmt.Errorf("create MSK cluster %s: %w", name, err)
	}

	return cluster.ClusterArn, nil
}

func (rc *ResourceCreator) deleteMSKCluster(arn string) error {
	if rc.backends.Kafka == nil {
		return nil
	}

	return rc.backends.Kafka.Backend.DeleteCluster(arn)
}

// ---- Transfer ----

func (rc *ResourceCreator) createTransferServer(
	logicalID string,
	props map[string]any,
	_, _ map[string]string,
) (string, error) {
	if rc.backends.Transfer == nil {
		return logicalID + "-stub", nil
	}

	var protocols []string
	if rawList, ok := props["Protocols"].([]any); ok {
		for _, v := range rawList {
			if s, ok2 := v.(string); ok2 {
				protocols = append(protocols, s)
			}
		}
	}
	if len(protocols) == 0 {
		protocols = []string{"SFTP"}
	}

	server, err := rc.backends.Transfer.Backend.CreateServer(protocols, nil)
	if err != nil {
		return "", fmt.Errorf("create Transfer server: %w", err)
	}

	return server.ServerID, nil
}

func (rc *ResourceCreator) deleteTransferServer(serverID string) error {
	if rc.backends.Transfer == nil {
		return nil
	}

	return rc.backends.Transfer.Backend.DeleteServer(serverID)
}

// ---- CloudTrail ----

func (rc *ResourceCreator) createCloudTrailTrail(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudTrail == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "TrailName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	s3Bucket := strProp(props, "S3BucketName", params, physicalIDs)
	s3KeyPrefix := strProp(props, "S3KeyPrefix", params, physicalIDs)
	snsTopicName := strProp(props, "SnsTopicName", params, physicalIDs)
	cwLogsLogGroupARN := strProp(props, "CloudWatchLogsLogGroupArn", params, physicalIDs)
	cwLogsRoleARN := strProp(props, "CloudWatchLogsRoleArn", params, physicalIDs)
	kmsKeyID := strProp(props, "KMSKeyId", params, physicalIDs)

	var includeGlobal, multiRegion, logValidation bool
	if v, ok := props["IncludeGlobalServiceEvents"].(bool); ok {
		includeGlobal = v
	}
	if v, ok := props["IsMultiRegionTrail"].(bool); ok {
		multiRegion = v
	}
	if v, ok := props["EnableLogFileValidation"].(bool); ok {
		logValidation = v
	}

	trail, err := rc.backends.CloudTrail.Backend.CreateTrail(
		name, s3Bucket, s3KeyPrefix, snsTopicName,
		cwLogsLogGroupARN, cwLogsRoleARN, kmsKeyID,
		includeGlobal, multiRegion, logValidation, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudTrail trail %s: %w", name, err)
	}

	return trail.TrailARN, nil
}

func (rc *ResourceCreator) deleteCloudTrailTrail(arn string) error {
	if rc.backends.CloudTrail == nil {
		return nil
	}

	return rc.backends.CloudTrail.Backend.DeleteTrail(arn)
}

// ---- CodePipeline ----

func (rc *ResourceCreator) createCodePipelinePipeline(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodePipeline == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID

	var decl codepipelinebackend.PipelineDeclaration
	if p, ok := props["Pipeline"].(map[string]any); ok {
		if n := resolve(p["Name"], params, physicalIDs); n != "" {
			name = n
		}

		decl.Name = name
		decl.RoleArn = resolve(p["RoleArn"], params, physicalIDs)

		if as, ok2 := p["ArtifactStore"].(map[string]any); ok2 {
			decl.ArtifactStore = codepipelinebackend.ArtifactStore{
				Type:     resolve(as["Type"], params, physicalIDs),
				Location: resolve(as["Location"], params, physicalIDs),
			}
		}
	} else {
		decl.Name = name
	}

	pipeline, err := rc.backends.CodePipeline.Backend.CreatePipeline(decl, nil)
	if err != nil {
		return "", fmt.Errorf("create CodePipeline pipeline %s: %w", name, err)
	}

	return pipeline.Metadata.PipelineArn, nil
}

func (rc *ResourceCreator) deleteCodePipelinePipeline(arn string) error {
	if rc.backends.CodePipeline == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.CodePipeline.Backend.DeletePipeline(name)
}

// ---- IoT ----

func (rc *ResourceCreator) createIoTThing(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	thingName := strProp(props, "ThingName", params, physicalIDs)
	if thingName == "" {
		thingName = logicalID
	}

	out, err := rc.backends.IoT.Backend.CreateThing(&iotbackend.CreateThingInput{
		ThingName: thingName,
	})
	if err != nil {
		return "", fmt.Errorf("create IoT thing %s: %w", thingName, err)
	}

	return out.ThingARN, nil
}

func (rc *ResourceCreator) deleteIoTThing(arn string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.IoT.Backend.DeleteThing(name)
}

func (rc *ResourceCreator) createIoTTopicRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IoT == nil {
		return logicalID + "-stub", nil
	}

	ruleName := strProp(props, "RuleName", params, physicalIDs)
	if ruleName == "" {
		ruleName = logicalID
	}

	var payload *iotbackend.TopicRulePayload
	if tp, ok := props["TopicRulePayload"].(map[string]any); ok {
		payload = &iotbackend.TopicRulePayload{
			SQL:         resolve(tp["SQL"], params, physicalIDs),
			Description: resolve(tp["Description"], params, physicalIDs),
		}
	}

	err := rc.backends.IoT.Backend.CreateTopicRule(&iotbackend.CreateTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: payload,
	})
	if err != nil {
		return "", fmt.Errorf("create IoT topic rule %s: %w", ruleName, err)
	}

	return "arn:aws:iot:" + rc.backends.Region + ":" + rc.backends.AccountID + ":rule/" + ruleName, nil
}

func (rc *ResourceCreator) deleteIoTTopicRule(arn string) error {
	if rc.backends.IoT == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.IoT.Backend.DeleteTopicRule(name)
}

// ---- Pipes ----

func (rc *ResourceCreator) createPipesPipe(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Pipes == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	roleARN := strProp(props, "RoleArn", params, physicalIDs)
	source := strProp(props, "Source", params, physicalIDs)
	target := strProp(props, "Target", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	pipe, err := rc.backends.Pipes.Backend.CreatePipe(name, roleARN, source, target, description, "", nil)
	if err != nil {
		return "", fmt.Errorf("create Pipes pipe %s: %w", name, err)
	}

	return pipe.ARN, nil
}

func (rc *ResourceCreator) deletePipesPipe(arn string) error {
	if rc.backends.Pipes == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.Pipes.Backend.DeletePipe(name)
}

// ---- EMR ----

func (rc *ResourceCreator) createEMRCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EMR == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	releaseLabel := strProp(props, "ReleaseLabel", params, physicalIDs)
	if releaseLabel == "" {
		releaseLabel = "emr-6.0.0"
	}

	cluster, err := rc.backends.EMR.Backend.RunJobFlow(name, releaseLabel, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create EMR cluster %s: %w", name, err)
	}

	return cluster.ARN, nil
}

func (rc *ResourceCreator) deleteEMRCluster(arn string) error {
	if rc.backends.EMR == nil {
		return nil
	}

	id := resourceNameFromARN(arn)

	return rc.backends.EMR.Backend.TerminateJobFlows([]string{id})
}

// ---- CloudWatch Dashboard ----

func (rc *ResourceCreator) createCloudWatchDashboard(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DashboardName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	body := strProp(props, "DashboardBody", params, physicalIDs)
	if body == "" {
		body = `{"widgets":[]}`
	}

	if err := rc.backends.CloudWatch.Backend.PutDashboard(name, body); err != nil {
		return "", fmt.Errorf("create CloudWatch dashboard %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteCloudWatchDashboard(name string) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	return rc.backends.CloudWatch.Backend.DeleteDashboards([]string{name})
}

// helpers for delete lookups in phase-3 resources

func (rc *ResourceCreator) deletePhase3ComputeResource(physicalID, resourceType string) (bool, error) {
	if handled, err := rc.deletePhase3ContainerResource(physicalID, resourceType); handled {
		return true, err
	}

	return rc.deletePhase3AppResource(physicalID, resourceType)
}

// deletePhase3ContainerResource handles EKS, EFS, and Batch deletions.
func (rc *ResourceCreator) deletePhase3ContainerResource(physicalID, resourceType string) (bool, error) {
	switch resourceType {
	case "AWS::EKS::Cluster":
		return true, rc.deleteEKSCluster(physicalID)
	case "AWS::EKS::Nodegroup":
		return true, rc.deleteEKSNodegroup(physicalID)
	case "AWS::EFS::FileSystem":
		return true, rc.deleteEFSFileSystem(physicalID)
	case "AWS::EFS::MountTarget":
		return true, rc.deleteEFSMountTarget(physicalID)
	case "AWS::Batch::ComputeEnvironment":
		return true, rc.deleteBatchComputeEnvironment(physicalID)
	case "AWS::Batch::JobQueue":
		return true, rc.deleteBatchJobQueue(physicalID)
	case "AWS::Batch::JobDefinition":
		return true, rc.deleteBatchJobDefinition(physicalID)
	default:
		return false, nil
	}
}

// deletePhase3AppResource handles CloudFront, AutoScaling, ApiGatewayV2, CodeBuild, and Glue deletions.
func (rc *ResourceCreator) deletePhase3AppResource(physicalID, resourceType string) (bool, error) {
	switch resourceType {
	case "AWS::CloudFront::Distribution":
		return true, rc.deleteCloudFrontDistribution(physicalID)
	case "AWS::AutoScaling::AutoScalingGroup":
		return true, rc.deleteAutoScalingGroup(physicalID)
	case "AWS::AutoScaling::LaunchConfiguration":
		return true, rc.deleteLaunchConfiguration(physicalID)
	case "AWS::ApiGatewayV2::Api":
		return true, rc.deleteAPIGatewayV2API(physicalID)
	case "AWS::ApiGatewayV2::Stage":
		return true, rc.deleteAPIGatewayV2Stage(physicalID)
	case "AWS::CodeBuild::Project":
		return true, rc.deleteCodeBuildProject(physicalID)
	case "AWS::Glue::Database":
		return true, rc.deleteGlueDatabase(physicalID)
	case "AWS::Glue::Job":
		return true, rc.deleteGlueJob(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) deletePhase3DataResource(physicalID, resourceType string) error {
	switch resourceType {
	case "AWS::DocDB::DBCluster":
		return rc.deleteDocDBCluster(physicalID)
	case "AWS::DocDB::DBInstance":
		return rc.deleteDocDBInstance(physicalID)
	case "AWS::Neptune::DBCluster":
		return rc.deleteNeptuneCluster(physicalID)
	case "AWS::Neptune::DBInstance":
		return rc.deleteNeptuneInstance(physicalID)
	case "AWS::MSK::Cluster":
		return rc.deleteMSKCluster(physicalID)
	case "AWS::Transfer::Server":
		return rc.deleteTransferServer(physicalID)
	case "AWS::CloudTrail::Trail":
		return rc.deleteCloudTrailTrail(physicalID)
	case "AWS::CodePipeline::Pipeline":
		return rc.deleteCodePipelinePipeline(physicalID)
	case "AWS::IoT::Thing":
		return rc.deleteIoTThing(physicalID)
	case "AWS::IoT::TopicRule":
		return rc.deleteIoTTopicRule(physicalID)
	case "AWS::Pipes::Pipe":
		return rc.deletePipesPipe(physicalID)
	case "AWS::EMR::Cluster":
		return rc.deleteEMRCluster(physicalID)
	case "AWS::CloudWatch::Dashboard":
		return rc.deleteCloudWatchDashboard(physicalID)
	default:
		return nil
	}
}
