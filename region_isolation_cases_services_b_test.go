package main

import (
	"bytes"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iotwireless"
	iotwirelesstypes "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	kafkaconnecttypes "github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/macie2"
	macie2types "github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbtypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/mediapackage"
	"github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor"
	mediatailortypes "github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mwaa"
	mwaatypes "github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	networkmonitortypes "github.com/aws/aws-sdk-go-v2/service/networkmonitor/types"
	"github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint"
	pinpointtypes "github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	redshiftdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	resourcegroupstypes "github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	"github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	texttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	tsqtypes "github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/transcribe"
	transcribetypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	vptypes "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workspaces"
)

func servicesBIsolationCases() []regionCase {
	return []regionCase{
		iotwirelessCase(),
		kafkaconnectCase(),
		kinesisanalyticsCase(),
		kinesisvideoCase(),
		lakeformationCase(),
		macie2Case(),
		managedblockchainCase(),
		mediaconvertCase(),
		medialiveCase(),
		mediapackageCase(),
		mediastoreCase(),
		mediastoredataCase(),
		mediatailorCase(),
		mgnCase(),
		mwaaCase(),
		networkmanagerCase(),
		networkmonitorCase(),
		omicsCase(),
		opsworksCase(),
		outpostsCase(),
		personalizeCase(),
		pinpointCase(),
		pollyCase(),
		quicksightCase(),
		ramCase(),
		redshiftdataCase(),
		rekognitionCase(),
		resiliencehubCase(),
		resourcegroupsCase(),
		rolesanywhereCase(),
		s3tablesCase(),
		serverlessrepoCase(),
		ssoadminCase(),
		swfCase(),
		textractCase(),
		timestreamqueryCase(),
		timestreamwriteCase(),
		transcribeCase(),
		translateCase(),
		verifiedpermissionsCase(),
		workmailCase(),
		workspacesCase(),
	}
}

func iotwirelessCase() regionCase {
	return regionCase{
		name: "iotwireless",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := iotwireless.NewFromConfig(cfg).CreateDeviceProfile(ctx,
				&iotwireless.CreateDeviceProfileInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := iotwireless.NewFromConfig(cfg).ListDeviceProfiles(ctx, &iotwireless.ListDeviceProfilesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.DeviceProfileList, func(d iotwirelesstypes.DeviceProfile) *string { return d.Name }), nil
		},
	}
}

func kafkaconnectCase() regionCase {
	return regionCase{
		name: "kafkaconnect",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kafkaconnect.NewFromConfig(cfg).CreateWorkerConfiguration(ctx,
				&kafkaconnect.CreateWorkerConfigurationInput{
					Name: aws.String(name), PropertiesFileContent: aws.String("a2V5LmNvbnZlcnRlcj14"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kafkaconnect.NewFromConfig(cfg).ListWorkerConfigurations(ctx,
				&kafkaconnect.ListWorkerConfigurationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.WorkerConfigurations,
				func(w kafkaconnecttypes.WorkerConfigurationSummary) *string { return w.Name }), nil
		},
	}
}

func kinesisanalyticsCase() regionCase {
	return regionCase{
		name: "kinesisanalytics",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kinesisanalytics.NewFromConfig(cfg).CreateApplication(ctx,
				&kinesisanalytics.CreateApplicationInput{ApplicationName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kinesisanalytics.NewFromConfig(cfg).
				ListApplications(ctx, &kinesisanalytics.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.ApplicationSummaries))
			for _, a := range out.ApplicationSummaries {
				names = append(names, aws.ToString(a.ApplicationName))
			}

			return names, nil
		},
	}
}

func kinesisvideoCase() regionCase {
	return regionCase{
		name: "kinesisvideo",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := kinesisvideo.NewFromConfig(cfg).CreateStream(ctx,
				&kinesisvideo.CreateStreamInput{StreamName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := kinesisvideo.NewFromConfig(cfg).ListStreams(ctx, &kinesisvideo.ListStreamsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.StreamInfoList))
			for _, s := range out.StreamInfoList {
				names = append(names, aws.ToString(s.StreamName))
			}

			return names, nil
		},
	}
}

func lakeformationCase() regionCase {
	return regionCase{
		name: "lakeformation",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := lakeformation.NewFromConfig(cfg).CreateLFTag(ctx,
				&lakeformation.CreateLFTagInput{TagKey: aws.String(name), TagValues: []string{"v"}})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := lakeformation.NewFromConfig(cfg).ListLFTags(ctx, &lakeformation.ListLFTagsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.LFTags))
			for _, t := range out.LFTags {
				names = append(names, aws.ToString(t.TagKey))
			}

			return names, nil
		},
	}
}

func macie2Case() regionCase {
	return regionCase{
		name: "macie2",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := macie2.NewFromConfig(cfg).CreateAllowList(ctx, &macie2.CreateAllowListInput{
				Name: aws.String(name), ClientToken: aws.String(name),
				Criteria: &macie2types.AllowListCriteria{Regex: aws.String("a+")},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := macie2.NewFromConfig(cfg).ListAllowLists(ctx, &macie2.ListAllowListsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.AllowLists, func(a macie2types.AllowListSummary) *string { return a.Name }), nil
		},
	}
}

func mediaconvertCase() regionCase {
	return regionCase{
		name: "mediaconvert",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mediaconvert.NewFromConfig(cfg).
				CreateQueue(ctx, &mediaconvert.CreateQueueInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mediaconvert.NewFromConfig(cfg).ListQueues(ctx, &mediaconvert.ListQueuesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Queues))
			for _, q := range out.Queues {
				names = append(names, aws.ToString(q.Name))
			}

			return names, nil
		},
	}
}

func medialiveCase() regionCase {
	return regionCase{
		name: "medialive",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := medialive.NewFromConfig(cfg).CreateCloudWatchAlarmTemplateGroup(ctx,
				&medialive.CreateCloudWatchAlarmTemplateGroupInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := medialive.NewFromConfig(cfg).ListCloudWatchAlarmTemplateGroups(ctx,
				&medialive.ListCloudWatchAlarmTemplateGroupsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.CloudWatchAlarmTemplateGroups))
			for _, g := range out.CloudWatchAlarmTemplateGroups {
				names = append(names, aws.ToString(g.Name))
			}

			return names, nil
		},
	}
}

func mediapackageCase() regionCase {
	return regionCase{
		name: "mediapackage",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mediapackage.NewFromConfig(cfg).
				CreateChannel(ctx, &mediapackage.CreateChannelInput{Id: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mediapackage.NewFromConfig(cfg).ListChannels(ctx, &mediapackage.ListChannelsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Channels))
			for _, c := range out.Channels {
				names = append(names, aws.ToString(c.Id))
			}

			return names, nil
		},
	}
}

func mediastoreCase() regionCase {
	return regionCase{
		name: "mediastore",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mediastore.NewFromConfig(cfg).CreateContainer(ctx,
				&mediastore.CreateContainerInput{ContainerName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mediastore.NewFromConfig(cfg).ListContainers(ctx, &mediastore.ListContainersInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Containers))
			for _, c := range out.Containers {
				names = append(names, aws.ToString(c.Name))
			}

			return names, nil
		},
	}
}

func mediatailorCase() regionCase {
	return regionCase{
		name: "mediatailor",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mediatailor.NewFromConfig(cfg).CreateSourceLocation(ctx, &mediatailor.CreateSourceLocationInput{
				SourceLocationName: aws.String(name),
				HttpConfiguration:  &mediatailortypes.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mediatailor.NewFromConfig(cfg).ListSourceLocations(ctx, &mediatailor.ListSourceLocationsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Items, func(s mediatailortypes.SourceLocation) *string { return s.SourceLocationName }), nil
		},
	}
}

func mgnCase() regionCase {
	return regionCase{
		name: "mgn",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			c := mgn.NewFromConfig(cfg)
			if _, err := c.InitializeService(ctx, &mgn.InitializeServiceInput{}); err != nil {
				return err
			}

			_, err := c.CreateApplication(ctx, &mgn.CreateApplicationInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mgn.NewFromConfig(cfg).ListApplications(ctx, &mgn.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Items))
			for _, a := range out.Items {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

// networkmanagerCase: global service homed in us-west-2 (AWS docs: "Network Manager is a global service").
func networkmanagerCase() regionCase {
	return regionCase{
		name: "networkmanager", global: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := networkmanager.NewFromConfig(cfg).CreateGlobalNetwork(ctx,
				&networkmanager.CreateGlobalNetworkInput{Description: aws.String(name)})

			return err
		},
	}
}

func networkmonitorCase() regionCase {
	return regionCase{
		name: "networkmonitor",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := networkmonitor.NewFromConfig(cfg).CreateMonitor(ctx,
				&networkmonitor.CreateMonitorInput{MonitorName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := networkmonitor.NewFromConfig(cfg).ListMonitors(ctx, &networkmonitor.ListMonitorsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Monitors, func(m networkmonitortypes.MonitorSummary) *string { return m.MonitorName }), nil
		},
	}
}

func omicsCase() regionCase {
	return regionCase{
		name: "omics",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := omics.NewFromConfig(cfg).
				CreateSequenceStore(ctx, &omics.CreateSequenceStoreInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := omics.NewFromConfig(cfg).ListSequenceStores(ctx, &omics.ListSequenceStoresInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.SequenceStores))
			for _, s := range out.SequenceStores {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func opsworksCase() regionCase {
	return regionCase{
		name: "opsworks",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := opsworks.NewFromConfig(cfg).CreateStack(ctx, &opsworks.CreateStackInput{
				Name: aws.String(name), Region: aws.String(cfg.Region),
				ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/svc"),
				DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/p"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := opsworks.NewFromConfig(cfg).DescribeStacks(ctx, &opsworks.DescribeStacksInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Stacks))
			for _, s := range out.Stacks {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func outpostsCase() regionCase {
	return regionCase{
		name: "outposts",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := outposts.NewFromConfig(cfg).CreateSite(ctx, &outposts.CreateSiteInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := outposts.NewFromConfig(cfg).ListSites(ctx, &outposts.ListSitesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Sites))
			for _, s := range out.Sites {
				names = append(names, aws.ToString(s.Name))
			}

			return names, nil
		},
	}
}

func personalizeCase() regionCase {
	return regionCase{
		name: "personalize",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := personalize.NewFromConfig(cfg).CreateDatasetGroup(ctx,
				&personalize.CreateDatasetGroupInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := personalize.NewFromConfig(cfg).ListDatasetGroups(ctx, &personalize.ListDatasetGroupsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.DatasetGroups))
			for _, g := range out.DatasetGroups {
				names = append(names, aws.ToString(g.Name))
			}

			return names, nil
		},
	}
}

func pinpointCase() regionCase {
	return regionCase{
		name: "pinpoint",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := pinpoint.NewFromConfig(cfg).CreateApp(ctx, &pinpoint.CreateAppInput{
				CreateApplicationRequest: &pinpointtypes.CreateApplicationRequest{Name: aws.String(name)},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := pinpoint.NewFromConfig(cfg).GetApps(ctx, &pinpoint.GetAppsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.ApplicationsResponse.Item,
				func(a pinpointtypes.ApplicationResponse) *string { return a.Name },
			), nil
		},
	}
}

func pollyCase() regionCase {
	return regionCase{
		name: "polly",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := polly.NewFromConfig(cfg).PutLexicon(ctx, &polly.PutLexiconInput{
				Name: aws.String(strings.ReplaceAll(name, "-", "")), Content: aws.String(pollyLexicon),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := polly.NewFromConfig(cfg).ListLexicons(ctx, &polly.ListLexiconsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Lexicons))
			for _, l := range out.Lexicons {
				names = append(names, rehyphenate(aws.ToString(l.Name)))
			}

			return names, nil
		},
	}
}

const pollyLexicon = `<?xml version="1.0" encoding="UTF-8"?>` +
	`<lexicon version="1.0" xmlns="http://www.w3.org/2005/01/pronunciation-lexicon" alphabet="ipa" xml:lang="en-US">` +
	`<lexeme><grapheme>W3C</grapheme><alias>World Wide Web Consortium</alias></lexeme></lexicon>`

// rehyphenate undoes the hyphen stripping done for names that must be alphanumeric.
func rehyphenate(n string) string {
	switch {
	case strings.HasPrefix(n, "rgboth"):
		return "rg-both-" + strings.TrimPrefix(n, "rgboth")
	case strings.HasPrefix(n, "rga"):
		return "rg-a-" + strings.TrimPrefix(n, "rga")
	case strings.HasPrefix(n, "rgb"):
		return "rg-b-" + strings.TrimPrefix(n, "rgb")
	}

	return n
}

func quicksightCase() regionCase {
	return regionCase{
		name: "quicksight",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := quicksight.NewFromConfig(cfg).CreateGroup(ctx, &quicksight.CreateGroupInput{
				AwsAccountId: aws.String("000000000000"), Namespace: aws.String("default"), GroupName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := quicksight.NewFromConfig(cfg).ListGroups(ctx, &quicksight.ListGroupsInput{
				AwsAccountId: aws.String("000000000000"), Namespace: aws.String("default"),
			})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.GroupList))
			for _, g := range out.GroupList {
				names = append(names, aws.ToString(g.GroupName))
			}

			return names, nil
		},
	}
}

func ramCase() regionCase {
	return regionCase{
		name: "ram",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ram.NewFromConfig(cfg).
				CreateResourceShare(ctx, &ram.CreateResourceShareInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ram.NewFromConfig(cfg).GetResourceShares(ctx,
				&ram.GetResourceSharesInput{ResourceOwner: ramtypes.ResourceOwnerSelf})
			if err != nil {
				return nil, err
			}

			return strs(out.ResourceShares, func(r ramtypes.ResourceShare) *string { return r.Name }), nil
		},
	}
}

func rekognitionCase() regionCase {
	return regionCase{
		name: "rekognition",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := rekognition.NewFromConfig(cfg).CreateCollection(ctx,
				&rekognition.CreateCollectionInput{CollectionId: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := rekognition.NewFromConfig(cfg).ListCollections(ctx, &rekognition.ListCollectionsInput{})
			if err != nil {
				return nil, err
			}

			return out.CollectionIds, nil
		},
	}
}

func resiliencehubCase() regionCase {
	return regionCase{
		name: "resiliencehub",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := resiliencehub.NewFromConfig(cfg).
				CreateApp(ctx, &resiliencehub.CreateAppInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := resiliencehub.NewFromConfig(cfg).ListApps(ctx, &resiliencehub.ListAppsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.AppSummaries))
			for _, a := range out.AppSummaries {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func resourcegroupsCase() regionCase {
	return regionCase{
		name: "resourcegroups",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := resourcegroups.NewFromConfig(cfg).CreateGroup(ctx, &resourcegroups.CreateGroupInput{
				Name: aws.String(name),
				ResourceQuery: &resourcegroupstypes.ResourceQuery{
					Type:  resourcegroupstypes.QueryTypeTagFilters10,
					Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"k"}]}`),
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := resourcegroups.NewFromConfig(cfg).ListGroups(ctx, &resourcegroups.ListGroupsInput{})
			if err != nil {
				return nil, err
			}

			return strs(
				out.GroupIdentifiers,
				func(g resourcegroupstypes.GroupIdentifier) *string { return g.GroupName },
			), nil
		},
	}
}

func rolesanywhereCase() regionCase {
	return regionCase{
		name: "rolesanywhere",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := rolesanywhere.NewFromConfig(cfg).CreateProfile(ctx, &rolesanywhere.CreateProfileInput{
				Name: aws.String(name), RoleArns: []string{"arn:aws:iam::000000000000:role/r"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := rolesanywhere.NewFromConfig(cfg).ListProfiles(ctx, &rolesanywhere.ListProfilesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Profiles))
			for _, p := range out.Profiles {
				names = append(names, aws.ToString(p.Name))
			}

			return names, nil
		},
	}
}

func s3tablesCase() regionCase {
	return regionCase{
		name: "s3tables",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := s3tables.NewFromConfig(cfg).
				CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := s3tables.NewFromConfig(cfg).ListTableBuckets(ctx, &s3tables.ListTableBucketsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.TableBuckets))
			for _, b := range out.TableBuckets {
				names = append(names, aws.ToString(b.Name))
			}

			return names, nil
		},
	}
}

func serverlessrepoCase() regionCase {
	return regionCase{
		name: "serverlessrepo",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := serverlessapplicationrepository.NewFromConfig(cfg).CreateApplication(ctx,
				&serverlessapplicationrepository.CreateApplicationInput{
					Name: aws.String(name), Author: aws.String("a"), Description: aws.String("d"),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := serverlessapplicationrepository.NewFromConfig(cfg).ListApplications(ctx,
				&serverlessapplicationrepository.ListApplicationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Applications))
			for _, a := range out.Applications {
				names = append(names, aws.ToString(a.Name))
			}

			return names, nil
		},
	}
}

func ssoadminCase() regionCase {
	return regionCase{
		name: "ssoadmin",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := ssoadmin.NewFromConfig(cfg).
				CreateInstance(ctx, &ssoadmin.CreateInstanceInput{Name: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := ssoadmin.NewFromConfig(cfg).ListInstances(ctx, &ssoadmin.ListInstancesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Instances))
			for _, i := range out.Instances {
				names = append(names, aws.ToString(i.Name))
			}

			return names, nil
		},
	}
}

func swfCase() regionCase {
	return regionCase{
		name: "swf",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := swf.NewFromConfig(cfg).RegisterDomain(ctx, &swf.RegisterDomainInput{
				Name: aws.String(name), WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := swf.NewFromConfig(cfg).ListDomains(ctx,
				&swf.ListDomainsInput{RegistrationStatus: swftypes.RegistrationStatusRegistered})
			if err != nil {
				return nil, err
			}

			return strs(out.DomainInfos, func(d swftypes.DomainInfo) *string { return d.Name }), nil
		},
	}
}

func textractCase() regionCase {
	return regionCase{
		name: "textract",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := textract.NewFromConfig(cfg).CreateAdapter(ctx, &textract.CreateAdapterInput{
				AdapterName: aws.String(name), FeatureTypes: []texttypes.FeatureType{texttypes.FeatureTypeForms},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := textract.NewFromConfig(cfg).ListAdapters(ctx, &textract.ListAdaptersInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Adapters, func(a texttypes.AdapterOverview) *string { return a.AdapterName }), nil
		},
	}
}

func timestreamwriteCase() regionCase {
	return regionCase{
		name: "timestreamwrite",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := timestreamwrite.NewFromConfig(cfg).CreateDatabase(ctx,
				&timestreamwrite.CreateDatabaseInput{DatabaseName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := timestreamwrite.NewFromConfig(cfg).ListDatabases(ctx, &timestreamwrite.ListDatabasesInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Databases))
			for _, d := range out.Databases {
				names = append(names, aws.ToString(d.DatabaseName))
			}

			return names, nil
		},
	}
}

func transcribeCase() regionCase {
	return regionCase{
		name: "transcribe",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := transcribe.NewFromConfig(cfg).CreateVocabularyFilter(ctx, &transcribe.CreateVocabularyFilterInput{
				VocabularyFilterName: aws.String(name), LanguageCode: transcribetypes.LanguageCodeEnUs,
				Words: []string{"badword"},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := transcribe.NewFromConfig(cfg).
				ListVocabularyFilters(ctx, &transcribe.ListVocabularyFiltersInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.VocabularyFilters, func(v transcribetypes.VocabularyFilterInfo) *string {
				return v.VocabularyFilterName
			}), nil
		},
	}
}

func translateCase() regionCase {
	return regionCase{
		name: "translate",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := translate.NewFromConfig(cfg).ImportTerminology(ctx, &translate.ImportTerminologyInput{
				Name: aws.String(name), MergeStrategy: translatetypes.MergeStrategyOverwrite,
				TerminologyData: &translatetypes.TerminologyData{
					File: []byte("en,fr\nhello,bonjour\n"), Format: translatetypes.TerminologyDataFormatCsv,
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := translate.NewFromConfig(cfg).ListTerminologies(ctx, &translate.ListTerminologiesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.TerminologyPropertiesList,
				func(t translatetypes.TerminologyProperties) *string { return t.Name }), nil
		},
	}
}

func verifiedpermissionsCase() regionCase {
	return regionCase{
		name: "verifiedpermissions",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := verifiedpermissions.NewFromConfig(cfg).
				CreatePolicyStore(ctx, &verifiedpermissions.CreatePolicyStoreInput{
					ValidationSettings: &vptypes.ValidationSettings{
						Mode: vptypes.ValidationModeOff,
					},
					Description: aws.String(name),
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := verifiedpermissions.NewFromConfig(cfg).
				ListPolicyStores(ctx, &verifiedpermissions.ListPolicyStoresInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.PolicyStores, func(p vptypes.PolicyStoreItem) *string { return p.Description }), nil
		},
	}
}

func workmailCase() regionCase {
	return regionCase{
		name: "workmail", uniqueNames: true,
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := workmail.NewFromConfig(cfg).
				CreateOrganization(ctx, &workmail.CreateOrganizationInput{Alias: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := workmail.NewFromConfig(cfg).ListOrganizations(ctx, &workmail.ListOrganizationsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.OrganizationSummaries))
			for _, o := range out.OrganizationSummaries {
				names = append(names, aws.ToString(o.Alias))
			}

			return names, nil
		},
	}
}

func workspacesCase() regionCase {
	return regionCase{
		name: "workspaces",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := workspaces.NewFromConfig(cfg).
				CreateIpGroup(ctx, &workspaces.CreateIpGroupInput{GroupName: aws.String(name)})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := workspaces.NewFromConfig(cfg).DescribeIpGroups(ctx, &workspaces.DescribeIpGroupsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Result))
			for _, g := range out.Result {
				names = append(names, aws.ToString(g.GroupName))
			}

			return names, nil
		},
	}
}

func managedblockchainCase() regionCase {
	return regionCase{
		name: "managedblockchain",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := managedblockchain.NewFromConfig(cfg).CreateNetwork(ctx, &managedblockchain.CreateNetworkInput{
				Name: aws.String(name), Framework: mbtypes.FrameworkHyperledgerFabric,
				FrameworkVersion: aws.String("2.2"), ClientRequestToken: aws.String(name),
				FrameworkConfiguration: &mbtypes.NetworkFrameworkConfiguration{
					Fabric: &mbtypes.NetworkFabricConfiguration{Edition: mbtypes.EditionStarter},
				},
				VotingPolicy: &mbtypes.VotingPolicy{ApprovalThresholdPolicy: &mbtypes.ApprovalThresholdPolicy{
					ThresholdPercentage:     aws.Int32(50),
					ProposalDurationInHours: aws.Int32(24),
					ThresholdComparator:     mbtypes.ThresholdComparatorGreaterThan,
				}},
				MemberConfiguration: &mbtypes.MemberConfiguration{
					Name: aws.String("m" + strings.ReplaceAll(name, "-", "")),
					FrameworkConfiguration: &mbtypes.MemberFrameworkConfiguration{
						Fabric: &mbtypes.MemberFabricConfiguration{
							AdminUsername: aws.String("admin"), AdminPassword: aws.String("Password1"),
						},
					},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := managedblockchain.NewFromConfig(cfg).ListNetworks(ctx, &managedblockchain.ListNetworksInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Networks, func(n mbtypes.NetworkSummary) *string { return n.Name }), nil
		},
	}
}

func mwaaCase() regionCase {
	return regionCase{
		name: "mwaa",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mwaa.NewFromConfig(cfg).CreateEnvironment(ctx, &mwaa.CreateEnvironmentInput{
				Name: aws.String(name), DagS3Path: aws.String("dags"),
				ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				SourceBucketArn:  aws.String("arn:aws:s3:::bucket"),
				NetworkConfiguration: &mwaatypes.NetworkConfiguration{
					SecurityGroupIds: []string{"sg-1"}, SubnetIds: []string{"subnet-1", "subnet-2"},
				},
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mwaa.NewFromConfig(cfg).ListEnvironments(ctx, &mwaa.ListEnvironmentsInput{})
			if err != nil {
				return nil, err
			}

			return out.Environments, nil
		},
	}
}

func mediastoredataCase() regionCase {
	return regionCase{
		name: "mediastoredata",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := mediastoredata.NewFromConfig(cfg).PutObject(ctx, &mediastoredata.PutObjectInput{
				Path: aws.String(name), Body: bytes.NewReader([]byte("x")),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := mediastoredata.NewFromConfig(cfg).ListItems(ctx, &mediastoredata.ListItemsInput{})
			if err != nil {
				return nil, err
			}

			names := make([]string, 0, len(out.Items))
			for _, i := range out.Items {
				names = append(names, aws.ToString(i.Name))
			}

			return names, nil
		},
	}
}

func redshiftdataCase() regionCase {
	return regionCase{
		name: "redshiftdata",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := redshiftdata.NewFromConfig(cfg).ExecuteStatement(ctx, &redshiftdata.ExecuteStatementInput{
				Sql: aws.String("select 1"), ClusterIdentifier: aws.String("c"), Database: aws.String("d"),
				DbUser: aws.String("u"), StatementName: aws.String(name),
			})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := redshiftdata.NewFromConfig(cfg).ListStatements(ctx, &redshiftdata.ListStatementsInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.Statements, func(s redshiftdatatypes.StatementData) *string { return s.StatementName }), nil
		},
	}
}

func timestreamqueryCase() regionCase {
	return regionCase{
		name: "timestreamquery",
		create: func(ctx context.Context, cfg aws.Config, name string) error {
			_, err := timestreamquery.NewFromConfig(cfg).
				CreateScheduledQuery(ctx, &timestreamquery.CreateScheduledQueryInput{
					Name:        aws.String(name),
					QueryString: aws.String("select 1"),
					ScheduleConfiguration: &tsqtypes.ScheduleConfiguration{
						ScheduleExpression: aws.String("rate(1 hour)"),
					},
					NotificationConfiguration: &tsqtypes.NotificationConfiguration{
						SnsConfiguration: &tsqtypes.SnsConfiguration{
							TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
						},
					},
					ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
					ErrorReportConfiguration: &tsqtypes.ErrorReportConfiguration{
						S3Configuration: &tsqtypes.S3Configuration{BucketName: aws.String("bucket")},
					},
					TargetConfiguration: &tsqtypes.TargetConfiguration{
						TimestreamConfiguration: &tsqtypes.TimestreamConfiguration{
							DatabaseName: aws.String("db"),
							TableName:    aws.String("t"),
							TimeColumn:   aws.String("time"),
							DimensionMappings: []tsqtypes.DimensionMapping{
								{Name: aws.String("d"), DimensionValueType: tsqtypes.DimensionValueTypeVarchar},
							},
							MultiMeasureMappings: &tsqtypes.MultiMeasureMappings{
								MultiMeasureAttributeMappings: []tsqtypes.MultiMeasureAttributeMapping{
									{
										SourceColumn:     aws.String("m"),
										MeasureValueType: tsqtypes.ScalarMeasureValueTypeDouble,
									},
								},
							},
						},
					},
				})

			return err
		},
		list: func(ctx context.Context, cfg aws.Config) ([]string, error) {
			out, err := timestreamquery.NewFromConfig(cfg).
				ListScheduledQueries(ctx, &timestreamquery.ListScheduledQueriesInput{})
			if err != nil {
				return nil, err
			}

			return strs(out.ScheduledQueries, func(q tsqtypes.ScheduledQuery) *string { return q.Name }), nil
		},
	}
}
