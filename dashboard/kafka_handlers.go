package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
)

// kafkaClusterView is the view model for a single MSK cluster.
type kafkaClusterView struct {
	ClusterArn   string
	ClusterName  string
	KafkaVersion string
	State        string
	BrokerCount  int32
}

// kafkaConfigurationView is the view model for a single MSK configuration.
type kafkaConfigurationView struct {
	Arn  string
	Name string
}

// kafkaIndexData is the template data for the Kafka dashboard page.
type kafkaIndexData struct {
	PageData

	Clusters       []kafkaClusterView
	Configurations []kafkaConfigurationView
}

// setupKafkaRoutes registers all Kafka dashboard routes.
func (h *DashboardHandler) setupKafkaRoutes() {
	h.SubRouter.GET("/dashboard/kafka", h.kafkaIndex)
	h.SubRouter.POST("/dashboard/kafka/clusters/create", h.kafkaCreateCluster)
	h.SubRouter.POST("/dashboard/kafka/clusters/delete", h.kafkaDeleteCluster)
}

// kafkaIndex renders the Kafka dashboard index page.
func (h *DashboardHandler) kafkaIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "kafka-operations",
		Title: "Using Amazon MSK",
		Cli: `aws kafka list-clusters \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MSK
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := kafka.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for MSK
import boto3

client = boto3.client('kafka', endpoint_url='http://localhost:8000')`,
	}

	if h.KafkaOps == nil {
		h.renderTemplate(w, "kafka/index.html", kafkaIndexData{
			PageData: PageData{
				Title:     "Kafka",
				ActiveTab: "kafka",
				Snippet:   snippet,
			},
			Clusters:       []kafkaClusterView{},
			Configurations: []kafkaConfigurationView{},
		})

		return nil
	}

	clusters := h.KafkaOps.Backend.ListClusters()
	clusterViews := make([]kafkaClusterView, 0, len(clusters))

	for _, cl := range clusters {
		clusterViews = append(clusterViews, kafkaClusterView{
			ClusterArn:   cl.ClusterArn,
			ClusterName:  cl.ClusterName,
			KafkaVersion: cl.KafkaVersion,
			State:        cl.State,
			BrokerCount:  cl.NumberOfBrokerNodes,
		})
	}

	configs := h.KafkaOps.Backend.ListConfigurations()
	configViews := make([]kafkaConfigurationView, 0, len(configs))

	for _, cfg := range configs {
		configViews = append(configViews, kafkaConfigurationView{
			Arn:  cfg.Arn,
			Name: cfg.Name,
		})
	}

	h.renderTemplate(w, "kafka/index.html", kafkaIndexData{
		PageData: PageData{
			Title:     "Kafka",
			ActiveTab: "kafka",
			Snippet:   snippet,
		},
		Clusters:       clusterViews,
		Configurations: configViews,
	})

	return nil
}

// kafkaCreateCluster handles POST /dashboard/kafka/clusters/create.
func (h *DashboardHandler) kafkaCreateCluster(c *echo.Context) error {
	if h.KafkaOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	kafkaVersion := c.Request().FormValue("kafka_version")
	subnetID := c.Request().FormValue("subnet_id")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if kafkaVersion == "" {
		kafkaVersion = "3.5.1"
	}

	if subnetID == "" {
		subnetID = "subnet-00000000"
	}

	_, err := h.KafkaOps.Backend.CreateCluster(
		name,
		kafkaVersion,
		1,
		kafkabackend.BrokerNodeGroupInfo{
			ClientSubnets: []string{subnetID},
			InstanceType:  "kafka.m5.large",
		},
		nil,
	)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/kafka")
}

// kafkaDeleteCluster handles POST /dashboard/kafka/clusters/delete.
func (h *DashboardHandler) kafkaDeleteCluster(c *echo.Context) error {
	if h.KafkaOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	clusterArn := c.Request().FormValue("cluster_arn")
	if clusterArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.KafkaOps.Backend.DeleteCluster(clusterArn); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/kafka")
}
