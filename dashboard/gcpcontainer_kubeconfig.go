package dashboard

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const gkeVerbKubeconfig = "kubeconfig"

// kubeconfig renders a kubeconfig for one cluster. It carries the cluster-admin token on docker-engine clusters, so
// it is only reachable through this POST verb, which the REST proxy answers with Cache-Control: no-store.
func (a *gcpContainerAPI) kubeconfig(r gcpRESTCall) (proto.Message, error) {
	c, err := a.b.GetCluster(&containerpb.GetClusterRequest{Name: r.name(gkeItem)})
	if err != nil {
		return nil, err
	}

	if c.GetEndpoint() == "" {
		return nil, status.Error(codes.FailedPrecondition, "cluster has no endpoint yet")
	}

	token := a.b.EngineToken()
	name := fmt.Sprintf("gke_%s_%s_%s", r.segs[1], r.segs[gkeLocationsIdx+1], c.GetName())

	var user string

	if token != "" {
		user = "    token: " + strconv.Quote(token) + "\n"
	} else {
		user = "    exec:\n      apiVersion: client.authentication.k8s.io/v1beta1\n" +
			"      command: gke-gcloud-auth-plugin\n      provideClusterInfo: true\n" +
			"      installHint: Install gke-gcloud-auth-plugin to authenticate against this cluster\n"
	}

	var sb strings.Builder

	sb.WriteString("apiVersion: v1\nkind: Config\ncurrent-context: " + strconv.Quote(name) + "\n")
	sb.WriteString("clusters:\n- name: " + strconv.Quote(name) + "\n  cluster:\n")
	sb.WriteString("    server: " + strconv.Quote("https://"+c.GetEndpoint()) + "\n")
	sb.WriteString("    certificate-authority-data: " +
		strconv.Quote(c.GetMasterAuth().GetClusterCaCertificate()) + "\n")
	sb.WriteString("contexts:\n- name: " + strconv.Quote(name) + "\n  context:\n    cluster: " +
		strconv.Quote(name) + "\n    user: " + strconv.Quote(name) + "\n")
	sb.WriteString("users:\n- name: " + strconv.Quote(name) + "\n  user:\n" + user)

	return structpb.NewStruct(map[string]any{"kubeconfig": sb.String(), "hasToken": token != ""})
}
