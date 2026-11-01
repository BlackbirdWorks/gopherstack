package ocidatabase

import (
	"fmt"
	"strconv"
	"strings"
)

// Consumer groups (SDK DatabaseConnectionStringProfileConsumerGroupEnum) and the services the stub exposes.
const (
	stubMtlsPort  = 1522
	stubTLSPort   = 1521
	engineHost    = "localhost"
	engineService = "FREEPDB1"
	retryCount    = 20
	retryDelay    = 3
)

var (
	groupsTransactional = []string{"HIGH", "MEDIUM", "LOW", "TP", "TPURGENT"} //nolint:gochecknoglobals // immutable
	groupsAnalytic      = []string{"HIGH", "MEDIUM", "LOW"}                   //nolint:gochecknoglobals // immutable
)

type profileWire struct {
	DisplayName       string `json:"displayName"`
	Value             string `json:"value"`
	Protocol          string `json:"protocol"`
	HostFormat        string `json:"hostFormat"`
	SessionMode       string `json:"sessionMode"`
	SyntaxFormat      string `json:"syntaxFormat"`
	ConsumerGroup     string `json:"consumerGroup"`
	TLSAuthentication string `json:"tlsAuthentication"`
}

type connStringsWire struct {
	AllConnectionStrings map[string]string `json:"allConnectionStrings"`
	High                 string            `json:"high,omitempty"`
	Medium               string            `json:"medium,omitempty"`
	Low                  string            `json:"low,omitempty"`
	Profiles             []profileWire     `json:"profiles"`
}

type connURLWire struct {
	SQLDevWebURL                     string `json:"sqlDevWebUrl,omitempty"`
	ApexURL                          string `json:"apexUrl,omitempty"`
	MachineLearningUserManagementURL string `json:"machineLearningUserManagementUrl,omitempty"`
	GraphStudioURL                   string `json:"graphStudioUrl,omitempty"`
	MongoDBURL                       string `json:"mongoDbUrl,omitempty"`
	MachineLearningNotebookURL       string `json:"machineLearningNotebookUrl,omitempty"`
	OrdsURL                          string `json:"ordsUrl,omitempty"`
	DatabaseTransformsURL            string `json:"databaseTransformsUrl,omitempty"`
}

// endpoint is where clients reach a database: the stub's documented-shape host or the engine's TLS listeners.
type endpoint struct {
	host     string
	service  string
	mtlsPort int
	tlsPort  int
	engine   bool
}

func groupsFor(workload string) []string {
	if workload == workloadDW {
		return groupsAnalytic
	}

	return groupsTransactional
}

func (e endpoint) serviceName(db *ADB, group string) string {
	if e.engine {
		return e.service
	}

	return fmt.Sprintf("%s_%s_%s.adb.oraclecloud.com", db.UniqueID, strings.ToLower(db.DBName), strings.ToLower(group))
}

func (e endpoint) descriptor(port int, service string) string {
	return fmt.Sprintf("(description= (retry_count=%d)(retry_delay=%d)(address=(protocol=tcps)(port=%d)(host=%s))"+
		"(connect_data=(service_name=%s))(security=(ssl_server_dn_match=yes)))",
		retryCount, retryDelay, port, e.host, service)
}

func (e endpoint) profile(db *ADB, group string, mutual bool) profileWire {
	port, auth, name := e.tlsPort, "SERVER", strings.ToLower(db.DBName)+"_"+strings.ToLower(group)+"_tls"
	if mutual {
		port, auth, name = e.mtlsPort, "MUTUAL", strings.ToLower(db.DBName)+"_"+strings.ToLower(group)
	}

	return profileWire{
		DisplayName: name, Value: e.descriptor(port, e.serviceName(db, group)), Protocol: "TCPS",
		HostFormat: "FQDN", SessionMode: "DIRECT", SyntaxFormat: "LONG", ConsumerGroup: group,
		TLSAuthentication: auth,
	}
}

// connectionStrings builds the profiles for db; the mutual-TLS ones always, the TLS ones when mTLS is optional.
func (e endpoint) connectionStrings(db *ADB) *connStringsWire {
	out := &connStringsWire{AllConnectionStrings: map[string]string{}, Profiles: []profileWire{}}

	for _, g := range groupsFor(db.Workload) {
		if !db.Mtls {
			out.Profiles = append(out.Profiles, e.profile(db, g, false))
		}

		out.Profiles = append(out.Profiles, e.profile(db, g, true))

		port := e.mtlsPort
		if !db.Mtls {
			port = e.tlsPort
		}

		out.AllConnectionStrings[g] = e.host + ":" + strconv.Itoa(port) + "/" + e.serviceName(db, g)
	}

	out.High = e.descriptor(e.mtlsPort, e.serviceName(db, "HIGH"))
	out.Medium = e.descriptor(e.mtlsPort, e.serviceName(db, "MEDIUM"))
	out.Low = e.descriptor(e.mtlsPort, e.serviceName(db, "LOW"))

	return out
}

func connectionURLs(db *ADB) *connURLWire {
	host := strings.ToUpper(
		db.UniqueID,
	) + "-" + strings.ToUpper(
		db.DBName,
	) + ".adb." + regionName + ".oraclecloudapps.com"
	base := "https://" + host

	u := &connURLWire{
		SQLDevWebURL:                     base + "/ords/sql-developer",
		ApexURL:                          base + "/ords/apex",
		MachineLearningUserManagementURL: base + "/omlusers/",
		GraphStudioURL:                   base + "/graphstudio/",
		MachineLearningNotebookURL:       base + "/oml",
		OrdsURL:                          base + "/ords/",
		DatabaseTransformsURL:            base + "/odi/",
	}

	if db.Workload == workloadAJD {
		u.MongoDBURL = "mongodb://" + strings.ToLower(host) + ":27017/"
	}

	return u
}

// tnsnames renders the wallet's tnsnames.ora.
func (e endpoint) tnsnames(db *ADB) string {
	var sb strings.Builder

	for _, g := range groupsFor(db.Workload) {
		p := e.profile(db, g, true)
		sb.WriteString(p.DisplayName + " = " + p.Value + "\n\n")

		if !db.Mtls {
			t := e.profile(db, g, false)
			sb.WriteString(t.DisplayName + " = " + t.Value + "\n\n")
		}
	}

	return sb.String()
}
