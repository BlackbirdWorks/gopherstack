package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- HDFS ---

func (b *InMemoryBackend) CreateLocationHdfs(
	subdirectory, authenticationType, simpleUser string,
	kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
	nameNodes []HdfsNameNode,
	blockSize int64,
	replicationFactor int32,
	qopConfig *QopConfiguration,
	agentArns []string,
	tags map[string]string,
	secretConfig SecretConfig,
) (*Location, error) {
	b.mu.Lock("CreateLocationHdfs")
	defer b.mu.Unlock()

	if err := b.validateAgentArns(agentArns); err != nil {
		return nil, err
	}

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	host := "hdfs"
	if len(nameNodes) > 0 {
		host = fmt.Sprintf("%s:%d", nameNodes[0].Hostname, nameNodes[0].Port)
	}

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("hdfs://%s/%s", host, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	storedNodes := make([]storedHdfsNameNode, len(nameNodes))
	for i, n := range nameNodes {
		storedNodes[i] = storedHdfsNameNode(n)
	}

	cfg := &storedHdfsConfig{
		NameNodes:          storedNodes,
		AuthenticationType: authenticationType,
		SimpleUser:         simpleUser,
		KerberosPrincipal:  kerberosPrincipal,
		KerberosKeytab:     kerberosKeytab,
		KerberosKrb5Conf:   kerberosKrb5Conf,
		KmsKeyProviderURI:  kmsKeyProviderURI,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		AgentArns:          agentArns,
		CmkSecretConfig:    toStoredCmkSecretConfig(secretConfig.Cmk),
		CustomSecretConfig: toStoredCustomSecretConfig(secretConfig.Custom),
	}

	if qopConfig != nil {
		cfg.QopConfiguration = &storedQopConfig{
			DataTransferProtection: qopConfig.DataTransferProtection,
			RPCProtection:          qopConfig.RPCProtection,
		}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeHDFS,
		CreationTime: now,
		Tags:         locationTags,
		Hdfs:         cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationHdfs(locationArn string) (*LocationHdfs, error) {
	b.mu.RLock("DescribeLocationHdfs")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeHDFS {
		return nil, ErrNotFound
	}

	out := &LocationHdfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Hdfs != nil {
		out.AuthenticationType = l.Hdfs.AuthenticationType
		out.SimpleUser = l.Hdfs.SimpleUser
		out.KerberosPrincipal = l.Hdfs.KerberosPrincipal
		out.KmsKeyProviderURI = l.Hdfs.KmsKeyProviderURI
		out.BlockSize = l.Hdfs.BlockSize
		out.ReplicationFactor = l.Hdfs.ReplicationFactor
		out.AgentArns = l.Hdfs.AgentArns

		nodes := make([]HdfsNameNode, len(l.Hdfs.NameNodes))
		for i, n := range l.Hdfs.NameNodes {
			nodes[i] = HdfsNameNode(n)
		}

		out.NameNodes = nodes

		if l.Hdfs.QopConfiguration != nil {
			out.QopConfiguration = &QopConfiguration{
				DataTransferProtection: l.Hdfs.QopConfiguration.DataTransferProtection,
				RPCProtection:          l.Hdfs.QopConfiguration.RPCProtection,
			}
		}

		out.CmkSecretConfig = fromStoredCmkSecretConfig(l.Hdfs.CmkSecretConfig)
		out.CustomSecretConfig = fromStoredCustomSecretConfig(l.Hdfs.CustomSecretConfig)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationHdfs(
	locationArn, subdirectory, authenticationType, simpleUser string,
	kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
	nameNodes []HdfsNameNode,
	blockSize int64,
	replicationFactor int32,
	qopConfig *QopConfiguration,
	agentArns []string,
	secretConfig SecretConfig,
) error {
	b.mu.Lock("UpdateLocationHdfs")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeHDFS {
		return ErrNotFound
	}

	if err := b.validateAgentArns(agentArns); err != nil {
		return err
	}

	if l.Hdfs == nil {
		l.Hdfs = &storedHdfsConfig{}
	}

	updateHdfsSubdirectory(l, subdirectory)
	updateHdfsNameNodes(l.Hdfs, nameNodes)
	updateHdfsScalarFields(
		l.Hdfs,
		authenticationType,
		simpleUser,
		kmsKeyProviderURI,
		blockSize,
		replicationFactor,
		agentArns,
	)
	updateHdfsKerberosFields(l.Hdfs, kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf)

	if qopConfig != nil {
		l.Hdfs.QopConfiguration = &storedQopConfig{
			DataTransferProtection: qopConfig.DataTransferProtection,
			RPCProtection:          qopConfig.RPCProtection,
		}
	}

	if secretConfig.Cmk != nil {
		l.Hdfs.CmkSecretConfig = toStoredCmkSecretConfig(secretConfig.Cmk)
	}

	if secretConfig.Custom != nil {
		l.Hdfs.CustomSecretConfig = toStoredCustomSecretConfig(secretConfig.Custom)
	}

	return nil
}

func updateHdfsSubdirectory(l *storedLocation, subdirectory string) {
	if subdirectory == "" {
		return
	}

	l.Subdirectory = subdirectory
	host := "hdfs"
	if len(l.Hdfs.NameNodes) > 0 {
		host = fmt.Sprintf("%s:%d", l.Hdfs.NameNodes[0].Hostname, l.Hdfs.NameNodes[0].Port)
	}

	sub := strings.TrimPrefix(subdirectory, "/")
	l.LocationURI = fmt.Sprintf("hdfs://%s/%s", host, sub)
}

func updateHdfsNameNodes(cfg *storedHdfsConfig, nameNodes []HdfsNameNode) {
	if len(nameNodes) == 0 {
		return
	}

	storedNodes := make([]storedHdfsNameNode, len(nameNodes))
	for i, n := range nameNodes {
		storedNodes[i] = storedHdfsNameNode(n)
	}

	cfg.NameNodes = storedNodes
}

func updateHdfsScalarFields(
	cfg *storedHdfsConfig,
	authenticationType, simpleUser, kmsKeyProviderURI string,
	blockSize int64,
	replicationFactor int32,
	agentArns []string,
) {
	if authenticationType != "" {
		cfg.AuthenticationType = authenticationType
	}

	if simpleUser != "" {
		cfg.SimpleUser = simpleUser
	}

	if kmsKeyProviderURI != "" {
		cfg.KmsKeyProviderURI = kmsKeyProviderURI
	}

	if blockSize > 0 {
		cfg.BlockSize = blockSize
	}

	if replicationFactor > 0 {
		cfg.ReplicationFactor = replicationFactor
	}

	if agentArns != nil {
		cfg.AgentArns = agentArns
	}
}

func updateHdfsKerberosFields(cfg *storedHdfsConfig, kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf string) {
	if kerberosPrincipal != "" {
		cfg.KerberosPrincipal = kerberosPrincipal
	}

	if kerberosKeytab != "" {
		cfg.KerberosKeytab = kerberosKeytab
	}

	if kerberosKrb5Conf != "" {
		cfg.KerberosKrb5Conf = kerberosKrb5Conf
	}
}
