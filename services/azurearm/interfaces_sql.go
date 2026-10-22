package azurearm

// SQLSpec is the part of a logical server's configuration the SQL Server engine acts on. An empty AdminPassword
// keeps the current one.
type SQLSpec struct {
	AdminLogin    string
	AdminPassword string
}

// SQLEndpoint is where a server's TDS port listens; Port is 0 while the server is state-only.
type SQLEndpoint struct {
	Host string
	Port int
}

// SQL is the seam the Microsoft.Sql resource provider uses to run real SQL Server engines for its logical
// servers. id is the lower-cased ARM resource ID of the server. Primitive types only, so this package never
// imports services/azuresql.
type SQL interface {
	EnsureServer(id string, spec SQLSpec) (SQLEndpoint, error)
	Endpoint(id string) (SQLEndpoint, bool)
	DeleteServer(id string) error
	EnsureDatabase(serverID, name, collation string) error
	DropDatabase(serverID, name string) error
}

// noopSQL leaves the provider metadata-only.
type noopSQL struct{}

func (noopSQL) EnsureServer(string, SQLSpec) (SQLEndpoint, error) { return SQLEndpoint{}, nil }
func (noopSQL) Endpoint(string) (SQLEndpoint, bool)               { return SQLEndpoint{}, false }
func (noopSQL) DeleteServer(string) error                         { return nil }
func (noopSQL) EnsureDatabase(string, string, string) error       { return nil }
func (noopSQL) DropDatabase(string, string) error                 { return nil }

var _ SQL = noopSQL{}

// SetSQL gives the registered Microsoft.Sql provider (if any) a real data-plane adapter, replacing its no-op
// default.
func (r *Registry) SetSQL(s SQL) {
	p, ok := r.ProviderNamed(namespaceMicrosoftSQL)
	if !ok {
		return
	}

	if sp, spOk := p.(*SQLProvider); spOk {
		sp.dataPlane = s
	}
}
