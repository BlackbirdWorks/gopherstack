package azuresql

// Spec is the part of a logical server's ARM configuration the engine acts on. An empty AdminLogin (Entra-only
// server) creates no SQL login; an empty AdminPassword on an existing server keeps the current one.
type Spec struct {
	AdminLogin    string `json:"adminLogin"`
	AdminPassword string `json:"adminPassword"`
}

// Endpoint is where a server's TDS port listens. Port is 0 when the server is state-only.
type Endpoint struct {
	Host string
	Port int
}

// Database is a database the backend has created on a server.
type Database struct {
	Name      string `json:"name"`
	Collation string `json:"collation,omitempty"`
}
