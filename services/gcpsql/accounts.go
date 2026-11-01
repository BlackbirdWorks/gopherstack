package gcpsql

import (
	"context"
	"sort"
	"strings"
)

const (
	maxNameBytes   = 63
	maxUserBytes   = 63
	typeBuiltIn    = "BUILT_IN"
	defaultMyHost  = "%"
	pgDefaultColl  = "en_US.UTF8"
	myDefaultChars = "utf8mb4"
	myDefaultColl  = "utf8mb4_0900_ai_ci"
)

func mysqlSystemDBs() []string {
	return []string{"information_schema", "mysql", "performance_schema", "sys"}
}

func isPG(version string) bool { return strings.HasPrefix(version, "POSTGRES") }

func dbLink(project, instance, name string) string {
	return instanceLink(project, instance) + "/databases/" + name
}

func newDatabase(rec *instRec, name, charset, collation string, system bool) *Database {
	in := &rec.Inst

	if charset == "" {
		charset, collation = defaultCharset(in.DatabaseVersion, collation)
	}

	d := &Database{
		Kind: kindDatabase, Name: name, Instance: in.Name, Project: in.Project, Charset: charset,
		Collation: collation, SelfLink: dbLink(in.Project, in.Name, name), System: system,
	}
	d.Etag = etagOf(d)

	return d
}

func defaultCharset(version, collation string) (string, string) {
	if isPG(version) {
		return "UTF8", orDefault(collation, pgDefaultColl)
	}

	return myDefaultChars, orDefault(collation, myDefaultColl)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

func seedAccounts(rec *instRec) {
	if isPG(rec.Inst.DatabaseVersion) {
		rec.Databases[pgAdminUser] = newDatabase(rec, pgAdminUser, "", "", true)
		rec.Users[userKey(pgAdminUser, "")] = newUser(rec, pgAdminUser, "", typeBuiltIn)

		return
	}

	for _, n := range mysqlSystemDBs() {
		rec.Databases[n] = newDatabase(rec, n, "", "", true)
	}

	rec.Users[userKey(mysqlAdminUser, defaultMyHost)] = newUser(rec, mysqlAdminUser, defaultMyHost, typeBuiltIn)
}

func userKey(name, host string) string { return name + "@" + host }

func newUser(rec *instRec, name, host, typ string) *userRec {
	u := &userRec{User: User{
		Kind: kindUser, Name: name, Host: host, Instance: rec.Inst.Name, Project: rec.Inst.Project, Type: typ,
	}}
	u.User.Etag = etagOf(u.User)

	return u
}

// DatabaseInput is the writable part of sql#database.
type DatabaseInput struct {
	Name      string `json:"name"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
}

func validDatabaseName(name string) bool {
	return nameRe.MatchString(name) && len(name) <= maxNameBytes
}

// revert undoes a request-time record change when its engine task failed.
func (b *InMemoryBackend) revert(fn func()) {
	b.mu.Lock("revert")
	fn()
	b.mu.Unlock()
}

func (b *InMemoryBackend) opRec(project, name string) (*instRec, error) {
	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateDeleting {
		return nil, errNotRunnable()
	}

	return rec, nil
}

// writableRec is opRec for changes a read replica rejects.
func (b *InMemoryBackend) writableRec(project, name string) (*instRec, error) {
	rec, err := b.opRec(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.MasterInstanceName != "" {
		return nil, errInvalid("The instance is a read replica and cannot be changed through its own databases " +
			"and users; change the primary instead.")
	}

	return rec, nil
}

// InsertDatabase creates a database.
func (b *InMemoryBackend) InsertDatabase(project, instance string, in DatabaseInput) (*Operation, error) {
	if !validDatabaseName(in.Name) {
		return nil, errInvalid("Invalid database name: %q.", in.Name)
	}

	b.mu.Lock("InsertDatabase")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	if _, dup := rec.Databases[in.Name]; dup {
		return nil, errConflict(
			"invalid",
			"Invalid request: failed to create database %s. Detail: database %q already exists.",
			in.Name,
			in.Name,
		)
	}

	d := newDatabase(rec, in.Name, in.Charset, in.Collation, false)
	rec.Databases[in.Name] = d

	return b.submitLocked(rec, project, "CREATE_DATABASE", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil {
			return nil
		}

		if cerr := eng.CreateDatabase(ctx, d.Name, in.Charset, in.Collation); cerr != nil {
			b.revert(func() { delete(rec.Databases, d.Name) })

			return cerr
		}

		return nil
	}), nil
}

// GetDatabase returns a database.
func (b *InMemoryBackend) GetDatabase(project, instance, name string) (*Database, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	d, ok := rec.Databases[name]
	if !ok {
		return nil, errNotFound("The database instance does not exist.")
	}

	cp := *d

	return &cp, nil
}

// ListDatabases lists the databases of an instance sorted by name.
func (b *InMemoryBackend) ListDatabases(project, instance string) ([]*Database, error) {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	out := make([]*Database, 0, len(rec.Databases))

	for _, d := range rec.Databases {
		cp := *d
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// UpdateDatabase accepts a patch or update that leaves the immutable charset and collation unchanged.
func (b *InMemoryBackend) UpdateDatabase(project, instance, name string, in DatabaseInput) (*Operation, error) {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	d, ok := rec.Databases[name]
	if !ok {
		return nil, errNotFound("The database instance does not exist.")
	}

	if (in.Charset != "" && in.Charset != d.Charset) || (in.Collation != "" && in.Collation != d.Collation) {
		return nil, errInvalid("The charset and collation of an existing database cannot be changed.")
	}

	return b.submitLocked(rec, project, "UPDATE_DATABASE", opMeta{}, func(context.Context) error { return nil }), nil
}

// DeleteDatabase drops a database.
func (b *InMemoryBackend) DeleteDatabase(project, instance, name string) (*Operation, error) {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	d, ok := rec.Databases[name]
	if !ok {
		return nil, errNotFound("The database instance does not exist.")
	}

	if d.System {
		return nil, errInvalid("The system database %q cannot be deleted.", name)
	}

	delete(rec.Databases, name)

	return b.submitLocked(rec, project, "DELETE_DATABASE", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil {
			return nil
		}

		if cerr := eng.DropDatabase(ctx, name); cerr != nil {
			b.revert(func() { rec.Databases[name] = d })

			return cerr
		}

		return nil
	}), nil
}

// UserInput is the writable part of sql#user.
type UserInput struct {
	PasswordPolicy map[string]any `json:"passwordPolicy"`
	Name           string         `json:"name"`
	Host           string         `json:"host"`
	Password       string         `json:"password"`
	Type           string         `json:"type"`
	DatabaseRoles  []string       `json:"databaseRoles"`
}

func normalizeHost(version, host string) string {
	if isPG(version) {
		return ""
	}

	return orDefault(host, defaultMyHost)
}

func (rec *instRec) findUser(name, host string) *userRec {
	h := normalizeHost(rec.Inst.DatabaseVersion, host)
	if u, ok := rec.Users[userKey(name, h)]; ok {
		return u
	}

	if host != "" {
		return nil
	}

	var found *userRec

	for _, u := range rec.Users {
		if u.User.Name == name {
			if found != nil {
				return nil
			}

			found = u
		}
	}

	return found
}

// InsertUser creates a database account.
func (b *InMemoryBackend) InsertUser(project, instance string, in UserInput) (*Operation, error) {
	if in.Name == "" || len(in.Name) > maxUserBytes {
		return nil, errInvalid("Invalid user name: %q.", in.Name)
	}

	b.mu.Lock("InsertUser")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	host := normalizeHost(rec.Inst.DatabaseVersion, in.Host)
	key := userKey(in.Name, host)

	if _, dup := rec.Users[key]; dup {
		return nil, errConflict(
			"invalid",
			"Invalid request: failed to create user %s. Detail: the user already exists.",
			in.Name,
		)
	}

	u := newUser(rec, in.Name, host, orDefault(in.Type, typeBuiltIn))
	u.User.PasswordPolicy, u.User.DatabaseRoles, u.Password = in.PasswordPolicy, in.DatabaseRoles, in.Password
	rec.Users[key] = u

	spec := UserSpec{Name: in.Name, Host: host, Password: in.Password}

	return b.submitLocked(rec, project, "CREATE_USER", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil {
			return nil
		}

		if cerr := eng.UpsertUser(ctx, spec); cerr != nil {
			b.revert(func() { delete(rec.Users, key) })

			return cerr
		}

		return nil
	}), nil
}

// UpdateUser changes the password and policy of an account.
func (b *InMemoryBackend) UpdateUser(project, instance, name, host string, in UserInput) (*Operation, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	u := rec.findUser(name, host)
	if u == nil {
		return nil, errNotFound("The Cloud SQL user does not exist.")
	}

	prev := *u
	if in.Password != "" {
		u.Password = in.Password
		if isBuiltinAdmin(rec.Inst.DatabaseVersion, u.User.Name) {
			rec.RootPassword = in.Password
		}
	}

	if in.PasswordPolicy != nil {
		u.User.PasswordPolicy = in.PasswordPolicy
	}

	u.User.Etag = etagOf(u.User)
	spec := UserSpec{Name: u.User.Name, Host: u.User.Host, Password: in.Password}

	return b.submitLocked(rec, project, "UPDATE_USER", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil || in.Password == "" {
			return nil
		}

		if cerr := eng.UpsertUser(ctx, spec); cerr != nil {
			b.revert(func() { *u = prev })

			return cerr
		}

		return nil
	}), nil
}

// GetUser returns an account.
func (b *InMemoryBackend) GetUser(project, instance, name, host string) (*User, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	u := rec.findUser(name, host)
	if u == nil {
		return nil, errNotFound("The Cloud SQL user does not exist.")
	}

	cp := u.User

	return &cp, nil
}

// ListUsers lists the accounts of an instance sorted by name and host.
func (b *InMemoryBackend) ListUsers(project, instance string) ([]*User, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	out := make([]*User, 0, len(rec.Users))

	for _, u := range rec.Users {
		cp := u.User
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}

		return out[i].Host < out[j].Host
	})

	return out, nil
}

// DeleteUser drops an account.
func (b *InMemoryBackend) DeleteUser(project, instance, name, host string) (*Operation, error) {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	u := rec.findUser(name, host)
	if u == nil {
		return nil, errNotFound("The Cloud SQL user does not exist.")
	}

	key := userKey(u.User.Name, u.User.Host)
	delete(rec.Users, key)

	spec := UserSpec{Name: u.User.Name, Host: u.User.Host}
	builtin := isBuiltinAdmin(rec.Inst.DatabaseVersion, spec.Name)

	return b.submitLocked(rec, project, "DELETE_USER", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil || builtin {
			return nil
		}

		if cerr := eng.DropUser(ctx, spec.Name, spec.Host); cerr != nil {
			b.revert(func() { rec.Users[key] = u })

			return cerr
		}

		return nil
	}), nil
}
