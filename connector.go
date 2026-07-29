package sqlds

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type Connector struct {
	UID            string
	cache          ConnectionCache
	driver         Driver
	driverSettings DriverSettings
	// storeKey is the base used for all connection cache keys. It is the
	// datasource UID, optionally suffixed with "-<username>" when the datasource
	// JSONData carries a username, so that users connecting with distinct
	// credentials get isolated pooled connections (vunet per-user connection).
	storeKey string
	// defaultKey is the cache key the bootstrap connection is stored under at
	// construction time: fmt.Sprintf("%s-default", storeKey) for the settings
	// NewConnector was called with. Per-request lookups do NOT use this field —
	// they re-derive the key from the current request (see baseKey) so each user
	// reaches their own connection.
	defaultKey string
	// Enabling multiple connections may cause that concurrent connection limits
	// are hit. The datasource enabling this should make sure connections are cached
	// if necessary.
	enableMultipleConnections bool
}

// ConnectorOption configures a Connector at construction time.
type ConnectorOption func(*Connector)

// WithCache installs a custom ConnectionCache on the Connector. When omitted,
// NewConnector defaults to NewSyncMapCache(). The option is applied before
// the bootstrap connection is stored, so the bootstrap entry lands in the
// custom cache.
func WithCache(cache ConnectionCache) ConnectorOption {
	return func(c *Connector) { c.cache = cache }
}

// storeKeyFor derives the connection cache base key for a datasource. It is the
// datasource UID (falling back to the numeric ID for Grafana < 8.0), suffixed
// with "-<username>" when JSONData carries a username. Keying by username gives
// each user their own pooled connection when they share a datasource but
// connect with distinct credentials.
func storeKeyFor(settings backend.DataSourceInstanceSettings) string {
	uid := settings.UID
	if uid == "" {
		uid = fmt.Sprintf("%d", settings.ID)
	}

	var dbInfo map[string]interface{}
	if err := json.Unmarshal(settings.JSONData, &dbInfo); err != nil {
		return uid
	}
	username, ok := dbInfo["username"].(string)
	if !ok {
		return uid
	}
	return fmt.Sprintf("%s-%s", uid, username)
}

// settingsFromContext returns the DataSourceInstanceSettings for the request
// being served, or nil outside a request. The Grafana SDK attaches the
// PluginContext of every inbound request to ctx (backend.MiddlewareHandler
// calls WithPluginContext on QueryData, CallResource and CheckHealth alike), so
// reading it here gives us the CURRENT caller's settings without threading an
// extra parameter through every call site.
func settingsFromContext(ctx context.Context) *backend.DataSourceInstanceSettings {
	return backend.PluginConfigFromContext(ctx).DataSourceInstanceSettings
}

// baseKey is the per-request connection cache base key. It is derived from the
// settings on ctx so that a per-user credential rewrite routes to a connection
// opened as *that* user, rather than reusing the one created at instance init.
// Outside a request (no PluginContext on ctx) it falls back to the connector's
// init-time storeKey.
func (c *Connector) baseKey(ctx context.Context) string {
	if settings := settingsFromContext(ctx); settings != nil {
		return storeKeyFor(*settings)
	}
	return c.storeKey
}

// defaultConnection returns the single-connection-path entry for baseKey,
// opening and caching one with the current request's settings if this user has
// no pooled connection yet. Shared by the query path and the health path so both
// resolve to the same per-user connection.
func (c *Connector) defaultConnection(ctx context.Context, baseKey string) (string, CachedConnection, error) {
	key := defaultKey(baseKey)
	if dbConn, ok := c.getDBConnection(key); ok {
		return key, dbConn, nil
	}

	settings := settingsFromContext(ctx)
	if settings == nil {
		return "", CachedConnection{}, ErrorMissingDBConnection
	}

	db, err := c.driver.Connect(ctx, *settings, nil)
	if err != nil {
		return "", CachedConnection{}, backend.DownstreamError(err)
	}
	dbConn := CachedConnection{db, *settings}
	c.storeDBConnection(key, dbConn)
	return key, dbConn, nil
}

func NewConnector(ctx context.Context, driver Driver, settings backend.DataSourceInstanceSettings, enableMultipleConnections bool, opts ...ConnectorOption) (*Connector, error) {
	ds := driver.Settings(ctx, settings)
	db, err := driver.Connect(ctx, settings, nil)
	if err != nil {
		return nil, backend.DownstreamError(err)
	}

	sk := storeKeyFor(settings)
	conn := &Connector{
		UID:                       settings.UID,
		storeKey:                  sk,
		driver:                    driver,
		driverSettings:            ds,
		defaultKey:                defaultKey(sk),
		enableMultipleConnections: enableMultipleConnections,
	}
	for _, opt := range opts {
		opt(conn)
	}
	if conn.cache == nil {
		conn.cache = NewSyncMapCache()
	}
	conn.storeDBConnection(conn.defaultKey, CachedConnection{db, settings})
	return conn, nil
}

func (c *Connector) Connect(ctx context.Context, headers http.Header) (*CachedConnection, error) {
	// Health must validate the connection of the user who triggered the check,
	// not the one captured at instance init — otherwise "Save & Test" reports the
	// init user's credentials as healthy for everybody.
	key, dbConn, err := c.defaultConnection(ctx, c.baseKey(ctx))
	if err != nil {
		return nil, err
	}

	if c.driverSettings.Retries == 0 {
		err := c.connect(ctx, dbConn)
		return nil, err
	}

	err = c.connectWithRetries(ctx, dbConn, key, headers)
	return &dbConn, err
}

func (c *Connector) connectWithRetries(ctx context.Context, conn CachedConnection, key string, headers http.Header) error {
	q := &Query{}
	if c.driverSettings.ForwardHeaders {
		applyHeaders(q, headers)
	}

	var db *sql.DB
	var err error
	for i := 0; i < c.driverSettings.Retries; i++ {
		db, err = c.Reconnect(ctx, conn, q, key)
		if err != nil {
			return err
		}
		conn := CachedConnection{
			db:       db,
			settings: conn.settings,
		}
		err = c.connect(ctx, conn)
		if err == nil {
			break
		}

		if !shouldRetry(c.driverSettings.RetryOn, err.Error()) {
			break
		}

		if i+1 == c.driverSettings.Retries {
			break
		}

		if c.driverSettings.Pause > 0 {
			time.Sleep(time.Duration(c.driverSettings.Pause * int(time.Second)))
		}
		backend.Logger.Warn(fmt.Sprintf("connect failed: %s. Retrying %d times", err.Error(), i+1))
	}

	return err
}

func (c *Connector) connect(ctx context.Context, conn CachedConnection) error {
	if err := c.ping(ctx, conn); err != nil {
		return backend.DownstreamError(err)
	}

	return nil
}

func (c *Connector) ping(ctx context.Context, conn CachedConnection) error {
	if c.driverSettings.Timeout == 0 {
		return conn.db.PingContext(ctx)
	}

	ctx, cancel := context.WithTimeout(ctx, c.driverSettings.Timeout)
	defer cancel()

	return conn.db.PingContext(ctx)
}

func (c *Connector) Reconnect(ctx context.Context, dbConn CachedConnection, q *Query, cacheKey string) (*sql.DB, error) {
	db, err := c.driver.Connect(ctx, dbConn.settings, q.ConnectionArgs)
	if err != nil {
		return nil, backend.DownstreamError(err)
	}

	if err = dbConn.db.Close(); err != nil {
		backend.Logger.Warn(fmt.Sprintf("closing existing connection failed: %s", err.Error()))
	}

	c.storeDBConnection(cacheKey, CachedConnection{db, dbConn.settings})
	return db, nil
}

// connCache returns the Connector's ConnectionCache, lazily installing the
// default sync.Map-backed cache if none is set. NewConnector always installs a
// cache, so the lazy path only covers Connector literals built outside this
// package (e.g. test fixtures). Routing every cache access through this single
// helper keeps the nil policy uniform across get/store/Dispose.
func (c *Connector) connCache() ConnectionCache {
	if c.cache == nil {
		c.cache = NewSyncMapCache()
	}
	return c.cache
}

func (c *Connector) getDBConnection(key string) (CachedConnection, bool) {
	return c.connCache().Load(key)
}

func (c *Connector) storeDBConnection(key string, dbConn CachedConnection) {
	c.connCache().Store(key, dbConn)
}

// Dispose is called when an existing SQLDatasource needs to be replaced
func (c *Connector) Dispose() {
	c.connCache().Dispose()
}

func (c *Connector) GetConnectionFromQuery(ctx context.Context, q *Query) (string, CachedConnection, error) {
	if !c.enableMultipleConnections && !c.driverSettings.ForwardHeaders && len(q.ConnectionArgs) > 0 && string(q.ConnectionArgs) != "{}" {
		return "", CachedConnection{}, MissingMultipleConnectionsConfig
	}
	// The base key comes from the CURRENT request (see baseKey), not from
	// instance init, so a per-user credential rewrite routes to a connection
	// opened as that user. Keying only once in NewConnector made every user share
	// the instance-init connection; this restores the pre-v5 getStoreKey(settings)
	// behaviour without changing the upstream signature.
	baseKey := c.baseKey(ctx)
	key, dbConn, err := c.defaultConnection(ctx, baseKey)
	if err != nil {
		return "", CachedConnection{}, err
	}
	if !c.enableMultipleConnections || len(q.ConnectionArgs) == 0 {
		backend.Logger.Debug("using single user connection")
		return key, dbConn, nil
	}

	key = keyWithConnectionArgs(baseKey, q.ConnectionArgs)
	if cachedConn, ok := c.getDBConnection(key); ok {
		backend.Logger.Debug("cached connection")
		return key, cachedConn, nil
	}

	db, err := c.driver.Connect(ctx, dbConn.settings, q.ConnectionArgs)
	if err != nil {
		backend.Logger.Debug("connect error " + err.Error())
		return "", CachedConnection{}, backend.DownstreamError(err)
	}
	backend.Logger.Debug("new connection(multiple) created")
	// Assign this connection in the cache
	dbConn = CachedConnection{db, dbConn.settings}
	c.storeDBConnection(key, dbConn)

	return key, dbConn, nil
}

func shouldRetry(retryOn []string, err string) bool {
	for _, r := range retryOn {
		if strings.Contains(err, r) {
			return true
		}
	}
	return false
}
