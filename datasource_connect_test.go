package sqlds

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

// Test_storeKeyFor guards the vunet per-user connection feature: connections
// must be keyed by "<uid>-<username>" when JSONData carries a username, so users
// sharing a datasource with distinct credentials get isolated pooled connections.
func Test_storeKeyFor(t *testing.T) {
	tests := []struct {
		desc     string
		uid      string
		id       int64
		jsonData string
		want     string
	}{
		{desc: "no username keys by uid", uid: "uid1", jsonData: `{}`, want: "uid1"},
		{desc: "username is appended", uid: "uid1", jsonData: `{"username":"alice"}`, want: "uid1-alice"},
		{desc: "different users differ", uid: "uid1", jsonData: `{"username":"bob"}`, want: "uid1-bob"},
		{desc: "empty uid falls back to id", uid: "", id: 42, jsonData: `{}`, want: "42"},
		{desc: "invalid json keys by uid", uid: "uid1", jsonData: `not-json`, want: "uid1"},
		{desc: "nil json keys by uid", uid: "uid1", jsonData: ``, want: "uid1"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			settings := backend.DataSourceInstanceSettings{UID: tt.uid, ID: tt.id}
			if tt.jsonData != "" {
				settings.JSONData = json.RawMessage(tt.jsonData)
			}
			if got := storeKeyFor(settings); got != tt.want {
				t.Fatalf("storeKeyFor = %q, want %q", got, tt.want)
			}
		})
	}
}

type fakeDriver struct {
	openDBfn func(msg json.RawMessage) (*sql.DB, error)

	Driver
}

func (d fakeDriver) Connect(_ context.Context, _ backend.DataSourceInstanceSettings, msg json.RawMessage) (db *sql.DB, err error) {
	return d.openDBfn(msg)
}

func (d fakeDriver) Macros() Macros {
	return Macros{}
}

func (d fakeDriver) Converters() []sqlutil.Converter {
	return []sqlutil.Converter{}
}

type fakeSQLConnector struct{}

func (f fakeSQLConnector) Connect(_ context.Context) (driver.Conn, error) {
	return nil, nil
}

func (f fakeSQLConnector) Driver() driver.Driver {
	return nil
}

func Test_getDBConnectionFromQuery(t *testing.T) {
	db := &sql.DB{}
	db2 := &sql.DB{}
	db3 := &sql.DB{}
	d := &fakeDriver{openDBfn: func(msg json.RawMessage) (*sql.DB, error) { return db3, nil }}
	tests := []struct {
		existingDB  *sql.DB
		expectedDB  *sql.DB
		desc        string
		dsUID       string
		args        string
		expectedKey string
	}{
		{
			desc:        "it should return the default db with no args",
			dsUID:       "uid1",
			args:        "",
			expectedKey: "uid1-default",
			expectedDB:  db,
		},
		{
			desc:        "it should return the cached connection for the given args",
			dsUID:       "uid1",
			args:        "foo",
			expectedKey: "uid1-2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
			existingDB:  db2,
			expectedDB:  db2,
		},
		{
			desc:        "it should create a new connection with the given args",
			dsUID:       "uid1",
			args:        "foo",
			expectedKey: "uid1-2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
			expectedDB:  db3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			conn := &Connector{UID: tt.dsUID, storeKey: tt.dsUID, defaultKey: defaultKey(tt.dsUID), driver: d, enableMultipleConnections: true, driverSettings: DriverSettings{}, cache: NewSyncMapCache()}
			settings := backend.DataSourceInstanceSettings{UID: tt.dsUID}
			key := defaultKey(tt.dsUID)
			// Add the mandatory default db
			conn.storeDBConnection(key, CachedConnection{db, settings})
			if tt.existingDB != nil {
				key = keyWithConnectionArgs(tt.dsUID, []byte(tt.args))
				conn.storeDBConnection(key, CachedConnection{tt.existingDB, settings})
			}

			key, dbConn, err := conn.GetConnectionFromQuery(context.Background(), &Query{ConnectionArgs: json.RawMessage(tt.args)}, &settings)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if key != tt.expectedKey {
				t.Fatalf("unexpected cache key %s", key)
			}
			if dbConn.db != tt.expectedDB {
				t.Fatalf("unexpected result %v", dbConn.db)
			}
		})
	}

	t.Run("it should return an error if connection args are used without enabling multiple connections", func(t *testing.T) {
		conn := &Connector{driver: d, enableMultipleConnections: false, cache: NewSyncMapCache()}
		_, _, err := conn.GetConnectionFromQuery(context.Background(), &Query{ConnectionArgs: json.RawMessage("foo")}, nil)
		if err == nil || !errors.Is(err, MissingMultipleConnectionsConfig) {
			t.Errorf("expecting error: %v", MissingMultipleConnectionsConfig)
		}
	})

	t.Run("it should return an error if the default connection is missing", func(t *testing.T) {
		conn := &Connector{driver: d, cache: NewSyncMapCache()}
		_, _, err := conn.GetConnectionFromQuery(context.Background(), &Query{}, nil)
		if err == nil || !errors.Is(err, MissingDBConnection) {
			t.Errorf("expecting error: %v", MissingDBConnection)
		}
	})
}

// Test_GetConnectionFromQuery_perUser guards the vunet per-user connection feature
// end-to-end through the query path: two users sharing one datasource UID but with
// different JSONData usernames MUST get isolated connections, keyed per-request from
// the CURRENT request settings. This is the regression the v5 port introduced —
// keying only once in NewConnector made every user reuse the instance-init
// connection. Test_storeKeyFor passed while this real path was broken, so the guard
// has to live here, on GetConnectionFromQuery.
func Test_GetConnectionFromQuery_perUser(t *testing.T) {
	dbAlice := &sql.DB{}
	dbBob := &sql.DB{}
	d := &fakeDriver{openDBfn: func(msg json.RawMessage) (*sql.DB, error) { return dbBob, nil }}

	// Connector bootstrapped for alice, exactly as NewConnector would for the instance.
	aliceSettings := backend.DataSourceInstanceSettings{UID: "uid1", JSONData: []byte(`{"username":"alice"}`)}
	conn := &Connector{
		UID:            "uid1",
		storeKey:       storeKeyFor(aliceSettings),
		defaultKey:     defaultKey(storeKeyFor(aliceSettings)),
		driver:         d,
		driverSettings: DriverSettings{},
		cache:          NewSyncMapCache(),
	}
	conn.storeDBConnection(conn.defaultKey, CachedConnection{dbAlice, aliceSettings})

	// alice reuses the bootstrap connection under key "uid1-alice-default".
	aliceKey, aliceConn, err := conn.GetConnectionFromQuery(context.Background(), &Query{}, &aliceSettings)
	if err != nil {
		t.Fatalf("alice: unexpected error %v", err)
	}
	if aliceKey != "uid1-alice-default" {
		t.Fatalf("alice: unexpected key %q", aliceKey)
	}
	if aliceConn.db != dbAlice {
		t.Fatalf("alice: expected the bootstrap connection")
	}

	// bob MUST get a distinct key and a freshly opened connection — never alice's.
	bobSettings := backend.DataSourceInstanceSettings{UID: "uid1", JSONData: []byte(`{"username":"bob"}`)}
	bobKey, bobConn, err := conn.GetConnectionFromQuery(context.Background(), &Query{}, &bobSettings)
	if err != nil {
		t.Fatalf("bob: unexpected error %v", err)
	}
	if bobKey != "uid1-bob-default" {
		t.Fatalf("bob: unexpected key %q", bobKey)
	}
	if bobKey == aliceKey {
		t.Fatalf("per-user isolation broken: alice and bob share key %q", bobKey)
	}
	if bobConn.db == dbAlice || bobConn.db != dbBob {
		t.Fatalf("per-user isolation broken: bob did not get his own connection")
	}
}

func Test_Dispose(t *testing.T) {
	t.Run("it should close connections", func(t *testing.T) {
		db := sql.OpenDB(fakeSQLConnector{})
		d := &fakeDriver{openDBfn: func(msg json.RawMessage) (*sql.DB, error) { return db, nil }}
		conn := &Connector{driver: d, cache: NewSyncMapCache()}
		ds := &SQLDatasource{connector: conn}
		conn.storeDBConnection(defaultKey("uid1"), CachedConnection{db: db})
		conn.storeDBConnection("foo", CachedConnection{db: db})
		ds.Dispose()
		count := 0
		conn.cache.Range(func(key string, value CachedConnection) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("did not close all connections")
		}
	})
}
