package httpd

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "InfluxDB"

	// DefaultBindSocket is the default unix socket to bind to.
	DefaultBindSocket = "/var/run/influxdb.sock"

	// DefaultMaxBodySize is the default maximum size of a client request body, in bytes. Specify 0 for no limit.
	DefaultMaxBodySize = 25e6

	// DefaultEnqueuedWriteTimeout is the maximum time a write request can wait to be processed.
	DefaultEnqueuedWriteTimeout = 30 * time.Second
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled                 bool              `toml:"enabled"`
	BindAddress             string            `toml:"bind-address"`
	AuthEnabled             bool              `toml:"auth-enabled"`
	LogEnabled              bool              `toml:"log-enabled"`
	SuppressWriteLog        bool              `toml:"suppress-write-log"`
	WriteTracing            bool              `toml:"write-tracing"`
	FluxEnabled             bool              `toml:"flux-enabled"`
	FluxLogEnabled          bool              `toml:"flux-log-enabled"`
	FluxTesting             bool              `toml:"flux-testing"`
	PprofEnabled            bool              `toml:"pprof-enabled"`
	PprofAuthEnabled        bool              `toml:"pprof-auth-enabled"`
	DebugPprofEnabled       bool              `toml:"debug-pprof-enabled"`
	PingAuthEnabled         bool              `toml:"ping-auth-enabled"`
	PromReadAuthEnabled     bool              `toml:"prom-read-auth-enabled"`
	HTTPHeaders             map[string]string `toml:"headers"`
	HTTPSEnabled            bool              `toml:"https-enabled"`
	HTTPSCertificate        string            `toml:"https-certificate"`
	HTTPSPrivateKey         string            `toml:"https-private-key"`
	MaxRowLimit             int               `toml:"max-row-limit"`
	MaxConnectionLimit      int               `toml:"max-connection-limit"`
	SharedSecret            string            `toml:"shared-secret"`
	Realm                   string            `toml:"realm"`
	UnixSocketEnabled       bool              `toml:"unix-socket-enabled"`
	UnixSocketGroup         *toml.Group       `toml:"unix-socket-group"`
	UnixSocketPermissions   toml.FileMode     `toml:"unix-socket-permissions"`
	BindSocket              string            `toml:"bind-socket"`
	MaxBodySize             int               `toml:"max-body-size"`
	AccessLogPath           string            `toml:"access-log-path"`
	AccessLogStatusFilters  []StatusFilter    `toml:"access-log-status-filters"`
	MaxConcurrentWriteLimit int               `toml:"max-concurrent-write-limit"`
	MaxEnqueuedWriteLimit   int               `toml:"max-enqueued-write-limit"`
	EnqueuedWriteTimeout    time.Duration     `toml:"enqueued-write-timeout"`
	TLS                     *tls.Config       `toml:"-"`
	ReadTimeout             time.Duration     `toml:"read-timeout"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:               true,
		FluxEnabled:           false,
		FluxLogEnabled:        false,
		FluxTesting:           false,
		BindAddress:           DefaultBindAddress,
		LogEnabled:            true,
		PprofEnabled:          true,
		PprofAuthEnabled:      false,
		DebugPprofEnabled:     false,
		PingAuthEnabled:       false,
		PromReadAuthEnabled:   false,
		HTTPSEnabled:          false,
		HTTPSCertificate:      "/etc/ssl/influxdb.pem",
		MaxRowLimit:           0,
		Realm:                 DefaultRealm,
		UnixSocketEnabled:     false,
		UnixSocketPermissions: 0777,
		BindSocket:            DefaultBindSocket,
		MaxBodySize:           DefaultMaxBodySize,
		EnqueuedWriteTimeout:  DefaultEnqueuedWriteTimeout,
	}
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":              true,
		"bind-address":         c.BindAddress,
		"https-enabled":        c.HTTPSEnabled,
		"max-row-limit":        c.MaxRowLimit,
		"max-connection-limit": c.MaxConnectionLimit,
		"access-log-path":      c.AccessLogPath,
		"flux-enabled":         c.FluxEnabled,
	}), nil
}

// StatusFilter will check if an http status code matches a certain pattern.
type StatusFilter struct {
	base    int
	divisor int
}

// reStatusFilter ensures that the format is digits optionally followed by X values.
var reStatusFilter = regexp.MustCompile(`^([1-5]\d*)([xX]*)$`)

// ParseStatusFilter will create a new status filter from the string.
func ParseStatusFilter(s string) (StatusFilter, error) {
	m := reStatusFilter.FindStringSubmatch(s)
	if m == nil {
		return StatusFilter{}, fmt.Errorf("status filter must be a digit that starts with 1-5 optionally followed by X characters")
	} else if len(s) != 3 {
		return StatusFilter{}, fmt.Errorf("status filter must be exactly 3 characters long")
	}

	// Compute the divisor and the expected value. If we have one X, we divide by 10 so we are only comparing
	// the first two numbers. If we have two Xs, we divide by 100 so we only compare the first number. We
	// then check if the result is equal to the remaining number.
	base, err := strconv.Atoi(m[1])
	if err != nil {
		return StatusFilter{}, err
	}

	divisor := 1
	switch len(m[2]) {
	case 1:
		divisor = 10
	case 2:
		divisor = 100
	}
	return StatusFilter{
		base:    base,
		divisor: divisor,
	}, nil
}

// Match will check if the status code matches this filter.
func (sf StatusFilter) Match(statusCode int) bool {
	if sf.divisor == 0 {
		return false
	}
	return statusCode/sf.divisor == sf.base
}

// UnmarshalText parses a TOML value into a duration value.
func (sf *StatusFilter) UnmarshalText(text []byte) error {
	f, err := ParseStatusFilter(string(text))
	if err != nil {
		return err
	}
	*sf = f
	return nil
}

// MarshalText converts a duration to a string for decoding toml
func (sf StatusFilter) MarshalText() (text []byte, err error) {
	var buf bytes.Buffer
	if sf.base != 0 {
		buf.WriteString(strconv.Itoa(sf.base))
	}

	switch sf.divisor {
	case 1:
	case 10:
		buf.WriteString("X")
	case 100:
		buf.WriteString("XX")
	default:
		return nil, errors.New("invalid status filter")
	}
	return buf.Bytes(), nil
}

type StatusFilters []StatusFilter

func (filters StatusFilters) Match(statusCode int) bool {
	if len(filters) == 0 {
		return true
	}

	for _, sf := range filters {
		if sf.Match(statusCode) {
			return true
		}
	}
	return false
}
