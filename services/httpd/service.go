// Package httpd implements the HTTP service and REST API for InfluxDB.
package httpd // import "github.com/influxdata/influxdb/services/httpd"

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/models"
	"go.uber.org/zap"
)

// statistics gathered by the httpd package.
const (
	statRequest                          = "req"                    // Number of HTTP requests served.
	statQueryRequest                     = "queryReq"               // Number of query requests served.
	statWriteRequest                     = "writeReq"               // Number of write requests serverd.
	statPingRequest                      = "pingReq"                // Number of ping requests served.
	statStatusRequest                    = "statusReq"              // Number of status requests served.
	statWriteRequestBytesReceived        = "writeReqBytes"          // Sum of all bytes in write requests.
	statQueryRequestBytesTransmitted     = "queryRespBytes"         // Sum of all bytes returned in query reponses.
	statPointsWrittenOK                  = "pointsWrittenOK"        // Number of points written OK.
	statValuesWrittenOK                  = "valuesWrittenOK"        // Number of values (fields) written OK.
	statPointsWrittenDropped             = "pointsWrittenDropped"   // Number of points dropped by the storage engine.
	statPointsWrittenFail                = "pointsWrittenFail"      // Number of points that failed to be written.
	statAuthFail                         = "authFail"               // Number of authentication failures.
	statRequestDuration                  = "reqDurationNs"          // Number of (wall-time) nanoseconds spent inside requests.
	statQueryRequestDuration             = "queryReqDurationNs"     // Number of (wall-time) nanoseconds spent inside query requests.
	statWriteRequestDuration             = "writeReqDurationNs"     // Number of (wall-time) nanoseconds spent inside write requests.
	statRequestsActive                   = "reqActive"              // Number of currently active requests.
	statWriteRequestsActive              = "writeReqActive"         // Number of currently active write requests.
	statClientError                      = "clientError"            // Number of HTTP responses due to client error.
	statServerError                      = "serverError"            // Number of HTTP responses due to server error.
	statRecoveredPanics                  = "recoveredPanics"        // Number of panics recovered by HTTP handler.
	statPromWriteRequest                 = "promWriteReq"           // Number of write requests to the prometheus endpoint.
	statPromReadRequest                  = "promReadReq"            // Number of read requests to the prometheus endpoint.
	statFluxQueryRequests                = "fluxQueryReq"           // Number of flux query requests served.
	statFluxQueryRequestDuration         = "fluxQueryReqDurationNs" // Number of (wall-time) nanoseconds spent executing Flux query requests.
	statFluxQueryRequestBytesTransmitted = "fluxQueryRespBytes"     // Sum of all bytes returned in Flux query reponses.

)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln        net.Listener
	addr      string
	https     bool
	cert      string
	key       string
	limit     int
	tlsConfig *tls.Config
	err       chan error

	httpServer http.Server

	unixSocket         bool
	unixSocketPerm     uint32
	unixSocketGroup    int
	bindSocket         string
	unixSocketListener net.Listener

	Handler *Handler

	Logger *zap.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	handler := NewHandler(c)
	s := &Service{
		addr:           c.BindAddress,
		https:          c.HTTPSEnabled,
		cert:           c.HTTPSCertificate,
		key:            c.HTTPSPrivateKey,
		limit:          c.MaxConnectionLimit,
		tlsConfig:      c.TLS,
		err:            make(chan error),
		unixSocket:     c.UnixSocketEnabled,
		unixSocketPerm: uint32(c.UnixSocketPermissions),
		bindSocket:     c.BindSocket,
		Handler:        handler,
		httpServer: http.Server{
			Handler:     handler,
			ReadTimeout: c.ReadTimeout,
		},
		Logger: zap.NewNop(),
	}
	if s.tlsConfig == nil {
		s.tlsConfig = new(tls.Config)
	}
	if s.key == "" {
		s.key = s.cert
	}
	if c.UnixSocketGroup != nil {
		s.unixSocketGroup = int(*c.UnixSocketGroup)
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting HTTP service", zap.Bool("authentication", s.Handler.Config.AuthEnabled))

	s.Handler.Open()

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.key)
		if err != nil {
			return err
		}

		tlsConfig := s.tlsConfig.Clone()
		tlsConfig.Certificates = []tls.Certificate{cert}

		listener, err := tls.Listen("tcp", s.addr, tlsConfig)
		if err != nil {
			return err
		}

		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}

		s.ln = listener
	}
	s.Logger.Info("Listening on HTTP",
		zap.Stringer("addr", s.ln.Addr()),
		zap.Bool("https", s.https))

	// Open unix socket listener.
	if s.unixSocket {
		if runtime.GOOS == "windows" {
			return fmt.Errorf("unable to use unix socket on windows")
		}
		if err := os.MkdirAll(path.Dir(s.bindSocket), 0777); err != nil {
			return err
		}
		if err := syscall.Unlink(s.bindSocket); err != nil && !os.IsNotExist(err) {
			return err
		}

		listener, err := net.Listen("unix", s.bindSocket)
		if err != nil {
			return err
		}
		if s.unixSocketPerm != 0 {
			if err := os.Chmod(s.bindSocket, os.FileMode(s.unixSocketPerm)); err != nil {
				return err
			}
		}
		if s.unixSocketGroup != 0 {
			if err := os.Chown(s.bindSocket, -1, s.unixSocketGroup); err != nil {
				return err
			}
		}

		s.Logger.Info("Listening on unix socket",
			zap.Stringer("addr", listener.Addr()))
		s.unixSocketListener = listener

		go s.serveUnixSocket()
	}

	// Enforce a connection limit if one has been given.
	if s.limit > 0 {
		s.ln = LimitListener(s.ln, s.limit)
	}

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		if s.ln.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Begin listening for requests in a separate goroutine.
	go s.serveTCP()
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	s.Handler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	if s.ln != nil {
		if err := s.ln.Close(); err != nil {
			return err
		}
	}
	if s.unixSocketListener != nil {
		if err := s.unixSocketListener.Close(); err != nil {
			return err
		}
	}
	return nil
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "httpd"))
	s.Handler.Logger = s.Logger
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return s.Handler.Statistics(models.NewTags(map[string]string{"bind": s.addr}).Merge(tags).Map())
}

// BoundHTTPAddr returns the string version of the address that the HTTP server is listening on.
// This is useful if you start an ephemeral server in test with bind address localhost:0.
func (s *Service) BoundHTTPAddr() string {
	return s.ln.Addr().String()
}

// serveTCP serves the handler from the TCP listener.
func (s *Service) serveTCP() {
	s.serve(s.ln)
}

// serveUnixSocket serves the handler from the unix socket listener.
func (s *Service) serveUnixSocket() {
	s.serve(s.unixSocketListener)
}

// serve serves the handler from the listener.
func (s *Service) serve(listener net.Listener) {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	if err := s.httpServer.Serve(listener); err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.Addr(), err)
	}
}
