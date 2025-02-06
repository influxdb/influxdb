package query

import (
	"time"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

// Logger persists metadata about executed queries.
type Logger interface {
	Log(Log) error
}

// Log captures a query and any relevant metadata for the query execution.
type Log struct {
	// Time is the time the query was completed
	Time time.Time
	// OrganizationID is the ID of the organization that requested the query
	OrganizationID platform2.ID
	// TraceID is the ID of the trace related to this query
	TraceID string
	// Sampled specifies whether the trace for TraceID was chosen for permanent storage
	// by the sampling mechanism of the tracer
	Sampled bool
	// Error is any error encountered by the query
	Error error

	// ProxyRequest is the query request
	ProxyRequest *ProxyRequest
	// ResponseSize is the size in bytes of the query response
	ResponseSize int64
	// Statistics is a set of statistics about the query execution
	Statistics flux.Statistics
}

// Redact removes any sensitive information before logging
func (q *Log) Redact() {
	if q.ProxyRequest != nil && q.ProxyRequest.Request.Authorization != nil {
		// Make shallow copy of request
		request := new(ProxyRequest)
		*request = *q.ProxyRequest

		// Make shallow copy of authorization
		auth := new(platform.Authorization)
		*auth = *q.ProxyRequest.Request.Authorization
		// Redact authorization token
		auth.Token = ""
		auth.HashedToken = ""

		// Apply redacted authorization
		request.Request.Authorization = auth

		// Apply redacted request
		q.ProxyRequest = request
	}
}
