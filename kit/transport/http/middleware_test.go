package http

import (
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap/zaptest"

	"github.com/influxdata/influxdb/v2/pkg/testttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {
	tests := []struct {
		name          string
		reqPath       string
		wantCount     int
		labelResponse string
		labelStatus   string
	}{
		{
			name:          "counter increments on success code",
			reqPath:       "/",
			wantCount:     1,
			labelResponse: "200",
			labelStatus:   "2XX",
		},
		{
			name:          "counter does not increment on failure code",
			reqPath:       "/badpath",
			wantCount:     0,
			labelResponse: "404",
			labelStatus:   "4XX",
		},
	}

	labels := []string{"handler", "method", "path", "status", "response_code", "user_agent"}

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "counter"}, labels)
			hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "hist"}, labels)
			reg := prom.NewRegistry(zaptest.NewLogger(t))
			reg.MustRegister(counter, hist)

			metricsMw := Metrics("testing", counter, hist)
			svr := metricsMw(nextHandler)
			r := httptest.NewRequest("GET", tt.reqPath, nil)
			w := httptest.NewRecorder()
			svr.ServeHTTP(w, r)

			mfs := promtest.MustGather(t, reg)
			m := promtest.FindMetric(mfs, "counter", map[string]string{
				"handler":       "testing",
				"method":        "GET",
				"path":          "/",
				"response_code": tt.labelResponse,
				"status":        tt.labelStatus,
				"user_agent":    "unknown",
			})

			if tt.wantCount == 0 {
				require.Nil(t, m)
				return
			}

			require.Equal(t, tt.wantCount, int(m.Counter.GetValue()))
			t.Fatal("lol")
		})
	}
}

func Test_normalizePath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "1",
			path:     path.Join("/api/v2/organizations", platform.ID(2).String()),
			expected: "/api/v2/organizations/:id",
		},
		{
			name:     "2",
			path:     "/api/v2/organizations",
			expected: "/api/v2/organizations",
		},
		{
			name:     "3",
			path:     "/",
			expected: "/",
		},
		{
			name:     "4",
			path:     path.Join("/api/v2/organizations", platform.ID(2).String(), "users", platform.ID(3).String()),
			expected: "/api/v2/organizations/:id/users/:id",
		},
		{
			name:     "5",
			path:     "/838442d56d.svg",
			expected: "/" + fileSlug + ".svg",
		},
		{
			name:     "6",
			path:     "/838442d56d.svg/extra",
			expected: "/838442d56d.svg/extra",
		},
		{
			name:     "7",
			path:     "/api/v2/restore/shards/1001",
			expected: path.Join("/api/v2/restore/shards/", shardSlug),
		},
		{
			name:     "8",
			path:     "/api/v2/restore/shards/1001/extra",
			expected: path.Join("/api/v2/restore/shards/", shardSlug, "extra"),
		},
		{
			name:     "9",
			path:     "/api/v2/backup/shards/1005",
			expected: path.Join("/api/v2/backup/shards/", shardSlug),
		},
		{
			name:     "10",
			path:     "/api/v2/backup/shards/1005/extra",
			expected: path.Join("/api/v2/backup/shards/", shardSlug, "extra"),
		},
		{
			name:     "11",
			path:     "/35bb8d560d.ttf",
			expected: "/" + fileSlug + ".ttf",
		},
		{
			name:     "12",
			path:     "/35bb8d560d.woff",
			expected: "/" + fileSlug + ".woff",
		},
		{
			name:     "13",
			path:     "/35bb8d560d.eot",
			expected: "/" + fileSlug + ".eot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := normalizePath(tt.path)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestCors(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("nextHandler"))
	})

	tests := []struct {
		name            string
		method          string
		headers         []string
		expectedStatus  int
		expectedHeaders map[string]string
	}{
		{
			name:           "OPTIONS without Origin",
			method:         "OPTIONS",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "OPTIONS with Origin",
			method:         "OPTIONS",
			headers:        []string{"Origin", "http://myapp.com"},
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "GET with Origin",
			method:         "GET",
			headers:        []string{"Origin", "http://anotherapp.com"},
			expectedStatus: http.StatusOK,
			expectedHeaders: map[string]string{
				"Access-Control-Allow-Origin": "http://anotherapp.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := SkipOptions(SetCORS(nextHandler))

			testttp.
				HTTP(t, tt.method, "/", nil).
				Headers("", "", tt.headers...).
				Do(svr).
				ExpectStatus(tt.expectedStatus).
				ExpectHeaders(tt.expectedHeaders)
		})
	}
}
