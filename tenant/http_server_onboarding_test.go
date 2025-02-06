package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func initOnboardHttpService(f itesting.OnboardingFields, useTokenHashing bool, t *testing.T) (influxdb.OnboardingService, func()) {
	t.Helper()
	ctx := context.Background()

	s := itesting.NewTestInmemStore(t)
	storage := tenant.NewStore(s)

	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(ctx, s, useTokenHashing)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := tenant.NewOnboardService(ten, authSvc)

	if !f.IsOnboarding {
		// create a dummy so so we can no longer onboard
		err := ten.CreateUser(ctx, &influxdb.User{Name: "dummy", Status: influxdb.Active})
		if err != nil {
			t.Fatal(err)
		}
	}

	handler := tenant.NewHTTPOnboardHandler(zaptest.NewLogger(t), svc)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)
	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := tenant.OnboardClientService{
		Client: httpClient,
	}

	return &client, server.Close
}

func TestOnboardService(t *testing.T) {
	itesting.OnboardInitialUser(initOnboardHttpService, t)
}
