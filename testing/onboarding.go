package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
)

var onboardCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y *platform.OnboardingResults) bool {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y == nil || y != nil && x == nil {
			return false
		}

		return x.User.Name == y.User.Name && x.User.OAuthID == y.User.OAuthID && x.User.Status == y.User.Status &&
			x.Org.Name == y.Org.Name && x.Org.Description == y.Org.Description &&
			x.Bucket.Type == y.Bucket.Type && x.Bucket.Description == y.Bucket.Description && x.Bucket.RetentionPolicyName == y.Bucket.RetentionPolicyName && x.Bucket.RetentionPeriod == y.Bucket.RetentionPeriod && x.Bucket.Name == y.Bucket.Name &&
			(x.Auth != nil && y.Auth != nil && cmp.Equal(x.Auth.Permissions, y.Auth.Permissions)) // its possible auth wont exist on the basic service level
	}),
}

// OnboardingFields will include the IDGenerator, TokenGenerator
// and IsOnboarding
type OnboardingFields struct {
	IDGenerator    platform2.IDGenerator
	TokenGenerator platform.TokenGenerator
	TimeGenerator  platform.TimeGenerator
	IsOnboarding   bool
}

// OnboardInitialUser testing
func OnboardInitialUser(
	init func(OnboardingFields, bool, *testing.T) (platform.OnboardingService, func()),
	t *testing.T,
) {
	type args struct {
		request *platform.OnboardingRequest
	}
	type wants struct {
		errCode string
		results *platform.OnboardingResults
	}
	tests := []struct {
		name   string
		fields OnboardingFields
		args   args
		wants  wants
	}{
		{
			name: "denied",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   false,
			},
			wants: wants{
				errCode: errors.EConflict,
			},
		},
		{
			name: "missing username",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					Org:    "org1",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: errors.EUnprocessableEntity,
			},
		},
		{
			name: "missing org",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:   "admin",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: errors.EUnprocessableEntity,
			},
		},
		{
			name: "missing bucket",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User: "admin",
					Org:  "org1",
				},
			},
			wants: wants{
				errCode: errors.EUnprocessableEntity,
			},
		},
		{
			name: "valid onboarding json should create a user, org, bucket, and authorization",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TimeGenerator:  mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:                   "admin",
					Org:                    "org1",
					Bucket:                 "bucket1",
					Password:               "password1",
					RetentionPeriodSeconds: 3600 * 24 * 7, // 1 week
				},
			},
			wants: wants{
				results: &platform.OnboardingResults{
					User: &platform.User{
						ID:     MustIDBase16(oneID),
						Name:   "admin",
						Status: platform.Active,
					},
					Org: &platform.Organization{
						ID:   MustIDBase16(twoID),
						Name: "org1",
						CRUDLog: platform.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Bucket: &platform.Bucket{
						ID:              MustIDBase16(threeID),
						Name:            "bucket1",
						OrgID:           MustIDBase16(twoID),
						RetentionPeriod: time.Hour * 24 * 7,
						CRUDLog: platform.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Auth: &platform.Authorization{
						ID:          MustIDBase16(fourID),
						Token:       oneToken,
						Status:      platform.Active,
						UserID:      MustIDBase16(oneID),
						Description: "admin's Token",
						OrgID:       MustIDBase16(twoID),
						Permissions: platform.OperPermissions(),
						CRUDLog: platform.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		for _, useTokenHashing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/TokenHashing=%t", tt.name, useTokenHashing), func(t *testing.T) {
				s, done := init(tt.fields, useTokenHashing, t)
				defer done()
				ctx := context.Background()
				results, err := s.OnboardInitialUser(ctx, tt.args.request)
				if tt.wants.errCode == "" {
					require.NoError(t, err)
				} else {
					require.Equal(t, tt.wants.errCode, errors.ErrorCode(err))
				}
				diff := cmp.Diff(results, tt.wants.results, onboardCmpOptions)
				require.Empty(t, diff)
			})
		}
	}
}

const (
	oneID    = "020f755c3c082000"
	twoID    = "020f755c3c082001"
	threeID  = "020f755c3c082002"
	fourID   = "020f755c3c082003"
	fiveID   = "020f755c3c082004"
	sixID    = "020f755c3c082005"
	oneToken = "020f755c3c082008"
)

type loopIDGenerator struct {
	s []string
	p int
}

func (g *loopIDGenerator) ID() platform2.ID {
	if g.p == len(g.s) {
		g.p = 0
	}
	id := MustIDBase16(g.s[g.p])
	g.p++
	return id
}
