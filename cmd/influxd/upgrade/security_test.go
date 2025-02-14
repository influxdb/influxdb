package upgrade

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/tenant"
	authv1 "github.com/influxdata/influxdb/v2/v1/authorization"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/bcrypt"
)

func TestUpgradeSecurity(t *testing.T) {

	type testCase struct {
		name    string
		users   []meta.UserInfo
		db2ids  map[string][]platform.ID
		wantErr error
		want    []*influxdb.Authorization
	}

	hash := func(password string) string {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		require.NoError(t, err)
		return string(hash)
	}

	var testCases = []testCase{
		{
			name: "ordinary",
			users: []meta.UserInfo{
				{ // not upgraded because admin
					Name:  "superman",
					Admin: true,
					Hash:  hash("superman@123"),
				},
				{ // not upgraded because no privileges
					Name:  "loser",
					Admin: false,
					Hash:  hash("loser@123"),
				},
				{
					Name:  "weatherman",
					Admin: false,
					Hash:  hash("weatherman@123"),
					Privileges: map[string]influxql.Privilege{
						"water": influxql.AllPrivileges,
						"air":   influxql.AllPrivileges,
					},
				},
				{
					Name:  "hitgirl",
					Admin: false,
					Hash:  hash("hitgirl@123"),
					Privileges: map[string]influxql.Privilege{
						"hits": influxql.WritePrivilege,
					},
				},
				{
					Name:  "boss@hits.org", // special name
					Admin: false,
					Hash:  hash("boss@123"),
					Privileges: map[string]influxql.Privilege{
						"hits": influxql.AllPrivileges,
					},
				},
				{
					Name:  "viewer",
					Admin: false,
					Hash:  hash("viewer@123"),
					Privileges: map[string]influxql.Privilege{
						"water": influxql.ReadPrivilege,
						"air":   influxql.ReadPrivilege,
					},
				},
			},
			db2ids: map[string][]platform.ID{
				"water": {0x33f9d67bc9cbc5b7, 0x33f9d67bc9cbc5b8, 0x33f9d67bc9cbc5b9},
				"air":   {0x43f9d67bc9cbc5b7, 0x43f9d67bc9cbc5b8, 0x43f9d67bc9cbc5b9},
				"hits":  {0x53f9d67bc9cbc5b7},
			},
			want: []*influxdb.Authorization{
				{
					Token:       "boss@hits.org",
					Status:      "active",
					Description: "boss@hits.org's Legacy Token",
				},
				{
					Token:       "hitgirl",
					Status:      "active",
					Description: "hitgirl's Legacy Token",
				},
				{
					Token:       "viewer",
					Status:      "active",
					Description: "viewer's Legacy Token",
				},
				{
					Token:       "weatherman",
					Status:      "active",
					Description: "weatherman's Legacy Token",
				},
			},
		},
		{
			name: "missing buckets",
			users: []meta.UserInfo{
				{
					Name:  "weatherman",
					Admin: false,
					Hash:  hash("weatherman@123"),
					Privileges: map[string]influxql.Privilege{
						"water": influxql.AllPrivileges,
						"air":   influxql.AllPrivileges,
					},
				},
			},
			db2ids:  nil,
			wantErr: errors.New("upgrade: there were errors/warnings, please fix them and run the command again"),
		},
		{
			name:  "no users",
			users: []meta.UserInfo{},
		},
	}

	for _, tc := range testCases {
		for _, useHashedTokens := range []bool{false, true} {
			tc := tc
			t.Run(fmt.Sprintf("%s/HashedTokens=%t", tc.name, useHashedTokens), func(t *testing.T) { // better to not run in parallel
				ctx := context.Background()
				log := zaptest.NewLogger(t)

				// mock v1 meta
				v1 := &influxDBv1{
					meta: &meta.Client{},
				}
				data := &meta.Data{
					Users: tc.users,
				}
				f := reflect.ValueOf(v1.meta).Elem().Field(4)
				f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
				f.Set(reflect.ValueOf(data))

				// mock v2 meta
				kvStore := inmem.NewKVStore()
				migrator, err := migration.NewMigrator(zap.NewNop(), kvStore, all.Migrations[:]...)
				require.NoError(t, err)
				err = migrator.Up(ctx)
				require.NoError(t, err)

				authStoreV1, err := authv1.NewStore(kvStore)
				require.NoError(t, err)

				tenantStore := tenant.NewStore(kvStore)
				tenantSvc := tenant.NewService(tenantStore)

				authStoreV2, err := authorization.NewStore(ctx, kvStore, useHashedTokens)
				require.NoError(t, err)

				v2 := &influxDBv2{
					authSvc: authv1.NewService(authStoreV1, tenantSvc),
					onboardSvc: tenant.NewOnboardService(
						tenantSvc,
						authorization.NewService(authStoreV2, tenantSvc),
					),
				}

				// onboard admin
				oReq := &influxdb.OnboardingRequest{
					User:                   "admin",
					Password:               "12345678",
					Org:                    "testers",
					Bucket:                 "def",
					RetentionPeriodSeconds: influxdb.InfiniteRetention,
				}
				oResp, err := setupAdmin(ctx, v2, oReq)
				require.NoError(t, err)

				// target options
				targetOptions := optionsV2{
					userName: oReq.User,
					orgName:  oReq.Org,
					token:    oResp.Auth.Token,
					orgID:    oResp.Auth.OrgID,
					userID:   oResp.Auth.UserID,
				}

				for k, v := range tc.db2ids {
					for i, id := range v {
						b := &influxdb.Bucket{
							ID:    id,
							Name:  fmt.Sprintf("%s_%d", k, id),
							OrgID: targetOptions.orgID,
						}
						err := tenantSvc.CreateBucket(context.Background(), b)
						require.NoError(t, err)
						tc.db2ids[k][i] = b.ID
					}
				}

				// fill in expected permissions now that we know IDs
				for _, want := range tc.want {
					for _, user := range tc.users {
						if want.Token == user.Name { // v1 username is v2 token
							var permissions []influxdb.Permission
							for db, privilege := range user.Privileges {
								ids, ok := tc.db2ids[db]
								require.True(t, ok)
								for _, id := range ids {
									id := id
									resource := influxdb.Resource{
										Type:  influxdb.BucketsResourceType,
										OrgID: &targetOptions.orgID,
										ID:    &id,
									}
									switch privilege {
									case influxql.ReadPrivilege:
										permissions = append(permissions, influxdb.Permission{
											Action:   influxdb.ReadAction,
											Resource: resource,
										})
									case influxql.WritePrivilege:
										permissions = append(permissions, influxdb.Permission{
											Action:   influxdb.WriteAction,
											Resource: resource,
										})
									case influxql.AllPrivileges:
										permissions = append(permissions, influxdb.Permission{
											Action:   influxdb.ReadAction,
											Resource: resource,
										})
										permissions = append(permissions, influxdb.Permission{
											Action:   influxdb.WriteAction,
											Resource: resource,
										})
									}
								}
							}
							want.Permissions = permissions
						}
					}
				}

				// command execution
				n, err := upgradeUsers(ctx, v1, v2, &targetOptions, tc.db2ids, log)
				if tc.wantErr != nil {
					require.EqualError(t, err, tc.wantErr.Error())
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, len(tc.want), n, "Upgraded count must match")

				for _, want := range tc.want {
					actual, err := v2.authSvc.FindAuthorizationByToken(ctx, want.Token)
					require.NoError(t, err)
					require.Equal(t, targetOptions.orgID, actual.OrgID)
					require.Equal(t, targetOptions.userID, actual.UserID)
					require.Equal(t, want.Token, actual.Token)
					require.Equal(t, want.Description, actual.Description)
					require.Equal(t, want.Status, actual.Status)
					require.ElementsMatch(t, want.Permissions, actual.Permissions)
				}
			})
		}
	}
}
