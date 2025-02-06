package auth

import (
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/testhelper"
	"github.com/stretchr/testify/require"
)

func Test_Auth_Basic(t *testing.T) {
	db := testhelper.NewTestBoltDb(t)
	defer db.Close()
	require.Equal(t, ""+
		`ID			User Name	User ID			Description			Token												Permissions`+"\n"+
		`08371db24dcc8000	testuser	08371db1dd8c8000	testuser's Token		A9Ovdl8SmP-rfp8wQ2vJoPUsZoQQJ3EochD88SlJcgrcLw4HBwgUqpSHQxc9N9Drg0_aY6Lp1jutBRcKhbV7aQ==	[read:annotations write:annotations read:authorizations write:authorizations read:buckets write:buckets read:checks write:checks read:dashboards write:dashboards read:dbrp write:dbrp read:documents write:documents read:labels write:labels read:notebooks write:notebooks read:notificationEndpoints write:notificationEndpoints read:notificationRules write:notificationRules read:orgs write:orgs read:scrapers write:scrapers read:secrets write:secrets read:sources write:sources read:tasks write:tasks read:telegrafs write:telegrafs read:users write:users read:variables write:variables read:views write:views read:remotes write:remotes read:replications write:replications]`+"\n"+
		`08371deae98c8000	testuser	08371db1dd8c8000	testuser's read buckets token	4-pZrlm84u9uiMVrPBeITe46KxfdEnvTX5H2CZh38BtAsXX4O47b8QwZ9jHL_Cek2w-VbVfRxDpo0Mu8ORiqyQ==	[read:orgs/dd7cd2292f6e974a/buckets]`+"\n",
		testhelper.MustRunCommand(t, NewAuthCommand(), "list", "--bolt-path", db.Name()))

	// org name not created
	require.EqualError(t, testhelper.RunCommand(t, NewAuthCommand(), "create-operator", "--bolt-path", db.Name(), "--org", "not-exist", "--username", "testuser"), "could not find org \"not-exist\": organization name \"not-exist\" not found")

	// user not created
	require.EqualError(t, testhelper.RunCommand(t, NewAuthCommand(), "create-operator", "--bolt-path", db.Name(), "--org", "myorg", "--username", "testuser2"), "could not find user \"testuser2\": user not found")

	// existing user creates properly
	require.NoError(t, testhelper.RunCommand(t, NewAuthCommand(), "create-operator", "--bolt-path", db.Name(), "--username", "testuser", "--org", "myorg"))

	require.Regexp(t, ""+
		`ID			User Name	User ID			Description			Token												Permissions`+"\n"+
		`08371db24dcc8000	testuser	08371db1dd8c8000	testuser's Token		A9Ovdl8SmP-rfp8wQ2vJoPUsZoQQJ3EochD88SlJcgrcLw4HBwgUqpSHQxc9N9Drg0_aY6Lp1jutBRcKhbV7aQ==	\[read:annotations write:annotations read:authorizations write:authorizations read:buckets write:buckets read:checks write:checks read:dashboards write:dashboards read:dbrp write:dbrp read:documents write:documents read:labels write:labels read:notebooks write:notebooks read:notificationEndpoints write:notificationEndpoints read:notificationRules write:notificationRules read:orgs write:orgs read:scrapers write:scrapers read:secrets write:secrets read:sources write:sources read:tasks write:tasks read:telegrafs write:telegrafs read:users write:users read:variables write:variables read:views write:views read:remotes write:remotes read:replications write:replications]`+"\n"+
		`08371deae98c8000	testuser	08371db1dd8c8000	testuser's read buckets token	4-pZrlm84u9uiMVrPBeITe46KxfdEnvTX5H2CZh38BtAsXX4O47b8QwZ9jHL_Cek2w-VbVfRxDpo0Mu8ORiqyQ==	\[read:orgs/dd7cd2292f6e974a/buckets\]`+"\n"+
		`[^\t]*	testuser	[^\t]*	testuser's Recovery Token	[^\t]*	\[read:authorizations write:authorizations read:buckets write:buckets read:dashboards write:dashboards read:orgs write:orgs read:sources write:sources read:tasks write:tasks read:telegrafs write:telegrafs read:users write:users read:variables write:variables read:scrapers write:scrapers read:secrets write:secrets read:labels write:labels read:views write:views read:documents write:documents read:notificationRules write:notificationRules read:notificationEndpoints write:notificationEndpoints read:checks write:checks read:dbrp write:dbrp read:notebooks write:notebooks read:annotations write:annotations read:remotes write:remotes read:replications write:replications\]`+"\n",
		testhelper.MustRunCommand(t, NewAuthCommand(), "list", "--bolt-path", db.Name()))
}
