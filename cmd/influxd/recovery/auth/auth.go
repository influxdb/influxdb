package auth

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/influxdata/influx-cli/v2/pkg/tabwriter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewAuthCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "auth",
		Short: "On-disk authorization management commands, for recovery",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErrf("See '%s -h' for help\n", cmd.CommandPath())
		},
	}

	base.AddCommand(NewAuthListCommand())
	base.AddCommand(NewAuthCreateCommand())

	return base
}

type authListCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
}

func NewAuthListCommand() *cobra.Command {
	var authCmd authListCommand
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List authorizations",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			authCmd.logger = newLogger
			authCmd.out = cmd.OutOrStdout()
			return authCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")

	cmd.Flags().StringVar(&authCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file.")

	return cmd
}

func (cmd *authListCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)

	// Giving an already extant AuthorizationHasher avoids scanning the existing hashes in the store.
	hasher, err := authorization.NewAuthorizationHasher()
	if err != nil {
		return err
	}
	// Create authStore read-only since we're not properly configuring if hashed tokens are enabled.
	authStore, err := authorization.NewStore(ctx, store, false, authorization.WithReadOnly(true), authorization.WithAuthorizationHasher(hasher))
	if err != nil {
		return err
	}
	// The value of useHashedTokens doesn't matter since authStore is read-only.
	auth := authorization.NewService(authStore, tenantService)
	filter := influxdb.AuthorizationFilter{}
	auths, _, err := auth.FindAuthorizations(ctx, filter)
	if err != nil {
		return err
	}

	return PrintAuth(ctx, cmd.out, auths, tenantService)
}

type authCreateCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
	username string
	org      string

	useTokenHashing bool
}

func NewAuthCreateCommand() *cobra.Command {
	var authCmd authCreateCommand
	cmd := &cobra.Command{
		Use:   "create-operator",
		Short: "Create new operator token for a user",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			authCmd.logger = newLogger
			authCmd.out = cmd.OutOrStdout()
			return authCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")
	cmd.Flags().StringVar(&authCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file")
	cmd.Flags().StringVar(&authCmd.username, "username", "", "Name of the user")
	cmd.Flags().StringVar(&authCmd.org, "org", "", "Name of the org")
	cmd.Flags().BoolVar(&authCmd.useTokenHashing, "store-token-hashes", false, "Store token hashes")

	return cmd
}

func (cmd *authCreateCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)
	hashVariantName := authorization.DefaultHashVariantName // In the future this could come from cmd
	ignoreMissingHashIndex := !cmd.useTokenHashing          // we can ignore a missing index only if the user did not request token hashing
	authStore, err := authorization.NewStore(ctx, store, cmd.useTokenHashing,
		authorization.WithAuthorizationHashVariantName(hashVariantName), authorization.WithIgnoreMissingHashIndex(ignoreMissingHashIndex))
	if err != nil {
		return err
	}
	auth := authorization.NewService(authStore, tenantService)

	if cmd.username == "" {
		return fmt.Errorf("must provide --username")
	}
	if cmd.org == "" {
		return fmt.Errorf("must provide --org")
	}

	// Find the user
	user, err := tenantService.FindUser(ctx, influxdb.UserFilter{Name: &cmd.username})
	if err != nil {
		return fmt.Errorf("could not find user %q: %w", cmd.username, err)
	}

	orgs, _, err := tenantService.FindOrganizations(ctx, influxdb.OrganizationFilter{
		Name: &cmd.org,
	})
	if err != nil {
		return fmt.Errorf("could not find org %q: %w", cmd.org, err)
	}
	org := orgs[0]

	// Create operator token
	authToCreate := &influxdb.Authorization{
		Description: fmt.Sprintf("%s's Recovery Token", cmd.username),
		Permissions: influxdb.OperPermissions(),
		UserID:      user.ID,
		OrgID:       org.ID,
	}
	if err := auth.CreateAuthorization(ctx, authToCreate); err != nil {
		return fmt.Errorf("could not create recovery token: %w", err)
	}

	// Print all authorizations now that we have added one
	filter := influxdb.AuthorizationFilter{}
	auths, _, err := auth.FindAuthorizations(ctx, filter)
	if err != nil {
		return err
	}
	return PrintAuth(ctx, cmd.out, auths, tenantService)
}

func PrintAuth(ctx context.Context, w io.Writer, v []*influxdb.Authorization, userSvc influxdb.UserService) error {
	headers := []string{
		"ID",
		"User Name",
		"User ID",
		"Description",
		"Token",
		"Permissions",
	}

	var rows []map[string]interface{}
	for _, t := range v {
		user, err := userSvc.FindUserByID(ctx, t.UserID)
		userName := ""
		if err == nil && user != nil {
			userName = user.Name
		}
		var token string
		if t.Token != "" {
			token = t.Token
		} else if t.HashedToken != "" {
			token = "REDACTED"
		} else {
			token = "N/A"
		}
		row := map[string]interface{}{
			"ID":          t.ID,
			"Description": t.Description,
			"User Name":   userName,
			"User ID":     t.UserID,
			"Token":       token,
			"Permissions": t.Permissions,
		}
		rows = append(rows, row)
	}

	writer := tabwriter.NewTabWriter(w, false)
	defer writer.Flush()
	if err := writer.WriteHeaders(headers...); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}
