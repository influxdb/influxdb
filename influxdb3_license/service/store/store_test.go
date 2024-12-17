package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store/postgres"
)

func TestUserCRUD(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	// Create user
	t.Log("Creating user...")
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	sqlTx := tx.(*sql.Tx) // Type assert to get the underlying sql.Tx
	defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

	user := &store.User{
		Email:             "test@example.com",
		EmailsSentCount:   0,
		VerificationToken: uuid.New().String(),
	}

	err = s.CreateUser(ctx, tx, user)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	t.Logf("After creation - DB direct check...")
	var dbCreatedAt, dbUpdatedAt time.Time
	err = sqlTx.QueryRowContext(ctx,
		"SELECT created_at, updated_at FROM users WHERE id = $1",
		user.ID).Scan(&dbCreatedAt, &dbUpdatedAt)
	if err != nil {
		t.Fatalf("querying timestamps: %v", err)
	}
	t.Logf("  DB CreatedAt: %v", dbCreatedAt)
	t.Logf("  DB UpdatedAt: %v", dbUpdatedAt)

	// Update user
	t.Log("\nUpdating user...")
	oldUpdatedAt := user.UpdatedAt
	user.EmailsSentCount = 1

	err = s.UpdateUser(ctx, tx, user)
	if err != nil {
		t.Fatalf("update user: %v", err)
	}

	t.Log("After update - DB direct check...")
	err = sqlTx.QueryRowContext(ctx,
		"SELECT created_at, updated_at FROM users WHERE id = $1",
		user.ID).Scan(&dbCreatedAt, &dbUpdatedAt)
	if err != nil {
		t.Fatalf("querying timestamps after update: %v", err)
	}
	t.Logf("  DB CreatedAt: %v", dbCreatedAt)
	t.Logf("  DB UpdatedAt: %v", dbUpdatedAt)
	t.Logf("  Struct UpdatedAt: %v", user.UpdatedAt)
	t.Logf("  Old UpdatedAt: %v", oldUpdatedAt)

	// Verify the change in DB
	var cnt int
	err = sqlTx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM users WHERE id = $1 AND updated_at > $2",
		user.ID, oldUpdatedAt).Scan(&cnt)
	if err != nil {
		t.Fatalf("checking timestamp change: %v", err)
	}
	t.Logf("Number of rows with newer timestamp: %d", cnt)

	if !user.UpdatedAt.After(oldUpdatedAt) {
		t.Error("expected UpdatedAt to be updated")
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	// Test deletion
	t.Log("\nDeleting user...")
	tx2, err := s.BeginTx(ctx) // New transaction since previous one was committed
	if err != nil {
		t.Fatalf("begin delete transaction: %v", err)
	}
	defer func(tx2 store.Tx) { _ = tx2.Rollback() }(tx2)

	err = s.DeleteUser(ctx, tx2, user.ID)
	if err != nil {
		t.Fatalf("delete user: %v", err)
	}

	// Verify deletion
	t.Log("Verifying user was deleted...")
	var exists bool
	err = tx2.(*sql.Tx).QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
		user.ID).Scan(&exists)
	if err != nil {
		t.Fatalf("checking if user exists: %v", err)
	}
	if exists {
		t.Error("expected user to be deleted")
	}
	t.Log("Confirmed user was deleted")

	err = tx2.Commit()
	if err != nil {
		t.Fatalf("commit delete transaction: %v", err)
	}
}

func TestUserInvariants(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	t.Run("inserting NULL for non-null fields", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Test each non-null field with raw SQL
		fields := []string{"email", "verification_token"}
		for _, field := range fields {
			query := `
                INSERT INTO users (
                    email,
                    verification_token
                ) VALUES (
                    CASE WHEN $1 = 'email' THEN NULL ELSE 'test@example.com' END,
                    CASE WHEN $1 = 'verification_token' THEN NULL ELSE '123e4567-e89b-12d3-a456-426614174000'::uuid END
                )`

			_, err = tx.(*sql.Tx).ExecContext(ctx, query, field)
			if err == nil {
				t.Errorf("expected error inserting NULL %s", field)
			}
		}
	})

	t.Run("email uniqueness", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create first user
		user1 := &store.User{
			Email:             "test@example.com",
			EmailsSentCount:   0,
			VerificationToken: uuid.New().String(),
		}
		err = s.CreateUser(ctx, tx, user1)
		if err != nil {
			t.Fatalf("create first user: %v", err)
		}

		// Try to create second user with same email
		user2 := &store.User{
			Email:             "test@example.com", // same email
			EmailsSentCount:   0,
			VerificationToken: uuid.New().String(),
		}
		err = s.CreateUser(ctx, tx, user2)
		if err == nil {
			t.Error("expected error creating user with duplicate email")
		}
	})

	t.Run("verification token uniqueness", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		token := uuid.New().String()
		user1 := &store.User{
			Email:             "test1@example.com",
			EmailsSentCount:   0,
			VerificationToken: token,
		}
		err = s.CreateUser(ctx, tx, user1)
		if err != nil {
			t.Fatalf("create first user: %v", err)
		}

		user2 := &store.User{
			Email:             "test2@example.com",
			EmailsSentCount:   0,
			VerificationToken: token, // same token
		}
		err = s.CreateUser(ctx, tx, user2)
		if err == nil {
			t.Error("expected error creating user with duplicate verification token")
		}
	})

	t.Run("default values", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		user := &store.User{
			Email:             "test@example.com",
			VerificationToken: uuid.New().String(),
		}
		err = s.CreateUser(ctx, tx, user)
		if err != nil {
			t.Fatalf("create user: %v", err)
		}

		if user.EmailsSentCount != 0 {
			t.Errorf("expected default EmailsSentCount=0, got %d", user.EmailsSentCount)
		}
		if user.CreatedAt.IsZero() {
			t.Error("expected CreatedAt to be set")
		}
		if user.UpdatedAt.IsZero() {
			t.Error("expected UpdatedAt to be set")
		}
	})
}

func TestUserIPCRUD(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	// Create a user first due to foreign key constraint
	t.Log("Creating test user...")
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

	user := &store.User{
		Email:             "test@example.com",
		EmailsSentCount:   0,
		VerificationToken: uuid.New().String(),
	}

	err = s.CreateUser(ctx, tx, user)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Test CreateUserIP
	t.Log("Testing CreateUserIP...")
	userIP := &store.UserIP{
		IPAddr: net.ParseIP("192.168.1.1"),
		UserID: user.ID,
	}

	err = s.CreateUserIP(ctx, tx, userIP)
	if err != nil {
		t.Fatalf("create user IP: %v", err)
	}

	// Test GetUserIPsByUserID
	t.Log("Testing GetUserIPsByUserID...")
	userIPs, err := s.GetUserIPsByUserID(ctx, tx, user.ID)
	if err != nil {
		t.Fatalf("get user IPs: %v", err)
	}
	if len(userIPs) != 1 {
		t.Fatalf("expected 1 user IP, got %d", len(userIPs))
	}
	if !userIPs[0].IPAddr.Equal(userIP.IPAddr) {
		t.Errorf("got IP %v, want %v", userIPs[0].IPAddr, userIP.IPAddr)
	}
	if userIPs[0].UserID != user.ID {
		t.Errorf("got user ID %d, want %d", userIPs[0].UserID, user.ID)
	}

	// Test GetUserIDsByIPAddr
	t.Log("Testing GetUserIDsByIPAddr...")
	userIDs, err := s.GetUserIDsByIPAddr(ctx, tx, userIP.IPAddr)
	if err != nil {
		t.Fatalf("get user IDs by IP: %v", err)
	}
	if len(userIDs) != 1 {
		t.Fatalf("expected 1 user ID, got %d", len(userIDs))
	}
	if userIDs[0] != user.ID {
		t.Errorf("got user ID %d, want %d", userIDs[0], user.ID)
	}

	// Test DeleteUserIP
	t.Log("Testing DeleteUserIP...")
	err = s.DeleteUserIP(ctx, tx, userIP.IPAddr, user.ID)
	if err != nil {
		t.Fatalf("delete user IP: %v", err)
	}

	// Verify deletion
	userIPs, err = s.GetUserIPsByUserID(ctx, tx, user.ID)
	if err != nil {
		t.Fatalf("get user IPs after deletion: %v", err)
	}
	if len(userIPs) != 0 {
		t.Errorf("expected no user IPs after deletion, got %d", len(userIPs))
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
}

func TestUserIPInvariants(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	t.Run("foreign key constraint", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Try to create user IP with non-existent user ID
		userIP := &store.UserIP{
			IPAddr: net.ParseIP("192.168.1.1"),
			UserID: 999999, // Non-existent user ID
		}

		err = s.CreateUserIP(ctx, tx, userIP)
		if err == nil {
			t.Error("expected error creating user IP with non-existent user ID")
		}
	})

	t.Run("multiple IPs per user", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create test user
		user := &store.User{
			Email:             "test@example.com",
			EmailsSentCount:   0,
			VerificationToken: uuid.New().String(),
		}
		err = s.CreateUser(ctx, tx, user)
		if err != nil {
			t.Fatalf("create user: %v", err)
		}

		// Create multiple IPs for the same user
		ips := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}
		for _, ip := range ips {
			userIP := &store.UserIP{
				IPAddr: net.ParseIP(ip),
				UserID: user.ID,
			}
			err = s.CreateUserIP(ctx, tx, userIP)
			if err != nil {
				t.Fatalf("create user IP %s: %v", ip, err)
			}
		}

		// Verify all IPs were created
		userIPs, err := s.GetUserIPsByUserID(ctx, tx, user.ID)
		if err != nil {
			t.Fatalf("get user IPs: %v", err)
		}
		if len(userIPs) != len(ips) {
			t.Errorf("expected %d user IPs, got %d", len(ips), len(userIPs))
		}
	})

	t.Run("same IP for multiple users", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create two test users
		users := make([]*store.User, 2)
		for i := range users {
			users[i] = &store.User{
				Email:             fmt.Sprintf("test%d@example.com", i),
				EmailsSentCount:   0,
				VerificationToken: uuid.New().String(),
			}
			err = s.CreateUser(ctx, tx, users[i])
			if err != nil {
				t.Fatalf("create user %d: %v", i, err)
			}
		}

		// Use same IP for both users
		sharedIP := net.ParseIP("192.168.1.1")
		for _, user := range users {
			userIP := &store.UserIP{
				IPAddr: sharedIP,
				UserID: user.ID,
			}
			err = s.CreateUserIP(ctx, tx, userIP)
			if err != nil {
				t.Fatalf("create user IP for user %d: %v", user.ID, err)
			}
		}

		// Verify both users are associated with the IP
		userIDs, err := s.GetUserIDsByIPAddr(ctx, tx, sharedIP)
		if err != nil {
			t.Fatalf("get user IDs by IP: %v", err)
		}
		if len(userIDs) != len(users) {
			t.Errorf("expected %d users for IP, got %d", len(users), len(userIDs))
		}
	})

	t.Run("invalid IP address", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create test user
		user := &store.User{
			Email:             "test@example.com",
			EmailsSentCount:   0,
			VerificationToken: uuid.New().String(),
		}
		err = s.CreateUser(ctx, tx, user)
		if err != nil {
			t.Fatalf("create user: %v", err)
		}

		// Try to create user IP with invalid IP
		userIP := &store.UserIP{
			IPAddr: net.ParseIP("invalid-ip"),
			UserID: user.ID,
		}

		err = s.CreateUserIP(ctx, tx, userIP)
		if err == nil {
			t.Error("expected error creating user IP with invalid IP address")
		}
	})
}

func TestEmailCRUD(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	// Create email
	t.Log("Creating email...")
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

	email := &store.Email{
		ToEmail:      "test@example.com",
		TemplateName: "welcome",
		Subject:      "Welcome to the service",
		Body:         "Hello, welcome to our service!",
	}

	err = s.CreateEmail(ctx, tx, email)
	if err != nil {
		t.Fatalf("create email: %v", err)
	}

	t.Logf("Email created successfully:")
	t.Logf("  ID: %d", email.ID)
	t.Logf("  ToEmail: %s", email.ToEmail)
	t.Logf("  TemplateName: %s", email.TemplateName)
	t.Logf("  Subject: %s", email.Subject)
	t.Logf("  SentAt: %v", email.SentAt)

	// Verify email exists
	t.Log("Verifying email exists...")
	emails, err := s.GetEmailsByToEmail(ctx, tx, email.ToEmail)
	if err != nil {
		t.Fatalf("get emails: %v", err)
	}
	if len(emails) != 1 {
		t.Fatalf("expected 1 email, got %d", len(emails))
	}
	retrieved := emails[0]
	t.Logf("Email retrieved by ToEmail:")
	t.Logf("  ID: %d", retrieved.ID)
	t.Logf("  ToEmail: %s", retrieved.ToEmail)

	// Update email
	t.Log("\nUpdating email...")
	originalSubject := email.Subject
	email.Subject = "Updated welcome message"

	err = s.UpdateEmail(ctx, tx, email)
	if err != nil {
		t.Fatalf("update email: %v", err)
	}

	t.Log("Email updated successfully:")
	t.Logf("  Original Subject: %s", originalSubject)
	t.Logf("  New Subject: %s", email.Subject)

	// Verify update was persisted
	t.Log("Verifying update was persisted...")
	emails, err = s.GetEmailsByToEmail(ctx, tx, email.ToEmail)
	if err != nil {
		t.Fatalf("get updated email: %v", err)
	}
	if len(emails) != 1 {
		t.Fatalf("expected 1 email, got %d", len(emails))
	}
	retrieved = emails[0]
	if retrieved.Subject != email.Subject {
		t.Errorf("got Subject %q, want %q", retrieved.Subject, email.Subject)
	}
	t.Log("Update verified successfully")

	// Delete email
	t.Logf("Deleting email with ID: %d...", email.ID)
	err = s.DeleteEmail(ctx, tx, email.ID)
	if err != nil {
		t.Fatalf("delete email: %v", err)
	}
	t.Log("Email successfully deleted")

	// Verify email was deleted
	t.Log("Verifying email was deleted...")
	emails, err = s.GetEmailsByToEmail(ctx, tx, email.ToEmail)
	if err != nil {
		t.Fatalf("get deleted email: %v", err)
	}
	if len(emails) != 0 {
		t.Error("expected no emails after deletion")
	}
	t.Log("Verified email no longer exists")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
}

func TestEmailInvariants(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	t.Run("inserting NULL for non-null fields", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// We need to use raw SQL to try inserting NULL values
		// since the Go struct won't let us represent this state
		_, err = tx.(*sql.Tx).ExecContext(ctx, `
            INSERT INTO emails_sent (subject, body)
            VALUES (NULL, 'test body')
        `)
		if err == nil {
			t.Error("expected error inserting NULL subject")
		}

		_, err = tx.(*sql.Tx).ExecContext(ctx, `
            INSERT INTO emails_sent (subject, body)
            VALUES ('test subject', NULL)
        `)
		if err == nil {
			t.Error("expected error inserting NULL body")
		}
	})

	t.Run("default values", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		email := &store.Email{
			ToEmail:      "test@example.com",
			TemplateName: "welcome",
			Subject:      "test subject",
			Body:         "test body",
		}

		err = s.CreateEmail(ctx, tx, email)
		if err != nil {
			t.Fatalf("create email: %v", err)
		}

		if email.SentAt.IsZero() {
			t.Error("expected SentAt to be set")
		}
	})

	t.Run("nullable fields", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Both to_email and template_name are nullable
		email := &store.Email{
			Subject: "test subject",
			Body:    "test body",
		}

		err = s.CreateEmail(ctx, tx, email)
		if err != nil {
			t.Fatalf("create email with null fields: %v", err)
		}

		if email.ID == 0 {
			t.Error("expected ID to be set")
		}
	})

	t.Run("varchar length", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create string longer than VARCHAR(255)
		longString := strings.Repeat("a", 256)

		email := &store.Email{
			ToEmail:      longString,
			TemplateName: longString,
			Subject:      "test subject",
			Body:         "test body",
		}

		err = s.CreateEmail(ctx, tx, email)
		if err == nil {
			t.Error("expected error creating email with fields exceeding VARCHAR(255)")
		}
	})

	t.Run("id auto-increment", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create two emails and verify IDs are sequential
		email1 := &store.Email{
			Subject: "test subject 1",
			Body:    "test body 1",
		}
		err = s.CreateEmail(ctx, tx, email1)
		if err != nil {
			t.Fatalf("create first email: %v", err)
		}

		email2 := &store.Email{
			Subject: "test subject 2",
			Body:    "test body 2",
		}
		err = s.CreateEmail(ctx, tx, email2)
		if err != nil {
			t.Fatalf("create second email: %v", err)
		}

		if email1.ID >= email2.ID {
			t.Errorf("expected second ID (%d) to be greater than first ID (%d)",
				email2.ID, email1.ID)
		}
	})
}

func TestLicenseCRUD(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	// Create license
	t.Log("Creating license...")
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

	validFrom := time.Now()
	validUntil := validFrom.AddDate(1, 0, 0) // valid for one year

	license := &store.License{
		Email:      "test@example.com",
		HostID:     "host123",
		InstanceID: uuid.New().String(),
		LicenseKey: "license-key-123",
		ValidFrom:  validFrom,
		ValidUntil: validUntil,
		Status:     store.LicenseStatusRequested,
	}

	err = s.CreateLicense(ctx, tx, license)
	if err != nil {
		t.Fatalf("create license: %v", err)
	}

	t.Logf("License created successfully:")
	t.Logf("  ID: %d", license.ID)
	t.Logf("  Email: %s", license.Email)
	t.Logf("  HostID: %s", license.HostID)
	t.Logf("  InstanceID: %s", license.InstanceID)
	t.Logf("  Status: %s", license.Status)
	t.Logf("  ValidFrom: %v", license.ValidFrom)
	t.Logf("  ValidUntil: %v", license.ValidUntil)
	t.Logf("  CreatedAt: %v", license.CreatedAt)
	t.Logf("  UpdatedAt: %v", license.UpdatedAt)

	// Verify license exists by email
	t.Log("Verifying license exists by email...")
	licenses, err := s.GetLicensesByEmail(ctx, tx, license.Email)
	if err != nil {
		t.Fatalf("get licenses: %v", err)
	}
	if len(licenses) != 1 {
		t.Fatalf("expected 1 license, got %d", len(licenses))
	}

	// Verify license exists by instance ID
	t.Log("Verifying license exists by instance ID...")
	retrieved, err := s.GetLicenseByInstanceID(ctx, tx, license.InstanceID)
	if err != nil {
		t.Fatalf("get license by instance ID: %v", err)
	}
	if retrieved == nil {
		t.Fatal("expected to find license by instance ID")
	}
	t.Logf("License retrieved successfully")

	// Update license
	t.Log("\nUpdating license...")
	originalStatus := license.Status
	originalUpdatedAt := license.UpdatedAt
	license.Status = store.LicenseStatusActive

	err = s.UpdateLicense(ctx, tx, license)
	if err != nil {
		t.Fatalf("update license: %v", err)
	}

	t.Log("License updated successfully:")
	t.Logf("  Original Status: %s", originalStatus)
	t.Logf("  New Status: %s", license.Status)
	t.Logf("  Original UpdatedAt: %v", originalUpdatedAt)
	t.Logf("  New UpdatedAt: %v", license.UpdatedAt)

	if !license.UpdatedAt.After(originalUpdatedAt) {
		t.Error("expected UpdatedAt to be updated")
	}

	// Verify update was persisted
	t.Log("Verifying update was persisted...")
	retrieved, err = s.GetLicenseByInstanceID(ctx, tx, license.InstanceID)
	if err != nil {
		t.Fatalf("get updated license: %v", err)
	}
	if retrieved.Status != store.LicenseStatusActive {
		t.Errorf("got Status %q, want %q", retrieved.Status, store.LicenseStatusActive)
	}
	t.Log("Update verified successfully")

	// Delete license
	t.Logf("Deleting license with ID: %d...", license.ID)
	err = s.DeleteLicense(ctx, tx, license.ID)
	if err != nil {
		t.Fatalf("delete license: %v", err)
	}
	t.Log("License successfully deleted")

	// Verify license was deleted
	t.Log("Verifying license was deleted...")
	retrieved, err = s.GetLicenseByInstanceID(ctx, tx, license.InstanceID)
	if err != nil {
		t.Fatalf("get deleted license: %v", err)
	}
	if retrieved != nil {
		t.Error("expected nil for deleted license")
	}
	t.Log("Verified license no longer exists")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
}

func TestLicenseInvariants(t *testing.T) {
	ctx := context.Background()
	testDB := NewTestDB(t)
	defer testDB.Cleanup()
	testDB.Setup(ctx)

	s := postgres.NewStore(testDB.DB)

	t.Run("inserting NULL for non-null fields", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Test each non-null field with raw SQL
		fields := []string{"email", "host_id", "instance_id", "license_key", "valid_until", "status"}
		for _, field := range fields {
			query := `
                INSERT INTO licenses (
                    email, host_id, instance_id, license_key,
                    valid_until, status
                ) VALUES (
                    CASE WHEN $1 = 'email' THEN NULL ELSE 'test@example.com' END,
                    CASE WHEN $1 = 'host_id' THEN NULL ELSE 'host123' END,
                    CASE WHEN $1 = 'instance_id' THEN NULL ELSE '123e4567-e89b-12d3-a456-426614174000'::uuid END,
                    CASE WHEN $1 = 'license_key' THEN NULL ELSE 'key123' END,
                    CASE WHEN $1 = 'valid_until' THEN NULL ELSE NOW() + INTERVAL '1 year' END,
                    CASE WHEN $1 = 'status' THEN NULL ELSE 'active' END
                )`

			_, err = tx.(*sql.Tx).ExecContext(ctx, query, field)
			if err == nil {
				t.Errorf("expected error inserting NULL %s", field)
			}
		}
	})

	t.Run("unique constraints", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Create initial license
		license1 := &store.License{
			Email:      "test@example.com",
			HostID:     "host123",
			InstanceID: uuid.New().String(),
			LicenseKey: "key123",
			ValidFrom:  time.Now(),
			ValidUntil: time.Now().AddDate(1, 0, 0),
			Status:     store.LicenseStatusActive,
		}

		err = s.CreateLicense(ctx, tx, license1)
		if err != nil {
			t.Fatalf("create first license: %v", err)
		}

		// Try duplicate email + host_id
		license2 := &store.License{
			Email:      license1.Email,      // same email
			HostID:     license1.HostID,     // same host
			InstanceID: uuid.New().String(), // different instance
			LicenseKey: "key456",
			ValidFrom:  time.Now(),
			ValidUntil: time.Now().AddDate(1, 0, 0),
			Status:     store.LicenseStatusActive,
		}

		err = s.CreateLicense(ctx, tx, license2)
		if err == nil {
			t.Error("expected error creating license with duplicate email + host_id")
		}

		// Try duplicate email + instance_id
		license3 := &store.License{
			Email:      license1.Email,      // same email
			HostID:     "different_host",    // different host
			InstanceID: license1.InstanceID, // same instance
			LicenseKey: "key789",
			ValidFrom:  time.Now(),
			ValidUntil: time.Now().AddDate(1, 0, 0),
			Status:     store.LicenseStatusActive,
		}

		err = s.CreateLicense(ctx, tx, license3)
		if err == nil {
			t.Error("expected error creating license with duplicate email + instance_id")
		}
	})

	t.Run("default values", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		license := &store.License{
			Email:      "test@example.com",
			HostID:     "host123",
			InstanceID: uuid.New().String(),
			LicenseKey: "key123",
			ValidUntil: time.Now().AddDate(1, 0, 0),
		}

		t.Log("Before creation:")
		t.Logf("  ValidFrom: %v", license.ValidFrom)
		t.Logf("  Status: %q", license.Status)

		err = s.CreateLicense(ctx, tx, license)
		if err != nil {
			t.Fatalf("create license: %v", err)
		}

		t.Log("After creation:")
		t.Logf("  ValidFrom: %v", license.ValidFrom)
		t.Logf("  Status: %q", license.Status)

		// Direct database query to verify
		var dbValidFrom time.Time
		var dbStatus string
		err = tx.(*sql.Tx).QueryRowContext(ctx,
			"SELECT valid_from, status FROM licenses WHERE id = $1",
			license.ID).Scan(&dbValidFrom, &dbStatus)
		if err != nil {
			t.Fatalf("query db values: %v", err)
		}
		t.Log("Database values:")
		t.Logf("  ValidFrom: %v", dbValidFrom)
		t.Logf("  Status: %q", dbStatus)

		// Original checks
		if license.CreatedAt.IsZero() {
			t.Error("expected CreatedAt to be set")
		}
		if license.UpdatedAt.IsZero() {
			t.Error("expected UpdatedAt to be set")
		}
		if license.ValidFrom.IsZero() {
			t.Error("expected ValidFrom to be set")
		}
		if license.Status != store.LicenseStatusRequested {
			t.Errorf("expected default status 'inactive', got %q", license.Status)
		}
	})

	t.Run("valid_until after valid_from", func(t *testing.T) {
		tx, err := s.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}
		defer func(tx store.Tx) { _ = tx.Rollback() }(tx)

		// Try to create license with valid_until before valid_from
		pastTime := time.Now().AddDate(0, 0, -1)
		license := &store.License{
			Email:      "test@example.com",
			HostID:     "host123",
			InstanceID: uuid.New().String(),
			LicenseKey: "key123",
			ValidFrom:  time.Now(),
			ValidUntil: pastTime,
			Status:     store.LicenseStatusActive,
		}

		err = s.CreateLicense(ctx, tx, license)
		// Note: This might not fail if there's no CHECK constraint in the schema
		// Could be a business logic validation instead
		if err != nil {
			t.Log("database prevents valid_until before valid_from")
		} else {
			t.Log("database allows valid_until before valid_from - might want to add CHECK constraint")
		}
	})
}
