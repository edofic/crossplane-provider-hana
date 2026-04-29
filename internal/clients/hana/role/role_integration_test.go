//go:build integration

package role

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/SAP/go-hdb/driver"

	"github.com/SAP/crossplane-provider-hana/apis/admin/v1alpha1"
	"github.com/SAP/crossplane-provider-hana/internal/clients/hana"
	"github.com/SAP/crossplane-provider-hana/internal/clients/xsql"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
)

// connectHANA reads HANA_BINDINGS and returns a live DB connection + username.
func connectHANA(t *testing.T) (xsql.DB, string) {
	t.Helper()
	bindings := os.Getenv("HANA_BINDINGS")
	if bindings == "" {
		t.Skip("HANA_BINDINGS not set, skipping integration test")
	}

	var creds map[string]string
	if err := json.Unmarshal([]byte(bindings), &creds); err != nil {
		t.Fatalf("failed to parse HANA_BINDINGS: %v", err)
	}

	credsBytes := map[string][]byte{
		"endpoint": []byte(creds["endpoint"]),
		"port":     []byte(creds["port"]),
		"username": []byte(creds["username"]),
		"password": []byte(creds["password"]),
	}

	logger := logging.NewNopLogger()
	connector := hana.New(logger)
	db, err := connector.Connect(context.Background(), credsBytes)
	if err != nil {
		t.Fatalf("failed to connect to HANA: %v", err)
	}

	return db, creds["username"]
}

func cleanup(t *testing.T, db xsql.DB, names ...string) {
	t.Helper()
	for _, name := range names {
		db.ExecContext(context.Background(), fmt.Sprintf(`DROP ROLE "%s"`, name))       //nolint:errcheck
		db.ExecContext(context.Background(), fmt.Sprintf(`DROP ROLEGROUP "%s"`, name)) //nolint:errcheck
	}
}

func queryRolegroupForRole(t *testing.T, db xsql.DB, roleName string) string {
	t.Helper()
	var rg sql.NullString
	row := db.QueryRowContext(context.Background(),
		"SELECT ROLEGROUP_NAME FROM SYS.ROLES WHERE ROLE_NAME = ?", roleName)
	if err := row.Scan(&rg); err != nil {
		t.Fatalf("failed to query rolegroup for role %q: %v", roleName, err)
	}
	return rg.String
}

func TestIntegration_RoleWithRolegroup(t *testing.T) {
	db, username := connectHANA(t)
	ctx := context.Background()

	const roleName = "INTTEST_ROLE_RG"
	const rolegroupName = "INTTEST_ROLEGROUP"

	// Cleanup before and after
	cleanup(t, db, roleName, rolegroupName)
	t.Cleanup(func() { cleanup(t, db, roleName, rolegroupName) })

	// Create the rolegroup directly in DB
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE ROLEGROUP "%s"`, rolegroupName)); err != nil {
		t.Fatalf("failed to create rolegroup: %v", err)
	}

	client := New(db, username)

	// Test 1: CREATE ROLE with SET ROLEGROUP
	t.Run("CreateWithRolegroup", func(t *testing.T) {
		params := &v1alpha1.RoleParameters{
			RoleName:  roleName,
			Rolegroup: rolegroupName,
		}
		if err := client.Create(ctx, params); err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		got := queryRolegroupForRole(t, db, roleName)
		if got != rolegroupName {
			t.Errorf("after Create: expected rolegroup %q, got %q", rolegroupName, got)
		}
	})

	// Test 2: Read should return the rolegroup
	t.Run("ReadReturnsRolegroup", func(t *testing.T) {
		params := &v1alpha1.RoleParameters{RoleName: roleName}
		obs, err := client.Read(ctx, params)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if obs.Rolegroup != rolegroupName {
			t.Errorf("Read: expected rolegroup %q, got %q", rolegroupName, obs.Rolegroup)
		}
	})

	// Test 3: UNSET ROLEGROUP via UpdateRolegroup
	t.Run("UnsetRolegroup", func(t *testing.T) {
		params := &v1alpha1.RoleParameters{
			RoleName:  roleName,
			Rolegroup: "",
		}
		if err := client.UpdateRolegroup(ctx, params); err != nil {
			t.Fatalf("UpdateRolegroup (unset) failed: %v", err)
		}

		got := queryRolegroupForRole(t, db, roleName)
		if got != "" {
			t.Errorf("after unset: expected empty rolegroup, got %q", got)
		}
	})

	// Test 4: SET ROLEGROUP via UpdateRolegroup
	t.Run("SetRolegroup", func(t *testing.T) {
		params := &v1alpha1.RoleParameters{
			RoleName:  roleName,
			Rolegroup: rolegroupName,
		}
		if err := client.UpdateRolegroup(ctx, params); err != nil {
			t.Fatalf("UpdateRolegroup (set) failed: %v", err)
		}

		got := queryRolegroupForRole(t, db, roleName)
		if got != rolegroupName {
			t.Errorf("after set: expected rolegroup %q, got %q", rolegroupName, got)
		}
	})

	// Test 5: Read after re-assignment
	t.Run("ReadAfterReassignment", func(t *testing.T) {
		params := &v1alpha1.RoleParameters{RoleName: roleName}
		obs, err := client.Read(ctx, params)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if obs.Rolegroup != rolegroupName {
			t.Errorf("Read after reassignment: expected rolegroup %q, got %q", rolegroupName, obs.Rolegroup)
		}
	})
}

func TestIntegration_RoleWithoutRolegroup(t *testing.T) {
	db, username := connectHANA(t)
	ctx := context.Background()

	const roleName = "INTTEST_ROLE_NORG"

	cleanup(t, db, roleName)
	t.Cleanup(func() { cleanup(t, db, roleName) })

	client := New(db, username)

	// Create role without rolegroup
	params := &v1alpha1.RoleParameters{RoleName: roleName}
	if err := client.Create(ctx, params); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Read should show empty rolegroup
	obs, err := client.Read(ctx, params)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if obs.Rolegroup != "" {
		t.Errorf("expected empty rolegroup, got %q", obs.Rolegroup)
	}
	if obs.RoleName != roleName {
		t.Errorf("expected roleName %q, got %q", roleName, obs.RoleName)
	}
}
