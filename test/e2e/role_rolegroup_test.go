//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/SAP/crossplane-provider-hana/apis/admin/v1alpha1"
	"github.com/SAP/crossplane-provider-hana/internal/clients/hana"
	"github.com/SAP/crossplane-provider-hana/internal/clients/xsql"
	"github.com/crossplane-contrib/xp-testing/pkg/xpconditions"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/logging"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sresources "sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"sigs.k8s.io/e2e-framework/klient/k8s"
)

// RoleRolegroupTestConfig holds configuration and state for role-rolegroup e2e tests.
type RoleRolegroupTestConfig struct {
	Resource       *k8sresources.Resources
	RolegroupNames []string
	RoleNames      []string
	db             xsql.Connector
	conn           xsql.DB
}

func (c *RoleRolegroupTestConfig) connectDB(ctx context.Context, t *testing.T) {
	secretData := getProviderConfigSecretData()
	secretDataBytes := make(map[string][]byte)
	for k, v := range secretData {
		secretDataBytes[k] = []byte(v)
	}
	conn, err := c.db.Connect(ctx, secretDataBytes)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	c.conn = conn
}

func (c *RoleRolegroupTestConfig) createRoleCR(ctx context.Context, t *testing.T, name string, spec v1alpha1.RoleParameters) *v1alpha1.Role {
	role := &v1alpha1.Role{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admin.hana.sap.crossplane.io/v1alpha1",
			Kind:       "Role",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.RoleSpec{
			ResourceSpec: xpv1.ResourceSpec{
				DeletionPolicy: xpv1.DeletionDelete,
				ProviderConfigReference: &xpv1.Reference{
					Name: "example",
				},
			},
			ForProvider: spec,
		},
	}

	if err := c.Resource.Create(ctx, role); err != nil {
		t.Fatalf("failed to create Role CR %q: %v", name, err)
	}

	c.RoleNames = append(c.RoleNames, spec.RoleName)
	t.Logf("Created Role CR %q (roleName=%q, rolegroup=%q)", name, spec.RoleName, spec.Rolegroup)
	return role
}

func (c *RoleRolegroupTestConfig) createRolegroupCR(ctx context.Context, t *testing.T, name string, spec v1alpha1.RolegroupParameters) *v1alpha1.Rolegroup {
	rg := &v1alpha1.Rolegroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admin.hana.sap.crossplane.io/v1alpha1",
			Kind:       "Rolegroup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.RolegroupSpec{
			ResourceSpec: xpv1.ResourceSpec{
				DeletionPolicy: xpv1.DeletionDelete,
				ProviderConfigReference: &xpv1.Reference{
					Name: "example",
				},
			},
			ForProvider: spec,
		},
	}

	if err := c.Resource.Create(ctx, rg); err != nil {
		t.Fatalf("failed to create Rolegroup CR %q: %v", name, err)
	}

	c.RolegroupNames = append(c.RolegroupNames, spec.RolegroupName)
	t.Logf("Created Rolegroup CR %q (rolegroupName=%q)", name, spec.RolegroupName)
	return rg
}

func (c *RoleRolegroupTestConfig) waitForRoleReady(ctx context.Context, t *testing.T, cfg *envconf.Config, role *v1alpha1.Role) {
	res := cfg.Client().Resources()
	xpc := xpconditions.New(res)
	err := wait.For(
		conditions.New(res).ResourceMatch(role, xpc.IsManagedResourceReadyAndReady),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("role %q did not become ready: %v", role.Name, err)
		out, _ := exec.Command("kubectl", "describe", "role.admin.hana.sap.crossplane.io", role.Name).CombinedOutput()
		t.Logf("kubectl describe:\n%s", string(out))
	} else {
		t.Logf("Role %q is Ready and Synced", role.Name)
	}
}

func (c *RoleRolegroupTestConfig) waitForRolegroupReady(ctx context.Context, t *testing.T, cfg *envconf.Config, rg *v1alpha1.Rolegroup) {
	res := cfg.Client().Resources()
	xpc := xpconditions.New(res)
	err := wait.For(
		conditions.New(res).ResourceMatch(rg, xpc.IsManagedResourceReadyAndReady),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("rolegroup %q did not become ready: %v", rg.Name, err)
	} else {
		t.Logf("Rolegroup %q is Ready and Synced", rg.Name)
	}
}

func (c *RoleRolegroupTestConfig) verifyRoleRolegroupInDB(ctx context.Context, t *testing.T, roleName, expectedRolegroup string) {
	var rolegroupName *string
	row := c.conn.QueryRowContext(ctx, "SELECT ROLEGROUP_NAME FROM SYS.ROLES WHERE ROLE_NAME = ?", roleName)
	if err := row.Scan(&rolegroupName); err != nil {
		t.Errorf("failed to query role %q: %v", roleName, err)
		return
	}

	actual := ""
	if rolegroupName != nil {
		actual = *rolegroupName
	}

	if actual != expectedRolegroup {
		t.Errorf("expected role %q to have rolegroup %q, got %q", roleName, expectedRolegroup, actual)
	} else {
		t.Logf("Verified role %q has rolegroup %q in HANA DB", roleName, expectedRolegroup)
	}
}

func (c *RoleRolegroupTestConfig) deleteRoleCR(ctx context.Context, t *testing.T, cfg *envconf.Config, role *v1alpha1.Role) {
	res := cfg.Client().Resources()
	if err := res.Delete(ctx, role); err != nil {
		t.Errorf("failed to delete role CR %q: %v", role.Name, err)
		return
	}
	err := wait.For(
		conditions.New(res).ResourceDeleted(role),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("role CR %q was not deleted in time: %v", role.Name, err)
	} else {
		t.Logf("Role CR %q deleted", role.Name)
	}
}

func (c *RoleRolegroupTestConfig) deleteRolegroupCR(ctx context.Context, t *testing.T, cfg *envconf.Config, rg *v1alpha1.Rolegroup) {
	res := cfg.Client().Resources()
	if err := res.Delete(ctx, rg); err != nil {
		t.Errorf("failed to delete rolegroup CR %q: %v", rg.Name, err)
		return
	}
	err := wait.For(
		conditions.New(res).ResourceDeleted(rg),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("rolegroup CR %q was not deleted in time: %v", rg.Name, err)
	} else {
		t.Logf("Rolegroup CR %q deleted", rg.Name)
	}
}

// --------------------------------------------------------------------------
// Test: Role with rolegroup assignment (create, verify, update, unset, delete)
// --------------------------------------------------------------------------

func TestRoleRolegroupAssignment(t *testing.T) {
	logger := logging.NewNopLogger()
	rolegroupName := "E2ETESTRGROLEASSIGN"
	roleName := "E2ETESTROLEWITHRG"
	rgCRName := "e2e-rg-for-role"
	roleCRName := "e2e-role-with-rolegroup"

	c := &RoleRolegroupTestConfig{
		db: hana.New(logger),
	}

	fB := features.New("RoleRolegroupAssignment")
	fB.WithLabel("kind", "Role")

	fB.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.connectDB(ctx, t)
		var err error
		if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
			t.Fatalf("failed to create resource client: %v", err)
		}
		return ctx
	})

	// Step 1: Create the rolegroup first
	fB.Assess("create-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.createRolegroupCR(ctx, t, rgCRName, v1alpha1.RolegroupParameters{
			RolegroupName: rolegroupName,
		})
		c.waitForRolegroupReady(ctx, t, cfg, rg)
		return ctx
	})

	// Step 2: Create role with rolegroup set
	fB.Assess("create-role-with-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		role := c.createRoleCR(ctx, t, roleCRName, v1alpha1.RoleParameters{
			RoleName:  roleName,
			Rolegroup: rolegroupName,
		})
		c.waitForRoleReady(ctx, t, cfg, role)
		return ctx
	})

	// Step 3: Verify role is assigned to the rolegroup in DB
	fB.Assess("verify-role-in-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRoleRolegroupInDB(ctx, t, roleName, rolegroupName)
		return ctx
	})

	// Step 4: Update role to unset rolegroup
	fB.Assess("unset-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		role := &v1alpha1.Role{}
		if err := c.Resource.Get(ctx, roleCRName, "", role); err != nil {
			t.Fatalf("failed to get role CR: %v", err)
		}
		role.Spec.ForProvider.Rolegroup = ""
		if err := c.Resource.Update(ctx, role); err != nil {
			t.Fatalf("failed to update role CR: %v", err)
		}

		// Wait for status to reflect the change
		res := cfg.Client().Resources()
		err := wait.For(
			conditions.New(res).ResourceMatch(role, func(obj k8s.Object) bool {
				r, ok := obj.(*v1alpha1.Role)
				if !ok {
					return false
				}
				return r.Status.AtProvider.Rolegroup == ""
			}),
			wait.WithTimeout(2*time.Minute),
		)
		if err != nil {
			t.Errorf("role %q status did not update after unsetting rolegroup: %v", roleCRName, err)
		}
		return ctx
	})

	// Step 5: Verify role no longer in a rolegroup
	fB.Assess("verify-rolegroup-unset", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRoleRolegroupInDB(ctx, t, roleName, "")
		return ctx
	})

	// Step 6: Re-assign the role to the rolegroup
	fB.Assess("reassign-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		role := &v1alpha1.Role{}
		if err := c.Resource.Get(ctx, roleCRName, "", role); err != nil {
			t.Fatalf("failed to get role CR: %v", err)
		}
		role.Spec.ForProvider.Rolegroup = rolegroupName
		if err := c.Resource.Update(ctx, role); err != nil {
			t.Fatalf("failed to update role CR: %v", err)
		}

		// Wait for status to reflect the change
		res := cfg.Client().Resources()
		err := wait.For(
			conditions.New(res).ResourceMatch(role, func(obj k8s.Object) bool {
				r, ok := obj.(*v1alpha1.Role)
				if !ok {
					return false
				}
				return r.Status.AtProvider.Rolegroup == rolegroupName
			}),
			wait.WithTimeout(2*time.Minute),
		)
		if err != nil {
			t.Errorf("role %q status did not update after reassigning rolegroup: %v", roleCRName, err)
		}
		return ctx
	})

	// Step 7: Verify role is back in the rolegroup
	fB.Assess("verify-rolegroup-reassigned", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRoleRolegroupInDB(ctx, t, roleName, rolegroupName)
		return ctx
	})

	// Cleanup: delete role first (must unset rolegroup before dropping rolegroup)
	fB.Assess("cleanup-role", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		role := &v1alpha1.Role{}
		if err := c.Resource.Get(ctx, roleCRName, "", role); err != nil {
			t.Fatalf("failed to get role CR: %v", err)
		}
		c.deleteRoleCR(ctx, t, cfg, role)
		return ctx
	})

	fB.Assess("cleanup-rolegroup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := &v1alpha1.Rolegroup{}
		if err := c.Resource.Get(ctx, rgCRName, "", rg); err != nil {
			t.Fatalf("failed to get rolegroup CR: %v", err)
		}
		c.deleteRolegroupCR(ctx, t, cfg, rg)
		return ctx
	})

	fB.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		for _, name := range c.RoleNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLE "%s"`, name))
		}
		for _, name := range c.RolegroupNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
		}
		_ = c.db.Disconnect()
		return ctx
	})

	testenv.Test(t, fB.Feature())
}
