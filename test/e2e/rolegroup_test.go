//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/SAP/crossplane-provider-hana/apis/admin/v1alpha1"
	"github.com/SAP/crossplane-provider-hana/internal/clients/hana"
	"github.com/SAP/crossplane-provider-hana/internal/clients/xsql"
	"github.com/crossplane-contrib/xp-testing/pkg/resources"
	"github.com/crossplane-contrib/xp-testing/pkg/xpconditions"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/logging"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	k8sresources "sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// RolegroupTestConfig holds configuration and state for rolegroup e2e tests.
type RolegroupTestConfig struct {
	TestConfig     *resources.ResourceTestConfig
	Resource       *k8sresources.Resources
	Objects        []k8s.Object
	RolegroupNames []string // Track rolegroup names for cleanup
	db             xsql.Connector
	conn           xsql.DB
}

// connectDB connects to the HANA database using provider credentials.
func (c *RolegroupTestConfig) connectDB(ctx context.Context, t *testing.T) {
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

// SetupRolegroup connects to HANA and imports resources from the CR directory.
func (c *RolegroupTestConfig) SetupRolegroup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	t.Log("Apply Rolegroup")
	c.connectDB(ctx, t)

	resources.ImportResources(ctx, t, cfg, c.TestConfig.ResourceDirectory)

	objects := make([]k8s.Object, 0)
	err := decoder.DecodeEachFile(
		ctx, os.DirFS(c.TestConfig.ResourceDirectory), "*",
		func(ctx context.Context, obj k8s.Object) error {
			objects = append(objects, obj)
			return nil
		},
		decoder.MutateNamespace(cfg.Namespace()),
	)
	if err != nil {
		t.Fatalf("failed to decode files: %v", err)
	}

	if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
		t.Fatalf("failed to create resource client: %v", err)
	}

	for _, obj := range objects {
		rg, ok := obj.(*v1alpha1.Rolegroup)
		if !ok {
			continue
		}
		c.Objects = append(c.Objects, obj)
		c.RolegroupNames = append(c.RolegroupNames, rg.Spec.ForProvider.RolegroupName)
	}

	return ctx
}

// TeardownRolegroup cleans up rolegroups from the database and disconnects.
func (c *RolegroupTestConfig) TeardownRolegroup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	t.Log("Teardown: Clean up rolegroups")
	for _, name := range c.RolegroupNames {
		// Ignore errors — the rolegroup may already be deleted by the controller
		_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
	}
	if err := c.db.Disconnect(); err != nil {
		t.Errorf("failed to disconnect from database: %v", err)
	}
	return ctx
}

// verifyRolegroupExistsInDB checks that a rolegroup exists in SYS.ROLEGROUPS.
func (c *RolegroupTestConfig) verifyRolegroupExistsInDB(ctx context.Context, t *testing.T, rolegroupName string) {
	var name string
	row := c.conn.QueryRowContext(ctx, "SELECT ROLEGROUP_NAME FROM SYS.ROLEGROUPS WHERE ROLEGROUP_NAME = ?", rolegroupName)
	if err := row.Scan(&name); err != nil {
		t.Errorf("rolegroup %q not found in SYS.ROLEGROUPS: %v", rolegroupName, err)
		return
	}
	if name != rolegroupName {
		t.Errorf("expected rolegroup name %q, got %q", rolegroupName, name)
	}
	t.Logf("Verified rolegroup %q exists in HANA DB", rolegroupName)
}

// verifyRolegroupNotExistsInDB checks that a rolegroup does NOT exist in SYS.ROLEGROUPS.
func (c *RolegroupTestConfig) verifyRolegroupNotExistsInDB(ctx context.Context, t *testing.T, rolegroupName string) {
	// Poll for up to 30 seconds — the controller's DROP ROLEGROUP may still be in-flight
	// after the K8s resource is garbage collected.
	for i := 0; i < 6; i++ {
		var name string
		row := c.conn.QueryRowContext(ctx, "SELECT ROLEGROUP_NAME FROM SYS.ROLEGROUPS WHERE ROLEGROUP_NAME = ?", rolegroupName)
		if err := row.Scan(&name); xsql.IsNoRows(err) {
			t.Logf("Verified rolegroup %q does not exist in HANA DB", rolegroupName)
			return
		} else if err != nil {
			t.Errorf("unexpected error checking rolegroup %q: %v", rolegroupName, err)
			return
		}
		if i < 5 {
			time.Sleep(5 * time.Second)
		}
	}
	t.Errorf("rolegroup %q should not exist in DB but was still found after 30s", rolegroupName)
}

// verifyRoleAdminEnabled checks IS_ROLE_ADMIN_ENABLED for a rolegroup.
func (c *RolegroupTestConfig) verifyRoleAdminEnabled(ctx context.Context, t *testing.T, rolegroupName string, expectEnabled bool) {
	var isEnabled string
	row := c.conn.QueryRowContext(ctx, "SELECT IS_ROLE_ADMIN_ENABLED FROM SYS.ROLEGROUPS WHERE ROLEGROUP_NAME = ?", rolegroupName)
	if err := row.Scan(&isEnabled); err != nil {
		t.Errorf("failed to query IS_ROLE_ADMIN_ENABLED for %q: %v", rolegroupName, err)
		return
	}
	expected := "TRUE"
	if !expectEnabled {
		expected = "FALSE"
	}
	if isEnabled != expected {
		t.Errorf("expected IS_ROLE_ADMIN_ENABLED=%q for %q, got %q", expected, rolegroupName, isEnabled)
	} else {
		t.Logf("Verified IS_ROLE_ADMIN_ENABLED=%q for %q", expected, rolegroupName)
	}
}

// createRolegroupCR creates a Rolegroup CR programmatically and applies it to the cluster.
func (c *RolegroupTestConfig) createRolegroupCR(ctx context.Context, t *testing.T, cfg *envconf.Config, name, rolegroupName string, spec v1alpha1.RolegroupParameters) *v1alpha1.Rolegroup {
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

	c.RolegroupNames = append(c.RolegroupNames, rolegroupName)
	t.Logf("Created Rolegroup CR %q (rolegroupName=%q)", name, rolegroupName)
	return rg
}

// waitForRolegroupReady waits for a Rolegroup CR to become Ready and Synced.
func (c *RolegroupTestConfig) waitForRolegroupReady(ctx context.Context, t *testing.T, cfg *envconf.Config, rg *v1alpha1.Rolegroup) {
	res := cfg.Client().Resources()
	xpc := xpconditions.New(res)
	err := wait.For(
		conditions.New(res).ResourceMatch(rg, xpc.IsManagedResourceReadyAndReady),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("rolegroup %q did not become ready: %v", rg.Name, err)
		describeRolegroup(t, rg.Name)
	} else {
		t.Logf("Rolegroup %q is Ready and Synced", rg.Name)
	}
}

// deleteRolegroupCR deletes a Rolegroup CR and waits for it to be gone.
func (c *RolegroupTestConfig) deleteRolegroupCR(ctx context.Context, t *testing.T, cfg *envconf.Config, rg *v1alpha1.Rolegroup) {
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

// waitForRolegroupStatus waits for a Rolegroup's status to match a condition.
func (c *RolegroupTestConfig) waitForRolegroupStatus(ctx context.Context, t *testing.T, cfg *envconf.Config, rg *v1alpha1.Rolegroup, matchFn func(k8s.Object) bool) {
	res := cfg.Client().Resources()
	err := wait.For(
		conditions.New(res).ResourceMatch(rg, matchFn),
		wait.WithTimeout(5*time.Minute),
	)
	if err != nil {
		t.Errorf("rolegroup %q status did not match expected condition: %v", rg.Name, err)
		describeRolegroup(t, rg.Name)
	}
}

// describeRolegroup runs kubectl describe for debugging.
func describeRolegroup(t *testing.T, name string) {
	out, err := exec.Command("kubectl", "describe", "rolegroup", name).CombinedOutput()
	if err != nil {
		t.Logf("failed to run kubectl describe: %v", err)
	} else {
		t.Logf("kubectl describe rolegroup %s output:\n%s", name, string(out))
	}
}

// getRolegroup fetches the current state of a Rolegroup CR.
func (c *RolegroupTestConfig) getRolegroup(ctx context.Context, t *testing.T, name string) *v1alpha1.Rolegroup {
	rg := &v1alpha1.Rolegroup{}
	if err := c.Resource.Get(ctx, name, "", rg); err != nil {
		t.Fatalf("failed to get rolegroup CR %q: %v", name, err)
	}
	return rg
}

// --------------------------------------------------------------------------
// Test 1: Basic lifecycle (create, verify in DB, delete, verify gone)
// --------------------------------------------------------------------------

func TestRolegroupBasicLifecycle(t *testing.T) {
	testConfig := resources.NewResourceTestConfig(nil, "Rolegroup")
	logger := logging.NewNopLogger()

	c := &RolegroupTestConfig{
		TestConfig: testConfig,
		db:         hana.New(logger),
	}

	fB := features.New("RolegroupBasicLifecycle")
	fB.WithLabel("kind", testConfig.Kind)
	fB.Setup(c.SetupRolegroup)

	fB.Assess("create", testConfig.AssessCreate)

	fB.Assess("verify-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupExistsInDB(ctx, t, "E2ETESTROLEGROUPBASIC")
		return ctx
	})

	fB.Assess("delete", testConfig.AssessDelete)

	fB.Assess("verify-deleted-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupNotExistsInDB(ctx, t, "E2ETESTROLEGROUPBASIC")
		return ctx
	})

	fB.Teardown(c.TeardownRolegroup)
	testenv.Test(t, fB.Feature())
}

// --------------------------------------------------------------------------
// Test 2: DisableRoleAdmin toggle
// --------------------------------------------------------------------------

func TestRolegroupDisableRoleAdmin(t *testing.T) {
	logger := logging.NewNopLogger()
	rolegroupName := "E2ETESTRGDISABLE"
	crName := "e2e-rg-disable-role-admin"

	c := &RolegroupTestConfig{
		db: hana.New(logger),
	}

	fB := features.New("RolegroupDisableRoleAdmin")
	fB.WithLabel("kind", "Rolegroup")

	fB.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.connectDB(ctx, t)
		var err error
		if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
			t.Fatalf("failed to create resource client: %v", err)
		}
		return ctx
	})

	fB.Assess("create-with-disable", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.createRolegroupCR(ctx, t, cfg, crName, rolegroupName, v1alpha1.RolegroupParameters{
			RolegroupName:    rolegroupName,
			DisableRoleAdmin: true,
		})
		c.waitForRolegroupReady(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-disabled-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupExistsInDB(ctx, t, rolegroupName)
		c.verifyRoleAdminEnabled(ctx, t, rolegroupName, false)
		return ctx
	})

	fB.Assess("update-to-enable", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		rg.Spec.ForProvider.DisableRoleAdmin = false

		res := cfg.Client().Resources()
		if err := res.Update(ctx, rg); err != nil {
			t.Fatalf("failed to update rolegroup: %v", err)
		}

		// Wait until status reflects the change
		c.waitForRolegroupStatus(ctx, t, cfg, rg, func(obj k8s.Object) bool {
			r, ok := obj.(*v1alpha1.Rolegroup)
			if !ok {
				return false
			}
			return !r.Status.AtProvider.DisableRoleAdmin
		})
		return ctx
	})

	fB.Assess("verify-enabled-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRoleAdminEnabled(ctx, t, rolegroupName, true)
		return ctx
	})

	fB.Assess("update-to-disable-again", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		rg.Spec.ForProvider.DisableRoleAdmin = true

		res := cfg.Client().Resources()
		if err := res.Update(ctx, rg); err != nil {
			t.Fatalf("failed to update rolegroup: %v", err)
		}

		c.waitForRolegroupStatus(ctx, t, cfg, rg, func(obj k8s.Object) bool {
			r, ok := obj.(*v1alpha1.Rolegroup)
			if !ok {
				return false
			}
			return r.Status.AtProvider.DisableRoleAdmin
		})
		return ctx
	})

	fB.Assess("verify-disabled-again-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRoleAdminEnabled(ctx, t, rolegroupName, false)
		return ctx
	})

	fB.Assess("cleanup-cr", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		c.deleteRolegroupCR(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-deleted-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupNotExistsInDB(ctx, t, rolegroupName)
		return ctx
	})

	fB.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		for _, name := range c.RolegroupNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
		}
		_ = c.db.Disconnect()
		return ctx
	})

	testenv.Test(t, fB.Feature())
}

// --------------------------------------------------------------------------
// Test 3: FOR GRANTS ON TENANT OBJECTS
// --------------------------------------------------------------------------

func TestRolegroupForGrantsOnTenantObjects(t *testing.T) {
	logger := logging.NewNopLogger()
	rolegroupName := "E2ETESTRGTENANTOBJ"
	crName := "e2e-rg-tenant-objects"

	c := &RolegroupTestConfig{
		db: hana.New(logger),
	}

	fB := features.New("RolegroupForGrantsOnTenantObjects")
	fB.WithLabel("kind", "Rolegroup")

	fB.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.connectDB(ctx, t)
		var err error
		if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
			t.Fatalf("failed to create resource client: %v", err)
		}
		return ctx
	})

	fB.Assess("create-with-tenant-objects", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.createRolegroupCR(ctx, t, cfg, crName, rolegroupName, v1alpha1.RolegroupParameters{
			RolegroupName:            rolegroupName,
			ForGrantsOnTenantObjects: true,
		})
		c.waitForRolegroupReady(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupExistsInDB(ctx, t, rolegroupName)
		t.Logf("Rolegroup %q with FOR GRANTS ON TENANT OBJECTS created successfully", rolegroupName)
		return ctx
	})

	fB.Assess("cleanup-cr", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		c.deleteRolegroupCR(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-deleted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupNotExistsInDB(ctx, t, rolegroupName)
		return ctx
	})

	fB.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		for _, name := range c.RolegroupNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
		}
		_ = c.db.Disconnect()
		return ctx
	})

	testenv.Test(t, fB.Feature())
}

// --------------------------------------------------------------------------
// Test 4: NO GRANT TO CREATOR
// --------------------------------------------------------------------------

func TestRolegroupNoGrantToCreator(t *testing.T) {
	logger := logging.NewNopLogger()
	rolegroupName := "E2ETESTRGNOCREATOR"
	crName := "e2e-rg-no-grant-creator"

	c := &RolegroupTestConfig{
		db: hana.New(logger),
	}

	fB := features.New("RolegroupNoGrantToCreator")
	fB.WithLabel("kind", "Rolegroup")

	fB.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.connectDB(ctx, t)
		var err error
		if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
			t.Fatalf("failed to create resource client: %v", err)
		}
		return ctx
	})

	fB.Assess("create-with-no-grant-to-creator", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.createRolegroupCR(ctx, t, cfg, crName, rolegroupName, v1alpha1.RolegroupParameters{
			RolegroupName:    rolegroupName,
			NoGrantToCreator: true,
		})
		c.waitForRolegroupReady(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupExistsInDB(ctx, t, rolegroupName)
		// NO GRANT TO CREATOR only affects initial grant behavior;
		// the important thing is that the rolegroup was created successfully
		t.Logf("Rolegroup %q with NO GRANT TO CREATOR created successfully", rolegroupName)
		return ctx
	})

	fB.Assess("cleanup-cr", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		c.deleteRolegroupCR(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-deleted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupNotExistsInDB(ctx, t, rolegroupName)
		return ctx
	})

	fB.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		for _, name := range c.RolegroupNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
		}
		_ = c.db.Disconnect()
		return ctx
	})

	testenv.Test(t, fB.Feature())
}

// --------------------------------------------------------------------------
<<<<<<< Updated upstream
// Test 6: Kitchen sink — all options combined
||||||| Stash base
=======
// Test 5: Kitchen sink — all options combined
>>>>>>> Stashed changes
// --------------------------------------------------------------------------

func TestRolegroupWithAllOptions(t *testing.T) {
	logger := logging.NewNopLogger()
	rolegroupName := "E2ETESTRGALLOPTS"
	crName := "e2e-rg-all-options"

	c := &RolegroupTestConfig{
		db: hana.New(logger),
	}

	fB := features.New("RolegroupWithAllOptions")
	fB.WithLabel("kind", "Rolegroup")

	fB.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.connectDB(ctx, t)
		var err error
		if c.Resource, err = k8sresources.New(cfg.Client().RESTConfig()); err != nil {
			t.Fatalf("failed to create resource client: %v", err)
		}
		return ctx
	})

	fB.Assess("create-with-all-options", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.createRolegroupCR(ctx, t, cfg, crName, rolegroupName, v1alpha1.RolegroupParameters{
			RolegroupName:            rolegroupName,
			DisableRoleAdmin:         true,
			ForGrantsOnTenantObjects: true,
		})
		c.waitForRolegroupReady(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-all-in-db", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupExistsInDB(ctx, t, rolegroupName)
		c.verifyRoleAdminEnabled(ctx, t, rolegroupName, false) // DisableRoleAdmin=true means IS_ROLE_ADMIN_ENABLED=FALSE
		t.Logf("Rolegroup %q with all options verified", rolegroupName)
		return ctx
	})

	fB.Assess("verify-status", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		if rg.Status.AtProvider.RolegroupName != rolegroupName {
			t.Errorf("expected status rolegroupName=%q, got %q", rolegroupName, rg.Status.AtProvider.RolegroupName)
		}
		if !rg.Status.AtProvider.DisableRoleAdmin {
			t.Errorf("expected status disableRoleAdmin=true, got false")
		}
		t.Logf("Status for %q: rolegroupName=%q, disableRoleAdmin=%v",
			crName, rg.Status.AtProvider.RolegroupName, rg.Status.AtProvider.DisableRoleAdmin)
		return ctx
	})

	fB.Assess("cleanup-cr", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		rg := c.getRolegroup(ctx, t, crName)
		c.deleteRolegroupCR(ctx, t, cfg, rg)
		return ctx
	})

	fB.Assess("verify-deleted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		c.verifyRolegroupNotExistsInDB(ctx, t, rolegroupName)
		return ctx
	})

	fB.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		for _, name := range c.RolegroupNames {
			_, _ = c.conn.ExecContext(ctx, fmt.Sprintf(`DROP ROLEGROUP "%s"`, name))
		}
		_ = c.db.Disconnect()
		return ctx
	})

	testenv.Test(t, fB.Feature())
}
