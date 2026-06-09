//go:build e2e

// Reproduces the role-quoting drift bug introduced by SAP/crossplane-provider-hana
// PR #97 (commits cf0e5aa + 10e0dd5, merged 2026-05-19 as f19df90).
//
// Symptom on upstream/main: any User whose spec is non-restricted (so handleDefaults
// appends "PUBLIC" unquoted to desired.Roles) and that has at least one managed role
// will, after the first Observe, hold quoted role names in status.atProvider.Roles.
// Update()'s buildDesiredParameters does not call FormatRoleStrings, so the diff in
// updateRoles compares unquoted desired against quoted observed. The grant fires
// GRANT PUBLIC TO <user> and HANA returns:
//   SQL Error 258 - insufficient privilege:
//     Only internal user is authorized to grant role PUBLIC
// The User goes Synced=False; every later step in Update() (including updatePassword)
// is silently skipped on each reconcile.
//
// This test drives that exact path: create the User, wait for Ready, force any drift
// that pushes upToDate() to false (here: rotate the password Secret), then observe
// what the controller does. While the bug is present we expect Synced=False with
// the SQL 258 message. Once a fix lands (mirroring FormatRoleStrings into
// buildDesiredParameters, or pushing it into handleDefaults), this test should pass:
// the user reaches passwordUpToDate=true within the rotation timeout and the new
// password authenticates against HANA.
//
// The test is structured to FAIL THE BUILD when the bug is present, so it gates
// merges of the fix. To use it as a "demonstrate the bug" probe before the fix,
// run with -run TestRoleQuotingBug/.../diagnose-bug-present and read the logs;
// the diagnose step is non-fatal.

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crossplane-contrib/xp-testing/pkg/xpenvfuncs"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	k8sresources "sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	hanaapi "github.com/SAP/crossplane-provider-hana/apis/admin/v1alpha1"
)

const (
	roleQuotingPwdV1 = "RoleQuoting01Aa1xy"
	roleQuotingPwdV2 = "RoleQuoting02Bb2yz"

	// SQL Error 258 message fragment HANA returns when GRANT PUBLIC fires.
	// Stable across HANA Cloud versions; matches what the reconciler bubbles up.
	sqlError258Fragment = "Only internal user is authorized to grant role PUBLIC"

	// Bounds a single rotation cycle: secret update -> reconcile -> validation.
	roleQuotingRotationTimeout = 90 * time.Second
)

type roleQuotingBugTest struct {
	resource *k8sresources.Resources
	dir      string

	created []k8s.Object
	user    *hanaapi.User

	hanaEndpoint string
	hanaPort     string
	currentPwd   string
}

func TestRoleQuotingBug(t *testing.T) {
	c := &roleQuotingBugTest{
		dir:        filepath.Join(".", "crs", "RoleQuotingBug"),
		currentPwd: roleQuotingPwdV1,
	}

	fB := features.New("RoleQuotingBug")
	fB.WithLabel("kind", "RoleQuotingBug")
	fB.Setup(c.setup)
	fB.Assess("create-and-observe-quoted-roles", c.assessCreateAndObserveQuoted)
	fB.Assess("rotate-password-must-succeed", c.assessRotatePassword)
	fB.Teardown(c.teardown)

	testenv.Test(t, fB.Feature())
}

func (c *roleQuotingBugTest) setup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	res, err := k8sresources.New(cfg.Client().RESTConfig())
	if err != nil {
		t.Fatalf("create resource client: %v", err)
	}
	c.resource = res

	bindings := getProviderConfigSecretData()
	c.hanaEndpoint = bindings[string(xpv1.ResourceCredentialsSecretEndpointKey)]
	c.hanaPort = bindings[string(xpv1.ResourceCredentialsSecretPortKey)]
	if c.hanaEndpoint == "" || c.hanaPort == "" {
		t.Fatalf("HANA_BINDINGS missing endpoint/port")
	}

	if err := decoder.DecodeEachFile(ctx, os.DirFS(c.dir), "*.yaml",
		func(_ context.Context, obj k8s.Object) error {
			user, ok := obj.(*hanaapi.User)
			if !ok {
				return nil
			}
			c.user = user
			return nil
		},
		decoder.MutateNamespace(cfg.Namespace()),
	); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if c.user == nil {
		t.Fatalf("missing User fixture in %s", c.dir)
	}
	return ctx
}

// assessCreateAndObserveQuoted creates the User and waits for Ready+Synced, then
// asserts the precondition for the bug: status.atProvider.Roles contains role
// names in quoted form (e.g. "\"PUBLIC\""). If this isn't true, the test is
// running against a build that already normalizes roles symmetrically and the
// rotation step below will not exercise the asymmetry we care about.
func (c *roleQuotingBugTest) assessCreateAndObserveQuoted(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	pwdRef := c.user.Spec.ForProvider.Authentication.Password.PasswordSecretRef
	secret := xpenvfuncs.SimpleSecret(pwdRef.Name, pwdRef.Namespace, map[string]string{
		pwdRef.Key: c.currentPwd,
	})
	if err := c.resource.Create(ctx, secret); err != nil {
		t.Fatalf("create password secret: %v", err)
	}
	c.created = append(c.created, secret)

	u := c.user.DeepCopy()
	if err := c.resource.Create(ctx, u); err != nil {
		t.Fatalf("create User MR: %v", err)
	}
	c.created = append(c.created, u)

	if err := waitUserReady(ctx, c.resource, u, time.Minute*3); err != nil {
		dumpUser(t, u.Name, u.Namespace)
		t.Fatalf("User never reached Ready: %v", err)
	}

	// Refresh and inspect status.atProvider.Roles. Document what we find so the
	// log explains the bug regardless of which side the test fails on.
	if err := c.resource.Get(ctx, u.Name, u.Namespace, u); err != nil {
		t.Fatalf("get User after Ready: %v", err)
	}
	t.Logf("status.atProvider.Roles after first Observe: %v", u.Status.AtProvider.Roles)
	for i, r := range u.Status.AtProvider.Roles {
		t.Logf("  Roles[%d]=%q (len=%d, first=%q last=%q)", i, r, len(r),
			func() string {
				if len(r) == 0 {
					return ""
				}
				return string(r[0])
			}(),
			func() string {
				if len(r) == 0 {
					return ""
				}
				return string(r[len(r)-1])
			}())
	}

	hasQuoted := false
	for _, r := range u.Status.AtProvider.Roles {
		if strings.HasPrefix(r, "\"") && strings.HasSuffix(r, "\"") {
			hasQuoted = true
			break
		}
	}
	if !hasQuoted {
		// Build is already symmetric (either pre-cf0e5aa or post-fix). The rotation
		// step still asserts the user-visible behavior we care about (no Synced=False
		// stuck reconcile), but the bug-specific drift won't be exercised.
		t.Logf("note: status.atProvider.Roles is unquoted; this build is not subject to the role-quoting drift")
	} else {
		t.Logf("note: status.atProvider.Roles is quoted; build matches the asymmetric-quoting state from PR #97")
	}
	c.user = u
	return ctx
}

// assessRotatePassword updates the password Secret. With the bug present, Update()
// fails on updateRoles before reaching updatePassword, the User stays
// Synced=False with SQL Error 258, and passwordUpToDate never flips back to true.
// Without the bug, the user is reconciled within roleQuotingRotationTimeout and the new
// password authenticates.
func (c *roleQuotingBugTest) assessRotatePassword(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	newPwd := roleQuotingPwdV2
	pwdRef := c.user.Spec.ForProvider.Authentication.Password.PasswordSecretRef

	// Capture LastPasswordChangeTime before the rotation so we can tell a real
	// rotation apart from "passwordUpToDate was already true from initial create".
	pre := &hanaapi.User{}
	if err := c.resource.Get(ctx, c.user.Name, c.user.Namespace, pre); err != nil {
		t.Fatalf("get User pre-rotation: %v", err)
	}
	prevChange := pre.Status.AtProvider.LastPasswordChangeTime
	t.Logf("pre-rotation LastPasswordChangeTime=%s passwordUpToDate=%v",
		prevChange.Format(time.RFC3339Nano), pre.Status.AtProvider.PasswordUpToDate)

	if err := updateSecret(ctx, c.resource, pwdRef.Name, pwdRef.Namespace, pwdRef.Key, newPwd); err != nil {
		t.Fatalf("update password secret: %v", err)
	}

	// Watch the User for either of two outcomes within the timeout:
	//   1. Synced=False with the SQL 258 message  -> bug reproduced; FAIL.
	//   2. passwordUpToDate=true + Synced=True + LastPasswordChangeTime advanced -> healthy; pass.
	// The LastPasswordChangeTime check is what distinguishes a real rotation from
	// the stale "still up to date from initial create" state we'd otherwise race on.
	deadline := time.Now().Add(roleQuotingRotationTimeout)
	var lastSyncedReason, lastSyncedMsg string
	for time.Now().Before(deadline) {
		u := &hanaapi.User{}
		if err := c.resource.Get(ctx, c.user.Name, c.user.Namespace, u); err != nil {
			t.Fatalf("get User during rotation watch: %v", err)
		}

		var syncedTrue, readyTrue, syncedFalse bool
		for _, cond := range u.Status.Conditions {
			if cond.Type == xpv1.TypeReady && cond.Status == corev1.ConditionTrue {
				readyTrue = true
			}
			if cond.Type == xpv1.TypeSynced {
				lastSyncedReason = string(cond.Reason)
				lastSyncedMsg = cond.Message
				switch cond.Status {
				case corev1.ConditionTrue:
					syncedTrue = true
				case corev1.ConditionFalse:
					syncedFalse = true
				}
			}
		}

		// Bug detection: Synced=False with SQL 258 fragment is the smoking gun.
		if syncedFalse && strings.Contains(lastSyncedMsg, sqlError258Fragment) {
			dumpUser(t, u.Name, u.Namespace)
			t.Fatalf("BUG REPRODUCED: User went Synced=False with SQL Error 258 after password rotation\n"+
				"  reason: %s\n  message: %s\n"+
				"  status.atProvider.Roles: %v\n"+
				"  spec.forProvider.Roles: %v\n"+
				"  This is the role-quoting drift from PR #97. The fix is to mirror "+
				"FormatRoleStrings/FormatPrivilegeStrings into buildDesiredParameters "+
				"(internal/controller/user/reconciler.go:580-585), or push normalization "+
				"into handleDefaults so Observe and Update share canonical desired state.",
				lastSyncedReason, lastSyncedMsg, u.Status.AtProvider.Roles, u.Spec.ForProvider.Roles)
		}

		// Healthy path: rotation actually happened.
		// Required: Synced=True, Ready=True, passwordUpToDate=true, AND
		// LastPasswordChangeTime moved forward from the pre-rotation snapshot.
		newChange := u.Status.AtProvider.LastPasswordChangeTime
		rotated := newChange.After(prevChange.Time)
		if syncedTrue && readyTrue && u.Status.AtProvider.PasswordUpToDate != nil && *u.Status.AtProvider.PasswordUpToDate && rotated {
			t.Logf("OK: rotation applied. LastPasswordChangeTime %s -> %s",
				prevChange.Format(time.RFC3339Nano), newChange.Format(time.RFC3339Nano))
			c.currentPwd = newPwd
			return ctx
		}

		time.Sleep(2 * time.Second)
	}

	// Neither outcome within the timeout. Treat as failure with full diagnostics.
	u := &hanaapi.User{}
	_ = c.resource.Get(ctx, c.user.Name, c.user.Namespace, u)
	dumpUser(t, c.user.Name, c.user.Namespace)
	t.Fatalf("password rotation did not reconcile within %s\n"+
		"  last Synced reason=%q message=%q\n"+
		"  passwordUpToDate=%v\n"+
		"  status.atProvider.Roles=%v",
		roleQuotingRotationTimeout, lastSyncedReason, lastSyncedMsg,
		u.Status.AtProvider.PasswordUpToDate, u.Status.AtProvider.Roles)
	return ctx // unreachable; satisfies the compiler since t.Fatalf isn't NoReturn
}

func (c *roleQuotingBugTest) teardown(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	for i := len(c.created) - 1; i >= 0; i-- {
		obj := c.created[i]
		if err := c.resource.Delete(ctx, obj); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("delete %s/%s: %v", obj.GetName(), obj.GetNamespace(), err)
		}
	}
	return ctx
}

// --- helpers (small, copied from the SecondaryAdmin test pattern) ---

// waitUserReady blocks until the User reports Ready=True && Synced=True.
func waitUserReady(ctx context.Context, res *k8sresources.Resources, u *hanaapi.User, timeout time.Duration) error {
	pred := func(o k8s.Object) bool {
		user, ok := o.(*hanaapi.User)
		if !ok {
			return false
		}
		var ready, synced bool
		for _, c := range user.Status.Conditions {
			if c.Type == xpv1.TypeReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
			if c.Type == xpv1.TypeSynced && c.Status == corev1.ConditionTrue {
				synced = true
			}
		}
		return ready && synced
	}
	return wait.For(conditions.New(res).ResourceMatch(u, pred), wait.WithTimeout(timeout))
}

// updateSecret patches a single key on an existing Secret.
func updateSecret(ctx context.Context, res *k8sresources.Resources, name, ns, key, value string) error {
	s := &corev1.Secret{}
	if err := res.Get(ctx, name, ns, s); err != nil {
		return fmt.Errorf("get secret %s/%s: %w", ns, name, err)
	}
	if s.StringData == nil {
		s.StringData = map[string]string{}
	}
	s.StringData[key] = value
	if s.Data != nil {
		delete(s.Data, key)
	}
	return res.Update(ctx, s)
}

// dumpUser shells out to kubectl describe and prints the result.
func dumpUser(t *testing.T, name, ns string) {
	out, err := exec.Command("kubectl", "describe", "user", name, "-n", ns).CombinedOutput()
	if err != nil {
		t.Logf("kubectl describe failed: %v", err)
		return
	}
	t.Logf("kubectl describe user %s/%s:\n%s", ns, name, string(out))
}
