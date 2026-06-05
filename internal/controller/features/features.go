/*
Copyright 2026 SAP SE or an SAP affiliate company and contributors.
*/

package features

import (
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/feature"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
)

// Feature flags.
const (
	// EnableAlphaExternalSecretStores enables alpha support for
	// External Secret Stores. See the below design for more details.
	// https://github.com/crossplane/crossplane/blob/390ddd/design/design-doc-external-secret-stores.md
	EnableAlphaExternalSecretStores feature.Flag = "EnableAlphaExternalSecretStores"
)

// ManagementPoliciesOpts returns the reconciler options needed to honor
// spec.managementPolicies on managed resources, but only when the
// EnableBetaManagementPolicies feature is enabled on the controller options.
// When the feature is disabled it returns nil, so the reconciler behaves
// as if it had never opted in.
func ManagementPoliciesOpts(o controller.Options) []managed.ReconcilerOption {
	if !o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		return nil
	}
	return []managed.ReconcilerOption{managed.WithManagementPolicies()}
}
