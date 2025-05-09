package v1alpha1

import (
	"go.virtual-secrets.dev/taskqueue/crds"

	"kmodules.xyz/client-go/apiextensions"
)

func (_ PendingTask) CustomResourceDefinition() *apiextensions.CustomResourceDefinition {
	return crds.MustCustomResourceDefinition(GroupVersion.WithResource(ResourcePendingTasks))
}
