package install

import (
	"go.virtual-secrets.dev/taskqueue/apis/ops/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

func Install(scheme *runtime.Scheme) {
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	scheme.AddUnversionedTypes(v1alpha1.InternalGV,
		&v1alpha1.PendingTask{},
		&v1alpha1.PendingTask{},
	)
}
