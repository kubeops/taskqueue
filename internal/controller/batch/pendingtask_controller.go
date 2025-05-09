/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package batch

import (
	"context"
	"fmt"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
)

// PendingTaskReconciler reconciles a PendingTask object
type PendingTaskReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	MapOfQueues map[string]*workqueue.Typed[string]
	MapOfTasks  map[string]struct{}
	Mu          sync.RWMutex
}

// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PendingTask object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *PendingTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)
	logger.Info("Reconciling PendingTask", "name", req.Name, "namespace", req.Namespace)

	key := req.NamespacedName.String()
	if r.checkExists(key) {
		return ctrl.Result{}, nil
	}

	ps := &batchv1alpha1.PendingTask{}
	if err := r.Get(ctx, req.NamespacedName, ps); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	tqName, err := r.getMatchedTaskQueueName(ps.Spec.Task)
	if err != nil {
		return ctrl.Result{}, err
	}

	r.Mu.Lock()
	defer r.Mu.Unlock()
	queue, exists := r.MapOfQueues[tqName]
	if !exists {
		klog.Infof("Queue for key '%s' not found, creating a new one.\n", tqName)
		queue = workqueue.NewTyped[string]()
		r.MapOfQueues[tqName] = queue
	}

	queue.Add(key)
	r.MapOfTasks[key] = struct{}{}
	return ctrl.Result{}, nil
}

func (r *PendingTaskReconciler) getMatchedTaskQueueName(task batchv1alpha1.UnitTask) (string, error) {
	taskQueueList := batchv1alpha1.TaskQueueList{}
	if err := r.Client.List(context.Background(), &taskQueueList, &client.ListOptions{}); err != nil {
		return "", fmt.Errorf("failed to list task queues: %w", err)
	}
	for _, tq := range taskQueueList.Items {
		for _, t := range tq.Spec.Tasks {
			if t.Type.Kind == task.Type.Kind && t.Type.APIGroup == task.Type.APIGroup {
				return tq.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no matching task queue found for task: %s/%s", task.Type.Kind, task.Type.APIGroup)
}

func (r *PendingTaskReconciler) checkExists(item string) bool {
	r.Mu.RLocker()
	defer r.Mu.RUnlock()
	_, exists := r.MapOfTasks[item]
	return exists
}

// SetupWithManager sets up the controller with the Manager.
func (r *PendingTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.PendingTask{}).
		Named("batch-pendingtask").
		Complete(r)
}
