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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
)

// PendingTaskReconciler reconciles a PendingTask object
type PendingTaskReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	MapOfQueues map[string]*workqueue.Typed[string]
	MapOfTasks  map[string]struct{}
	Mu          sync.Mutex
}

// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/finalizers,verbs=update

func (r *PendingTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PendingTask", "name", req.Name, "namespace", req.Namespace)

	r.Mu.Lock()
	defer r.Mu.Unlock()

	taskKey := req.NamespacedName.Name
	if r.taskAlreadyExists(taskKey) {
		return ctrl.Result{}, nil
	}

	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(ctx, req.NamespacedName, pendingTask); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	taskQueueName, err := r.matchTaskQueue(ctx, pendingTask.Spec.TaskType)
	if err != nil {
		return ctrl.Result{}, err
	}
	if taskQueueName == "" {
		logger.Info("No matching TaskQueue found, requeuing after delay", "taskType", pendingTask.Spec.TaskType)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	r.enqueueTask(taskQueueName, taskKey)

	return ctrl.Result{}, nil
}

func (r *PendingTaskReconciler) taskAlreadyExists(key string) bool {
	_, exists := r.MapOfTasks[key]
	return exists
}

func (r *PendingTaskReconciler) matchTaskQueue(ctx context.Context, taskType batchv1alpha1.TypedResourceReference) (string, error) {
	var taskQueueList batchv1alpha1.TaskQueueList
	if err := r.List(ctx, &taskQueueList); err != nil {
		return "", fmt.Errorf("failed to list TaskQueues: %w", err)
	}

	for _, tq := range taskQueueList.Items {
		for _, task := range tq.Spec.Tasks {
			if task.Type.Kind == taskType.Kind && task.Type.APIGroup == taskType.APIGroup {
				return tq.Name, nil
			}
		}
	}

	klog.Infof("No TaskQueue matched for task type %s/%s", taskType.APIGroup, taskType.Kind)
	return "", nil
}

func (r *PendingTaskReconciler) enqueueTask(queueName, taskKey string) {
	queue, exists := r.MapOfQueues[queueName]
	if !exists {
		klog.Infof("Creating new queue for TaskQueue '%s'", queueName)
		queue = workqueue.NewTyped[string]()
		r.MapOfQueues[queueName] = queue
	}

	r.MapOfQueues[queueName].Add(taskKey)
	r.MapOfTasks[taskKey] = struct{}{}
}

func (r *PendingTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.PendingTask{}).
		Named("batch-pendingtask").
		Complete(r)
}
