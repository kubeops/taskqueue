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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	kmc "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sync"

	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
)

// PendingTaskReconciler reconciles a pendingTask object
type PendingTaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// MapOfQueues holds worker queues indexed by TaskQueue name
	MapOfQueues map[string]*workqueue.Typed[string]
	mu          sync.RWMutex
}

// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/finalizers,verbs=update

func (r *PendingTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PendingTask", "name", req.Name, "namespace", req.Namespace)

	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(ctx, req.NamespacedName, pendingTask); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if pendingTask.Status.TaskQueueName != "" {
		return ctrl.Result{}, nil
	}

	taskQueueName, err := r.matchTaskQueue(ctx, pendingTask.Spec.TaskType)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to find matching TaskQueue: %w", err)
	}
	if taskQueueName == "" {
		logger.Info("No matching TaskQueue found", "taskType", pendingTask.Spec.TaskType)
		return ctrl.Result{}, nil
	}

	pendingTask.Status = batchv1alpha1.PendingTaskStatus{
		TaskQueueName: taskQueueName,
	}
	if err := r.updateStatus(ctx, pendingTask); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

	// Enqueue task after successful status update
	r.enqueueTask(taskQueueName, req.Name)
	logger.Info("Successfully enqueued task", "taskQueue", taskQueueName, "task", req.Name)

	return ctrl.Result{}, nil
}

func (r *PendingTaskReconciler) updateStatus(ctx context.Context, pt *batchv1alpha1.PendingTask) error {
	_, err := kmc.PatchStatus(
		ctx,
		r.Client,
		pt,
		func(obj client.Object) client.Object {
			in := obj.(*batchv1alpha1.PendingTask)
			in.Status = pt.Status
			return in
		},
	)
	return client.IgnoreNotFound(err)
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
	return "", nil
}

func (r *PendingTaskReconciler) enqueueTask(queueName, taskKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.MapOfQueues[queueName]; !exists {
		klog.Infof("Initializing new work queue for TaskQueue: %s", queueName)
		r.MapOfQueues[queueName] = workqueue.NewTyped[string]()
	}

	r.MapOfQueues[queueName].Add(taskKey)
	klog.V(4).Infof("Added task %s to queue %s", taskKey, queueName)
}

func (r *PendingTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.PendingTask{}).
		Complete(r)
}
