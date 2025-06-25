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

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"
	"kubeops.dev/taskqueue/pkg/queue"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/errors"
	cu "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PendingTaskReconciler reconciles a pendingTask object
type PendingTaskReconciler struct {
	client.Client
	once      sync.Once
	Scheme    *runtime.Scheme
	QueuePool *queue.SharedQueuePool
}

// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.k8s.appscode.com,resources=pendingtasks/finalizers,verbs=update

func (r *PendingTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PendingTask", "name", req.Name, "namespace", req.Namespace)

	if err := r.syncQueuePoolOnce(ctx, logger); err != nil {
		return ctrl.Result{Requeue: true}, fmt.Errorf("failed to sync queue pool: %v", err)
	}

	pendingTask := &queueapi.PendingTask{}
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

	pendingTask.Status = queueapi.PendingTaskStatus{
		TaskQueueName: taskQueueName,
	}

	r.QueuePool.Add(taskQueueName)
	r.QueuePool.Enqueue(taskQueueName, req.Name)

	logger.Info("Successfully enqueued task", "taskQueue", taskQueueName, "task", req.Name)
	if err := r.updateStatus(ctx, pendingTask); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *PendingTaskReconciler) updateStatus(ctx context.Context, pt *queueapi.PendingTask) error {
	_, err := cu.PatchStatus(
		ctx,
		r.Client,
		pt,
		func(obj client.Object) client.Object {
			in := obj.(*queueapi.PendingTask)
			in.Status = pt.Status
			return in
		},
	)
	return client.IgnoreNotFound(err)
}

func (r *PendingTaskReconciler) syncQueuePoolOnce(ctx context.Context, logger logr.Logger) error {
	var errs []error
	r.once.Do(func() {
		var pendingTaskList queueapi.PendingTaskList
		if err := r.List(ctx, &pendingTaskList); err != nil {
			errs = append(errs, fmt.Errorf("failed to list pendingTasks: %w", err))
			return
		}
		for _, pt := range pendingTaskList.Items {
			taskQueueName, err := r.matchTaskQueue(ctx, pt.Spec.TaskType)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to find matching TaskQueue: %w", err))
			}
			if taskQueueName == "" {
				logger.Info("No matching TaskQueue found", "taskType", pt.Spec.TaskType)
			}
			r.QueuePool.Add(taskQueueName)
			r.QueuePool.Enqueue(taskQueueName, pt.Name)
		}
	})
	return errors.NewAggregate(errs)
}

func (r *PendingTaskReconciler) matchTaskQueue(ctx context.Context, taskType metav1.GroupKind) (string, error) {
	var taskQueueList queueapi.TaskQueueList
	if err := r.List(ctx, &taskQueueList); err != nil {
		return "", fmt.Errorf("failed to list TaskQueues: %w", err)
	}

	for _, tq := range taskQueueList.Items {
		for _, task := range tq.Spec.Tasks {
			if task.Type.Kind == taskType.Kind && task.Type.Group == taskType.Group {
				return tq.Name, nil
			}
		}
	}
	return "", nil
}

func (r *PendingTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&queueapi.PendingTask{}).
		Complete(r)
}
