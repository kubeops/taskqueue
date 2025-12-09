/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

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

	queueapi "kubeops.dev/taskqueue/apis/batch/v1alpha1"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func (r *TaskQueueReconciler) syncWatchingResourcesOnce(ctx context.Context, logger logr.Logger, tq *queueapi.TaskQueue) error {
	var errs []error
	r.once.Do(func() {
		logger.Info("Syncing watching resources for TaskQueue", "taskQueue", tq.Name)
		for typeRef := range tq.Status.TriggeredTasksStatus {
			gvk := parseGVKFromKey(typeRef)
			gvr, err := getGVR(r.DiscoveryClient, metav1.GroupKind{Group: gvk.Group, Kind: gvk.Kind})
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to get GVR: %w", err))
				return
			}
			r.startWatchingResource(ctx, gvr)
		}
	})
	return utilerrors.NewAggregate(errs)
}

func (r *TaskQueueReconciler) startWatchingResource(ctx context.Context, gvr schema.GroupVersionResource) {
	shouldStart := true
	if r.QueuePool.ExecuteFunc(func() {
		if _, exist := r.StartedWatchersByGVR[gvr]; exist {
			shouldStart = false
		}
		r.StartedWatchersByGVR[gvr] = struct{}{}
	}); !shouldStart {
		return
	}

	_, _ = r.DynamicInformerFactory.ForResource(gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new any) {
			if err := r.handleResourcesUpdate(ctx, new); err != nil {
				klog.Errorf("failed to handle resources update: %v", err)
			}
		},
		DeleteFunc: func(delObj any) {
			if err := r.handleResourcesUpdate(ctx, delObj); err != nil {
				klog.Errorf("failed to handle resources update: %v", err)
			}
		},
	})

	r.DynamicInformerFactory.Start(make(chan struct{}))
}

func (r *TaskQueueReconciler) handleResourcesUpdate(ctx context.Context, obj any) error {
	newObj := obj.(*unstructured.Unstructured)
	taskQueueName, err := r.findMatchingTaskQueue(ctx, newObj)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to find matching TaskQueue: %w", err)
	}
	tq, err := r.getTaskQueue(ctx, "", taskQueueName)
	if err != nil {
		return fmt.Errorf("failed to get TaskQueue: %w", err)
	}
	if tq == nil {
		return nil
	}
	if _, err := r.syncTaskQueueStatus(ctx, tq, func(key string) bool {
		return key == fmt.Sprintf("%s/%s", newObj.GetNamespace(), newObj.GetName())
	}); err != nil {
		return fmt.Errorf("failed to sync task status: %w", err)
	}

	if err := r.updateStatus(ctx, tq); err != nil {
		return fmt.Errorf("failed to update TaskQueue status: %w", err)
	}
	return err
}

func (r *TaskQueueReconciler) handlerForUpdatePendingTask(ctx context.Context, e event.TypedUpdateEvent[client.Object], w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pt := e.ObjectNew.(*queueapi.PendingTask)
	if pt.Status.TaskQueueName != "" {
		log.FromContext(ctx).V(1).Info("PendingTask updated, enqueuing referenced TaskQueue", "pendingTaskName", pt.Name, "taskQueueName", pt.Status.TaskQueueName)
		w.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: pt.Status.TaskQueueName},
		})
	}
}

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&queueapi.TaskQueue{}).
		Watches(
			&queueapi.PendingTask{},
			&handler.Funcs{
				UpdateFunc: r.handlerForUpdatePendingTask,
			},
		).
		Complete(r)
}
