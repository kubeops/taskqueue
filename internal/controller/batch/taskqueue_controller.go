package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	kmc "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"sync"

	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
)

const taskQueueFinalizer = "batch.kubeops.dev/finalizer"

type TaskQueueReconciler struct {
	client.Client
	taskQueue              *batchv1alpha1.TaskQueue
	DiscoveryClient        *discovery.DiscoveryClient
	DynamicInformerFactory dynamicinformer.DynamicSharedInformerFactory

	QueueLocks          sync.Map
	Scheme              *runtime.Scheme
	MapOfQueues         map[string]*workqueue.Typed[string]
	MapOfWatchResources map[schema.GroupVersionResource]struct{}
}

func (r *TaskQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling TaskQueue", "name", req.Name, "namespace", req.Namespace)

	if err := r.getTaskQueue(ctx, req.Name); err != nil || r.taskQueue == nil {
		return ctrl.Result{}, err
	}
	if !r.taskQueue.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, logger)
	}

	if err := r.ensureFinalizer(ctx, logger); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateTriggeredTasksStatus(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update triggered tasks status: %w", err)
	}

	if err := r.processPendingTasks(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to process pending tasks: %w", err)
	}

	if err := r.updateStatus(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *TaskQueueReconciler) startWatchingResource(ctx context.Context, pt *batchv1alpha1.PendingTask) error {
	gvr, err := r.getGVR(pt.Spec.TaskType)
	if err != nil {
		return err
	}

	if _, exist := r.MapOfWatchResources[gvr]; exist {
		return nil
	}
	r.MapOfWatchResources[gvr] = struct{}{}
	_, _ = r.DynamicInformerFactory.ForResource(gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {

		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			obj := newObj.(*unstructured.Unstructured)
			taskQueueName, err := r.matchTaskQueue(ctx, batchv1alpha1.TypedResourceReference{
				APIGroup: obj.GroupVersionKind().Group,
				Kind:     obj.GroupVersionKind().Kind,
			})
			if err != nil && !errors.IsNotFound(err) {
				klog.Error(err, "failed to match task queue from watch")
			}

			err = r.withQueueLock(taskQueueName, func() error {
				if err := r.getTaskQueue(ctx, taskQueueName); err != nil {
					return fmt.Errorf("failed to get task queue: %w", err)
				}
				if err := r.updateTriggeredTaskStatus(ctx, obj.GetName()); err != nil {
					return fmt.Errorf("failed to update triggered task status: %w", err)
				}
				if err := r.updateStatus(ctx); err != nil {
					return fmt.Errorf("failed to update status: %w", err)
				}
				return nil
			})
			if err != nil {
				klog.Error(err, "failed to update task queue")
			}
		},
		DeleteFunc: func(obj interface{}) {
		},
	})
	r.DynamicInformerFactory.Start(make(chan struct{}))
	return nil
}

func (r *TaskQueueReconciler) withQueueLock(taskQueueName string, fn func() error) error {
	mutexIfRace, _ := r.QueueLocks.LoadOrStore(taskQueueName, &sync.Mutex{})
	mutex := mutexIfRace.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	return fn()
}

func (r *TaskQueueReconciler) updateTriggeredTaskStatus(ctx context.Context, taskName string) error {
	if r.taskQueue.Status.TriggeredTasksPhase == nil {
		r.taskQueue.Status.TriggeredTasksPhase = make(map[string]batchv1alpha1.TaskPhase)
	}
	isPhaseUpdated := false
	for key := range r.taskQueue.Status.TriggeredTasksPhase {
		if isPhaseUpdated {
			break
		}
		if strings.HasSuffix(key, taskName) {
			isPhaseUpdated = true
			gvk, ns, name := parseKey(key)
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)

			if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
				if errors.IsNotFound(err) {
					delete(r.taskQueue.Status.TriggeredTasksPhase, key)
					err = nil
				}
				return err
			}

			ruleSet := r.getRuleSetFromTaskQueue(gvk)
			if ok, err := evaluateCEL(obj, ruleSet.Success); err == nil && ok {
				delete(r.taskQueue.Status.TriggeredTasksPhase, key)
				err = nil
			} else if err != nil {
				return err
			}

			if ok, err := evaluateCEL(obj, ruleSet.InProgress); err == nil && ok {
				r.taskQueue.Status.TriggeredTasksPhase[key] = batchv1alpha1.TaskPhaseInProgress
				return nil
			} else if err != nil {
				return err
			}

			if ok, err := evaluateCEL(obj, ruleSet.Failed); err == nil && ok {
				delete(r.taskQueue.Status.TriggeredTasksPhase, key)
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *TaskQueueReconciler) matchTaskQueue(ctx context.Context, taskType batchv1alpha1.TypedResourceReference) (string, error) {
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

func (r *TaskQueueReconciler) getTaskQueue(ctx context.Context, name string) error {
	r.taskQueue = &batchv1alpha1.TaskQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, r.taskQueue); err != nil {
		if errors.IsNotFound(err) {
			r.taskQueue = nil
			return nil
		}
		return err
	}
	return nil
}

func (r *TaskQueueReconciler) ensureFinalizer(ctx context.Context, logger logr.Logger) error {
	if !controllerutil.ContainsFinalizer(r.taskQueue, taskQueueFinalizer) {
		logger.Info("Adding finalizer for TaskQueue")
		controllerutil.AddFinalizer(r.taskQueue, taskQueueFinalizer)
		if err := r.Update(ctx, r.taskQueue); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return err
		}
	}
	return nil
}

func (r *TaskQueueReconciler) updateTriggeredTasksStatus(ctx context.Context) error {
	if r.taskQueue.Status.TriggeredTasksPhase == nil {
		r.taskQueue.Status.TriggeredTasksPhase = make(map[string]batchv1alpha1.TaskPhase)
	}

	var errs []error
	for key := range r.taskQueue.Status.TriggeredTasksPhase {
		gvk, ns, name := parseKey(key)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)

		if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
			if errors.IsNotFound(err) {
				delete(r.taskQueue.Status.TriggeredTasksPhase, key)
				continue
			}
			errs = append(errs, err)
			continue
		}

		ruleSet := r.getRuleSetFromTaskQueue(gvk)
		if ok, err := evaluateCEL(obj, ruleSet.Success); err == nil && ok {
			delete(r.taskQueue.Status.TriggeredTasksPhase, key)
			continue
		} else if err != nil {
			errs = append(errs, err)
		}

		if ok, err := evaluateCEL(obj, ruleSet.InProgress); err == nil && ok {
			r.taskQueue.Status.TriggeredTasksPhase[key] = batchv1alpha1.TaskPhaseInProgress
			continue
		} else if err != nil {
			errs = append(errs, err)
		}

		if ok, err := evaluateCEL(obj, ruleSet.Failed); err == nil && ok {
			delete(r.taskQueue.Status.TriggeredTasksPhase, key)
			continue
		} else if err != nil {
			errs = append(errs, err)
		}
	}

	return utilerrors.NewAggregate(errs)
}

func (r *TaskQueueReconciler) processPendingTasks(ctx context.Context) error {
	_, exists := r.MapOfQueues[r.taskQueue.Name]
	if !exists {
		klog.Infof("no queue found for %s\n", r.taskQueue.Name)
		return nil
	}
	err := r.withQueueLock(r.taskQueue.Name, func() error {
		inProgress := r.getInProgressTaskCount()
		if inProgress < r.taskQueue.Spec.MaxConcurrentTasks && r.MapOfQueues[r.taskQueue.Name].Len() > 0 {
			pt, err := r.getPendingTask(ctx)
			if err != nil {
				return client.IgnoreNotFound(err)
			}
			if err = r.createTasksObject(ctx, pt); err != nil {
				return fmt.Errorf("failed to create pendingTasks object: %w", err)
			}
			return r.startWatchingResource(ctx, pt)
		}
		return nil
	})
	return err
}

func (r *TaskQueueReconciler) createTasksObject(ctx context.Context, pt *batchv1alpha1.PendingTask) error {
	var obj unstructured.Unstructured
	if err := json.Unmarshal(pt.Spec.Resource.Raw, &obj.Object); err != nil {
		return fmt.Errorf("failed to unmarshal task resource: %w", err)
	}
	if _, err := kmc.CreateOrPatch(ctx, r.Client, &obj,
		func(obj client.Object, createOp bool) client.Object {
			in := obj.(*unstructured.Unstructured)
			return in
		},
	); err != nil {
		return fmt.Errorf("failed to create or patch task: %w", err)
	}

	r.taskQueue.Status.TriggeredTasksPhase[getKeyFromTaskObject(obj)] = batchv1alpha1.TaskPhaseInPending
	if err := r.Delete(ctx, pt); err != nil {
		return fmt.Errorf("failed to delete task object: %w", err)
	}
	return nil
}

func (r *TaskQueueReconciler) getGVR(typeRef batchv1alpha1.TypedResourceReference) (schema.GroupVersionResource, error) {
	apiResource, version, err := r.getPreferredResourceVersion(schema.GroupKind{
		Group: typeRef.APIGroup,
		Kind:  typeRef.Kind,
	})
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("failed to get preferred version for %s/%s: %w", typeRef.APIGroup, typeRef.Kind, err)
	}
	return schema.GroupVersionResource{Group: typeRef.APIGroup, Version: version, Resource: apiResource}, nil
}

func (r *TaskQueueReconciler) getPreferredResourceVersion(gk schema.GroupKind) (string, string, error) {
	groups, err := r.DiscoveryClient.ServerGroups()
	if err != nil {
		return "", "", fmt.Errorf("list API groups: %w", err)
	}
	for _, group := range groups.Groups {
		if group.Name != gk.Group {
			continue
		}
		for _, ver := range group.Versions {
			resList, err := r.DiscoveryClient.ServerResourcesForGroupVersion(ver.GroupVersion)
			if err != nil {
				continue
			}
			for _, r := range resList.APIResources {
				if r.Kind == gk.Kind {
					return r.Name, ver.Version, nil
				}
			}
		}
	}
	return "", "", fmt.Errorf("resource %s/%s not found in discovery", gk.Group, gk.Kind)
}

func (r *TaskQueueReconciler) updateStatus(ctx context.Context) error {
	_, err := kmc.PatchStatus(
		ctx,
		r.Client,
		r.taskQueue,
		func(obj client.Object) client.Object {
			in := obj.(*batchv1alpha1.TaskQueue)
			in.Status = r.taskQueue.Status
			return in
		},
	)
	return client.IgnoreNotFound(err)
}

func (r *TaskQueueReconciler) patchTaskQueueStatus(ctx context.Context, tq *batchv1alpha1.TaskQueue) error {
	logger := log.FromContext(ctx).WithValues("taskQueueName", tq.Name)
	desiredStatus := tq.Status.DeepCopy() // Status is from the locally managed 'tq' instance

	// Create a minimal object for patching to avoid stale ResourceVersion issues if 'tq' itself is old.
	patchObj := &batchv1alpha1.TaskQueue{}
	patchObj.SetNamespace(tq.GetNamespace())
	patchObj.SetName(tq.GetName())
	// patchObj.SetUID(tq.GetUID()) // UID can help with optimistic locking if patch helper uses it.

	_, err := kmc.PatchStatus(
		ctx,
		r.Client,
		patchObj, // kmc.PatchStatus will Get this object then apply the func
		func(obj client.Object) client.Object {
			in := obj.(*batchv1alpha1.TaskQueue)
			in.Status = *desiredStatus
			return in
		},
	)
	if err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to patch TaskQueue status from watch handler")
		return err
	}
	return nil
}

func (r *TaskQueueReconciler) getRuleSetFromTaskQueue(gvk schema.GroupVersionKind) batchv1alpha1.ObjectPhaseRules {
	for _, task := range r.taskQueue.Spec.Tasks {
		if task.Type.Kind == gvk.Kind && task.Type.APIGroup == gvk.Group {
			return task.Rules
		}
	}
	return batchv1alpha1.ObjectPhaseRules{}
}

func (r *TaskQueueReconciler) getInProgressTaskCount() int {
	count := 0
	for _, phase := range r.taskQueue.Status.TriggeredTasksPhase {
		if phase == batchv1alpha1.TaskPhaseInProgress || phase == batchv1alpha1.TaskPhaseInPending {
			count++
		}
	}
	return count
}

func (r *TaskQueueReconciler) getPendingTask(ctx context.Context) (*batchv1alpha1.PendingTask, error) {
	key, shutdown := r.MapOfQueues[r.taskQueue.Name].Get()
	if shutdown {
		return nil, fmt.Errorf("taskqueue for %s is shutting down", r.taskQueue.Name)
	}
	r.MapOfQueues[r.taskQueue.Name].Done(key)
	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(ctx, types.NamespacedName{Name: key}, pendingTask); err != nil {
		return nil, err
	}
	return pendingTask, nil
}

func (r *TaskQueueReconciler) reconcileDelete(ctx context.Context, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Reconciling deletion for TaskQueue", "name", r.taskQueue.Name)

	mutexIfRace, loaded := r.QueueLocks.LoadAndDelete(r.taskQueue.Name)
	if loaded {
		mutex := mutexIfRace.(*sync.Mutex)
		mutex.Lock()
		defer mutex.Unlock()
		if queue, ok := r.MapOfQueues[r.taskQueue.Name]; ok {
			logger.Info("Shutting down and removing workqueue for deleted TaskQueue", "name", r.taskQueue.Name)
			queue.ShutDown()
		}
	}
	delete(r.MapOfQueues, r.taskQueue.Name)

	if controllerutil.ContainsFinalizer(r.taskQueue, taskQueueFinalizer) {
		logger.Info("Removing finalizer for TaskQueue", "name", r.taskQueue.Name)
		controllerutil.RemoveFinalizer(r.taskQueue, taskQueueFinalizer)
		if err := r.Update(ctx, r.taskQueue); err != nil {
			logger.Error(err, "Failed to remove finalizer for TaskQueue", "name", r.taskQueue.Name)
			return ctrl.Result{}, err
		}
	}
	logger.Info("TaskQueue deleted successfully", "name", r.taskQueue.Name)
	return ctrl.Result{}, nil
}

func parseKey(key string) (schema.GroupVersionKind, string, string) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		klog.Errorf("Invalid key format encountered in parseKey: %s", key)
		return schema.GroupVersionKind{}, "", ""
	}

	group := parts[0]
	if group == "_" {
		group = ""
	}

	namespace := parts[3]
	if namespace == "_" {
		namespace = ""
	}

	gvk := schema.GroupVersionKind{
		Group:   group,
		Version: parts[1],
		Kind:    parts[2],
	}

	return gvk, namespace, parts[4]
}

func evaluateCEL(obj *unstructured.Unstructured, rule string) (bool, error) {
	if obj == nil || obj.Object == nil {
		return false, fmt.Errorf("object is nil or empty")
	}
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil {
		return false, fmt.Errorf("failed to access status field: %v", err)
	}
	if !found {
		return false, nil
	}
	phase, ok := status["phase"]
	if !ok {
		return false, nil
	}
	if _, ok := phase.(string); !ok {
		return false, fmt.Errorf("status.phase is not a string: got %T", phase)
	}
	env, err := cel.NewEnv(cel.Declarations(decls.NewVar("self", decls.NewMapType(decls.String, decls.Dyn))))
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %v", err)
	}
	ast, issues := env.Compile(rule)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("failed to compile CEL rule %q: %v", rule, issues.Err())
	}
	program, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL program: %v", err)
	}
	out, _, err := program.Eval(map[string]any{
		"self": obj.Object,
	})
	if err != nil {
		return false, fmt.Errorf("failed to evaluate CEL rule %q: %v", rule, err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL rule %q did not return a boolean: got %T", rule, out.Value())
	}

	return result, nil
}

func getKeyFromTaskObject(obj unstructured.Unstructured) string {
	group := obj.GroupVersionKind().Group
	if group == "" {
		group = "_"
	}
	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "_"
	}
	gvk := obj.GroupVersionKind()
	return fmt.Sprintf("%s/%s/%s/%s/%s", group, gvk.Version, gvk.Kind, namespace, obj.GetName())
}

func (r *TaskQueueReconciler) handlerForUpdatePendingTask(ctx context.Context, e event.TypedUpdateEvent[client.Object], w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pt := e.ObjectNew.(*batchv1alpha1.PendingTask)
	if pt.Status.TaskQueueName != "" {
		log.FromContext(ctx).V(1).Info("PendingTask updated, enqueuing referenced TaskQueue", "pendingTaskName", pt.Name, "taskQueueName", pt.Status.TaskQueueName)
		w.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: pt.Status.TaskQueueName},
		})
	}

}

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.TaskQueue{}).
		Watches(
			&batchv1alpha1.PendingTask{},
			&handler.Funcs{
				UpdateFunc: r.handlerForUpdatePendingTask,
			},
		).
		Complete(r)
}
