package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/klog/v2"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/workqueue"
	kmc "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1alpha1 "kubeops.dev/taskqueue/api/batch/v1alpha1"
)

const taskQueueFinalizer = "batch.kubeops.dev/finalizer"

type TaskQueueReconciler struct {
	client.Client
	taskQueue   *batchv1alpha1.TaskQueue
	MapOfQueues map[string]*workqueue.Typed[string]
	MapOfTasks  map[string]struct{}
	QueueLocks  sync.Map
	Scheme      *runtime.Scheme
}

func (r *TaskQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling TaskQueue", "name", req.Name, "namespace", req.Namespace)

	if err := r.getTaskQueue(ctx, req, logger); err != nil || r.taskQueue == nil {
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

	return r.processPendingTasks(ctx)
}

func (r *TaskQueueReconciler) getTaskQueue(ctx context.Context, req ctrl.Request, logger logr.Logger) error {
	r.taskQueue = &batchv1alpha1.TaskQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, r.taskQueue); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("TaskQueue resource not found. Ignoring since object must be deleted.")
			return nil
		}
		logger.Error(err, "Unable to fetch TaskQueue")
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

func (r *TaskQueueReconciler) processPendingTasks(ctx context.Context) (ctrl.Result, error) {
	mutexIfRace, _ := r.QueueLocks.LoadOrStore(r.taskQueue.Name, &sync.Mutex{})
	mutex := mutexIfRace.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()

	inProgress := r.getInProgressTaskCount()
	var errs []error
	defer func() {
		if err := r.updateStatus(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to update status: %w", err))
		}
	}()

	_, exists := r.MapOfQueues[r.taskQueue.Name]
	if !exists {
		klog.Infof("no queue found for %s\n", r.taskQueue.Name)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	for inProgress < r.taskQueue.Spec.MaxConcurrentTasks && r.MapOfQueues[r.taskQueue.Name].Len() > 0 {
		key, _ := r.MapOfQueues[r.taskQueue.Name].Get()
		r.MapOfQueues[r.taskQueue.Name].Done(key)
		if err := r.processSingleTask(ctx, key); err != nil {
			errs = append(errs, err)
		}
		inProgress++
	}

	if len(errs) > 0 {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile TaskQueue: %v", errs)
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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

func (r *TaskQueueReconciler) processSingleTask(ctx context.Context, key string) error {
	pt, err := r.getPendingTaskFromKey(key)
	if err != nil {
		if errors.IsNotFound(err) {
			return r.Delete(ctx, pt)
		}
		return fmt.Errorf("failed to retrieve pending task: %w", err)
	}

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
	delete(r.MapOfTasks, pt.Name)
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

func (r *TaskQueueReconciler) getPendingTaskFromKey(taskName string) (*batchv1alpha1.PendingTask, error) {
	pendingTask := &batchv1alpha1.PendingTask{}
	if err := r.Get(context.TODO(), types.NamespacedName{Name: taskName}, pendingTask); err != nil {
		return nil, err
	}
	return pendingTask, nil
}

func (r *TaskQueueReconciler) reconcileDelete(ctx context.Context, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Reconciling deletion for TaskQueue")
	if controllerutil.ContainsFinalizer(r.taskQueue, taskQueueFinalizer) {
		logger.Info("Removing finalizer for TaskQueue")
		controllerutil.RemoveFinalizer(r.taskQueue, taskQueueFinalizer)
		if err := r.Update(ctx, r.taskQueue); err != nil {
			logger.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	logger.Info("TaskQueue deleted successfully")
	return ctrl.Result{}, nil
}

func parseKey(key string) (schema.GroupVersionKind, string, string) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		panic(fmt.Sprintf("invalid key format: %s", key))
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

func (r *TaskQueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.TaskQueue{}).
		Named("batch-taskqueue").
		Complete(r)
}
