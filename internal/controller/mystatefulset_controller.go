/*
Copyright 2024.

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

package controller

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	workloadjojotongiov1 "jojotong.io/mystatefulset/api/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MyStatefulSetReconciler reconciles a MyStatefulSet object
type MyStatefulSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	finalizer = "mystatefulset.jojotong.io/finalizer"
)

// +kubebuilder:rbac:groups=workload.jojotong.io.jojotong.io,resources=mystatefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workload.jojotong.io.jojotong.io,resources=mystatefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workload.jojotong.io.jojotong.io,resources=mystatefulsets/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MyStatefulSet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *MyStatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("mystatefulset", req.NamespacedName)

	instance := &workloadjojotongiov1.MyStatefulSet{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil && errors.IsNotFound(err) {
		// Request object not found, could have been deleted after reconcile request.
		// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
		// Return and don't requeue
		log.Info("MyStatefulSet resource not found. Ignoring since object must be deleted")
		return ctrl.Result{}, nil
	} else if err != nil {
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get MyStatefulSet")
		return ctrl.Result{}, err
	}

	// Check if the MyStatefulSet instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isMyStatefulSetMarkedToBeDeleted := instance.GetDeletionTimestamp() != nil
	if isMyStatefulSetMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(instance, finalizer) {
			// Run finalization logic for mystatefulsetFinalizer. If the
			// finalization logic fails, don't remove the finalizer so
			// that we can retry during the next reconciliation.
			if err := r.finalizeMyStatefulSet(log, instance); err != nil {
				return ctrl.Result{}, err
			}

			// Remove mystatefulsetFinalizer. Once all finalizers have been
			// removed, the object will be deleted.
			controllerutil.RemoveFinalizer(instance, finalizer)
			err := r.Update(ctx, instance)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer for this CR
	if !controllerutil.ContainsFinalizer(instance, finalizer) {
		controllerutil.AddFinalizer(instance, finalizer)
		err = r.Update(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Namespace could have been deleted in the middle of the reconcile
	ns := &v1.Namespace{}
	err = r.Get(ctx, types.NamespacedName{Name: instance.Namespace, Namespace: v1.NamespaceAll}, ns)
	if (err != nil && errors.IsNotFound(err)) || (ns.Status.Phase == "Terminating") {
		log.Info(fmt.Sprintf("The namespace '%v' does not exist or is in Terminating status, canceling Reconciling", instance.Namespace))
		return ctrl.Result{}, nil
	} else if err != nil {
		log.Error(err, "Failed to check if namespace exists")
		return ctrl.Result{}, err
	}

	if err := addDefaultFields(instance); err != nil {
		return ctrl.Result{}, err
	}

	// pods
	founds := &v1.PodList{}
	pods, err := podsForMyStatefulSet(instance, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}
	err = r.List(ctx, founds, client.InNamespace(instance.Namespace), client.MatchingLabels(instance.Spec.Template.Labels))
	if err != nil {
		return ctrl.Result{}, err
	}

	if hasPodsChanged(founds.Items, pods) {
		log.Info("MyStatefulSet spec has changed, updating Pods")
		if err := syncMyStatefulset(r.Client, founds.Items, pods); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func hasPodsChanged(founds, pods []v1.Pod) bool {
	// 直接 DeepEqual 会有问题，因为存在时间戳、status配置不一致，需要忽略，这里为了简单先这么处理
	// 1. 对比pods数量
	// 2. 遍历对比pod配置
	return reflect.DeepEqual(founds, pods)
}

func syncMyStatefulset(cli client.Client, founds, pods []v1.Pod) error {

	// 1. 遍历pods, 从index 倒序遍历，检查配置是否有误
	// 2. 配置有误的cli.Delete
	// 3. 正序创建cli.Create

	// 4. list pvc
	// 5. 根据遍历 pod 检查pvc绑定与否
	// 6. 根据 instance.Spec.VolumeClaimTemplates 创建pvc
	return nil
}

func podsForMyStatefulSet(instance *workloadjojotongiov1.MyStatefulSet, scheme *runtime.Scheme) ([]v1.Pod, error) {
	replicas := 0
	if instance.Spec.Replicas != nil {
		replicas = int(*instance.Spec.Replicas)
	}
	pods := make([]v1.Pod, replicas)
	for i := 0; i < replicas; i++ {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", instance.Name, i),
				Namespace: instance.Namespace,
			},
			Spec: v1.PodSpec{
				// 根据pod template 填入此spec
				// 此处省略，用k8s库里面的StatefulSetController应该可以迅速实现
			},
		}

		if err := ctrl.SetControllerReference(instance, pod, scheme); err != nil {
			return nil, err
		}
		pods = append(pods, *pod)
	}
	return pods, nil
}

func addDefaultFields(in *workloadjojotongiov1.MyStatefulSet) error {
	if in.Spec.Replicas == nil {
		var r int32 = 1
		in.Spec.Replicas = &r
	}
	return nil
}

func (r *MyStatefulSetReconciler) finalizeMyStatefulSet(log logr.Logger, instance *workloadjojotongiov1.MyStatefulSet) error {
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MyStatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadjojotongiov1.MyStatefulSet{}).
		Complete(r)
}
