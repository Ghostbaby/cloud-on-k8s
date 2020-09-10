// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/pkg/utils/stringsutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClaimsToResize map[string]resource.Quantity

// GarbageCollectPVCs ensures PersistentVolumeClaims created for the given es resource are deleted
// when no longer used, since this is not done automatically by the StatefulSet controller.
// Related issue in the k8s repo: https://github.com/kubernetes/kubernetes/issues/55045
// PVCs that are not supposed to exist given the actual and expected StatefulSets are removed.
// This covers:
// * leftover PVCs created for StatefulSets that do not exist anymore
// * leftover PVCs created for StatefulSets replicas that don't exist anymore (eg. downscale from 5 to 3 nodes)
func GarbageCollectPVCs(
	k8sClient k8s.Client,
	es esv1.Elasticsearch,
	actualStatefulSets sset.StatefulSetList,
	expectedStatefulSets sset.StatefulSetList,
) error {
	// PVCs are using the same labels as their corresponding StatefulSet, so we can filter on ES cluster name.
	var pvcs corev1.PersistentVolumeClaimList
	ns := client.InNamespace(es.Namespace)
	matchLabels := label.NewLabelSelectorForElasticsearch(es)
	if err := k8sClient.List(&pvcs, ns, matchLabels); err != nil {
		return err
	}
	for _, pvc := range pvcsToRemove(pvcs.Items, actualStatefulSets, expectedStatefulSets) {
		log.Info("Deleting PVC", "namespace", pvc.Namespace, "pvc_name", pvc.Name)
		if err := k8sClient.Delete(&pvc); err != nil {
			return err
		}
	}
	return nil
}

// pvcsToRemove filters the given pvcs to ones that can be safely removed based on Pods
// of actual and expected StatefulSets.
func pvcsToRemove(
	pvcs []corev1.PersistentVolumeClaim,
	actualStatefulSets sset.StatefulSetList,
	expectedStatefulSets sset.StatefulSetList,
) []corev1.PersistentVolumeClaim {
	// Build the list of PVCs from both actual & expected StatefulSets (may contain duplicate entries).
	// The list may contain PVCs for Pods that do not exist (eg. not created yet), but does not
	// consider Pods in the process of being deleted (but not deleted yet), since already covered
	// by checking expectations earlier in the process.
	// Then, just return existing PVCs that are not part of that list.
	toKeep := stringsutil.SliceToMap(append(actualStatefulSets.PVCNames(), expectedStatefulSets.PVCNames()...))
	var toRemove []corev1.PersistentVolumeClaim // nolint
	for _, pvc := range pvcs {
		if _, exists := toKeep[pvc.Name]; exists {
			continue
		}
		toRemove = append(toRemove, pvc)
	}
	return toRemove
}

func resizePVCs(k8sClient k8s.Client, expectedSset appsv1.StatefulSet, actualSset appsv1.StatefulSet) error {
	expectedClaims := expectedSset.Spec.VolumeClaimTemplates
	// TODO: should check all existing PVs, not only ones from claims to resize
	toResize, err := getClaimsToResize(k8sClient, expectedClaims, actualSset.Spec.VolumeClaimTemplates)
	if err != nil {
		return err
	}
	// update the spec of all PVCs that deserve to
	for _, claim := range toResize {
		for _, podName := range sset.PodNames(actualSset) {
			pvcName := fmt.Sprintf("%s-%s", claim.Name, podName)
			var pvc corev1.PersistentVolumeClaim
			err := k8sClient.Get(types.NamespacedName{Namespace: actualSset.Namespace, Name: pvcName}, &pvc)
			if err != nil {
				return err
			}
			if !pvc.DeletionTimestamp.IsZero() {
				// pvc is scheduled for deletion, there's no point in resizing it
				continue
			}
			claimStorage := claim.Spec.Resources.Requests.Storage()
			pvcStorage := pvc.Spec.Resources.Requests.Storage()
			if claimStorage == nil || pvcStorage == nil {
				continue
			}
			if pvcStorage.Equal(*claimStorage) {
				continue
			}
			log.Info("Resizing PVC storage requests",
				"namespace", pvc.Namespace, "pvc_name", pvcName,
				"old_value", pvcStorage.String(), "new_value", claimStorage.String())
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *claimStorage
			if err := k8sClient.Update(&pvc); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: duplicate
func getClaimMatchingName(claims []corev1.PersistentVolumeClaim, name string) *corev1.PersistentVolumeClaim {
	for i, claim := range claims {
		if claim.Name == name {
			return &claims[i]
		}
	}
	return nil
}

// getClaimsToResize inspects a StatefulSet expected claims vs. actual ones,
// and returns the list of claims that need to be resized.
func getClaimsToResize(k8sClient k8s.Client, expectedClaims []corev1.PersistentVolumeClaim, actualClaims []corev1.PersistentVolumeClaim) ([]corev1.PersistentVolumeClaim, error) {
	var claimsToResize []corev1.PersistentVolumeClaim
	for _, expectedClaim := range expectedClaims {
		actualClaim := getClaimMatchingName(actualClaims, expectedClaim.Name)
		if actualClaim == nil {
			continue
		}

		// is storage size increased?
		actualStorage := actualClaim.Spec.Resources.Requests.Storage()
		expectedStorage := expectedClaim.Spec.Resources.Requests.Storage()
		if actualStorage == nil || expectedStorage == nil {
			continue
		}
		switch expectedStorage.Cmp(*actualStorage) {
		case 0: // same size
			continue
		case -1: // decrease
			return nil, fmt.Errorf("storage requests in claim %s cannot be decreased from %s to %s", expectedClaim.Name, actualStorage.String(), expectedStorage.String())
		default: // increase
		}

		// does the storage class allow volume expansion?
		sc, err := getStorageClass(k8sClient, expectedClaim)
		if err != nil {
			return nil, err
		}
		if !allowsVolumeExpansion(sc) {
			return nil, fmt.Errorf("storage class %s does not allow volume expansion to resize claim %s", sc.Name, expectedClaim.Name)
		}

		claimsToResize = append(claimsToResize, expectedClaim)
	}
	return claimsToResize, nil
}

func getStorageClass(k8sClient k8s.Client, claim corev1.PersistentVolumeClaim) (storagev1.StorageClass, error) {
	if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName == "" {
		return getDefaultStorageClass(k8sClient)
	}
	var sc storagev1.StorageClass
	if err := k8sClient.Get(types.NamespacedName{Name: *claim.Spec.StorageClassName}, &sc); err != nil {
		return storagev1.StorageClass{}, fmt.Errorf("cannot retrieve storage class: %w", err)
	}
	return sc, nil
}

func getDefaultStorageClass(k8sClient k8s.Client) (storagev1.StorageClass, error) {
	var scs storagev1.StorageClassList
	if err := k8sClient.List(&scs); err != nil {
		return storagev1.StorageClass{}, err
	}
	for _, sc := range scs.Items {
		if isDefaultStorageClass(sc.ObjectMeta) {
			return sc, nil
		}
	}
	return storagev1.StorageClass{}, errors.New("no default storage class found")
}

func isDefaultStorageClass(obj metav1.ObjectMeta) bool {
	if len(obj.Annotations) == 0 {
		return false
	}
	if obj.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
		obj.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
		return true
	}
	return false
}

func allowsVolumeExpansion(sc storagev1.StorageClass) bool {
	return sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion
}
