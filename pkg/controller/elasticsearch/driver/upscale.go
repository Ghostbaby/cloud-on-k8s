// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/observer"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/version/zen1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/version/zen2"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	appsv1 "k8s.io/api/apps/v1"
)

type upscaleCtx struct {
	parentCtx     context.Context
	k8sClient     k8s.Client
	es            esv1.Elasticsearch
	observedState observer.State
	esState       ESState
	expectations  *expectations.Expectations
}

type UpscaleResults struct {
	ActualStatefulSets sset.StatefulSetList
	Requeue            bool
}

// HandleUpscaleAndSpecChanges reconciles expected NodeSet resources.
// It does:
// - create any new StatefulSets
// - update existing StatefulSets specification, to be used for future pods rotation
// - upscale StatefulSet for which we expect more replicas
// - limit master node creation to one at a time
// It does not:
// - perform any StatefulSet downscale (left for downscale phase)
// - perform any pod upgrade (left for rolling upgrade phase)
func HandleUpscaleAndSpecChanges(
	ctx upscaleCtx,
	actualStatefulSets sset.StatefulSetList,
	expectedResources nodespec.ResourcesList,
) (UpscaleResults, error) {
	results := UpscaleResults{}
	// adjust expected replicas to control nodes creation and deletion
	adjusted, err := adjustResources(ctx, actualStatefulSets, expectedResources)
	if err != nil {
		return results, fmt.Errorf("adjust resources: %w", err)
	}
	// reconcile all resources
	for _, res := range adjusted {
		if err := settings.ReconcileConfig(ctx.k8sClient, ctx.es, res.StatefulSet.Name, res.Config); err != nil {
			return results, fmt.Errorf("reconcile config: %w", err)
		}
		if _, err := common.ReconcileService(ctx.parentCtx, ctx.k8sClient, &res.HeadlessService, &ctx.es); err != nil {
			return results, fmt.Errorf("reconcile service: %w", err)
		}
		recreateSset := false
		if actualSset, exists := actualStatefulSets.GetByName(res.StatefulSet.Name); exists {
			err := resizePVCs(ctx.k8sClient, res.StatefulSet, actualSset)
			if err != nil {
				return results, fmt.Errorf("resize PVCs: %w", err)
			}
			recreateSset, err = needsRecreate(ctx.k8sClient, res.StatefulSet, actualSset)
			if err != nil {
				return results, fmt.Errorf("recreate StatefulSet: %w", err)
			}
			if recreateSset {
				log.Info("Deleting StatefulSet to account for resized PVCs, it will be recreated automatically",
					"namespace", actualSset.Namespace, "statefulset_name", actualSset.Name)

				opts := client.DeleteOptions{}
				// ensure Pods are not also deleted
				orphanPolicy := metav1.DeletePropagationOrphan
				opts.PropagationPolicy = &orphanPolicy
				// ensure we are not deleting based on wrong assumptions
				opts.Preconditions = &metav1.Preconditions{
					UID:             &actualSset.UID,
					ResourceVersion: &actualSset.ResourceVersion,
				}
				if err := ctx.k8sClient.Delete(&actualSset, &opts); err != nil {
					return results, fmt.Errorf("delete StatefulSet for PVC resize: %w", err)
				}
				// it's safer to requeue: the StatefulSet will be recreated at next reconciliations
				results.Requeue = true
				continue
			}
		}
		reconciled, err := sset.ReconcileStatefulSet(ctx.k8sClient, ctx.es, res.StatefulSet, ctx.expectations)
		if err != nil {
			return results, fmt.Errorf("reconcile StatefulSet: %w", err)
		}
		// update actual with the reconciled ones for next steps to work with up-to-date information
		actualStatefulSets = actualStatefulSets.WithStatefulSet(reconciled)
	}
	results.ActualStatefulSets = actualStatefulSets
	return results, nil
}

func adjustResources(
	ctx upscaleCtx,
	actualStatefulSets sset.StatefulSetList,
	expectedResources nodespec.ResourcesList,
) (nodespec.ResourcesList, error) {
	upscaleState := newUpscaleState(ctx, actualStatefulSets, expectedResources)
	adjustedResources := make(nodespec.ResourcesList, 0, len(expectedResources))
	for _, nodeSpecRes := range expectedResources {
		adjusted, err := adjustStatefulSetReplicas(ctx.k8sClient, upscaleState, actualStatefulSets, *nodeSpecRes.StatefulSet.DeepCopy())
		if err != nil {
			return nil, err
		}
		nodeSpecRes.StatefulSet = adjusted
		adjustedResources = append(adjustedResources, nodeSpecRes)
	}
	// adapt resources configuration to match adjusted replicas
	if err := adjustZenConfig(ctx.k8sClient, ctx.es, adjustedResources); err != nil {
		return nil, fmt.Errorf("adjust discovery config: %w", err)
	}
	return adjustedResources, nil
}

func adjustZenConfig(k8sClient k8s.Client, es esv1.Elasticsearch, resources nodespec.ResourcesList) error {
	// patch configs to consider zen1 minimum master nodes
	if err := zen1.SetupMinimumMasterNodesConfig(k8sClient, es, resources); err != nil {
		return err
	}
	// patch configs to consider zen2 initial master nodes
	if err := zen2.SetupInitialMasterNodes(es, k8sClient, resources); err != nil {
		return err
	}
	return nil
}

// adjustStatefulSetReplicas updates the replicas count in expected according to
// what is allowed by the upscaleState, that may be mutated as a result.
func adjustStatefulSetReplicas(
	k8sClient k8s.Client,
	upscaleState *upscaleState,
	actualStatefulSets sset.StatefulSetList,
	expected appsv1.StatefulSet,
) (appsv1.StatefulSet, error) {
	expectedReplicas := sset.GetReplicas(expected)

	actual, alreadyExists := actualStatefulSets.GetByName(expected.Name)
	actualReplicas := sset.GetReplicas(actual)
	if !alreadyExists {
		// the StatefulSet may have been deleted to handle PVC resize,
		// in which case we should consider the number of existing orphan Pods
		// when re-creating it
		pods, err := sset.GetActualPodsForStatefulSet(k8sClient, k8s.ExtractNamespacedName(&expected))
		if err != nil {
			return appsv1.StatefulSet{}, err
		}
		actualReplicas = int32(len(pods))
		if actualReplicas > 0 {
			log.V(1).Info("Adjusting StatefulSet replicas to based on orphan Pods", "replicas", actualReplicas, "namespace", expected.Namespace, "statefulset_name", expected.Name)
		}
	}

	if actualReplicas < expectedReplicas {
		return upscaleState.limitNodesCreation(actual, expected)
	}

	if alreadyExists && expectedReplicas < actualReplicas {
		// this is a downscale.
		// We still want to update the sset spec to the newest one, but leave scaling down as it's done later.
		nodespec.UpdateReplicas(&expected, actual.Spec.Replicas)
	}

	return expected, nil
}

// isReplicaIncrease returns true if expected replicas are higher than actual replicas.
func isReplicaIncrease(actual appsv1.StatefulSet, expected appsv1.StatefulSet) bool {
	return sset.GetReplicas(expected) > sset.GetReplicas(actual)
}

// needsRecreate returns true if the StatefulSet needs to be re-created to account for storage expansion.
func needsRecreate(k8sClient k8s.Client, expectedSset appsv1.StatefulSet, actualSset appsv1.StatefulSet) (bool, error) {
	toResize, err := getClaimsToResize(k8sClient, expectedSset.Spec.VolumeClaimTemplates, actualSset.Spec.VolumeClaimTemplates)
	if err != nil {
		return false, err
	}
	return len(toResize) > 0, nil
}

//func recreateStatefulSet() error {
//	if resizedCount > 0 {
//		log.Info("Deleting StatefulSet to account for resized PVCs, it will be recreated automatically",
//			"namespace", expectedSset.Namespace, "statefulset_name", expectedSset.Name, "resized_count", resizedCount)
//		orphanPolicy := metav1.DeletePropagationOrphan
//		opts := &client.DeleteOptions{
//			// ensure Pods owned by that StatefulSet are not also deleted
//			PropagationPolicy: &orphanPolicy,
//			// ensure we are not deleting based on wrong assumptions
//			Preconditions: &metav1.Preconditions{
//				UID:             &actualSset.UID,
//				ResourceVersion: &actualSset.ResourceVersion,
//			},
//		}
//		return k8sClient.Delete(&actualSset, opts)
//	}
//	return nil
//}
