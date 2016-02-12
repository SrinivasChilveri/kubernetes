/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package priorities

import (
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

type InterPodAffinity struct {
	nodeLister algorithm.NodeLister
}

func NewInterPodAffinityPriority(nodeLister algorithm.NodeLister) algorithm.PriorityFunction {
	interPodAffinity := &InterPodAffinity{
		nodeLister: nodeLister,
	}
	return interPodAffinity.CalculateInterPodAffinityPriority
}

// if the topology key length is more than zero then just calculate the weigt based on node label value with key as topology key
// other wise all the node label values has same weight
func calculatePriorityBasedOnTopologyKeyForAffinity(node *api.Node, weight int, topologyKey string, counts map[string]int) {
	if len(topologyKey) <= 0 && node.Labels != nil {
		for _, index := range node.Labels {
			counts[index] += weight
		}
	} else if len(topologyKey) > 0 && node.Labels != nil && len(node.Labels[topologyKey]) > 0 {
		index := node.Labels[topologyKey]
		counts[index] += weight
	}
	return
}

// if the topology key is empty then all the node label values has same weight
// anti affinity is success because of the pod topology key doesn't match the node's Label key then all Label values has same weight
// anti affinity is success because of the pod doesn't match the pods in the node then all Label values has same weight
func calculatePriorityBasedOnTopologyKeyForAntiAffinity(node *api.Node, weight int, topologyKey string, counts map[string]int) {
	for _, index := range node.Labels {
		counts[index] += weight
	}
	return
}

// checkAnyPodThatMatchPodAffinityTermAndGetPriority first filters the given pods by given namespaces from podAffinityTerm
// then returns true if any filtered pod matches the given podAffinityTerm, calculates the num of matches & unmatches ,
// then retruns the same in different conditions
func checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod *api.Pod, existingPods []*api.Pod, node *api.Node, podAffinityTerm api.PodAffinityTerm) (int, bool, error) {
	podSelector, err := api.LabelSelectorAsSelector(podAffinityTerm.LabelSelector)
	if err != nil {
		return 0, false, err
	}
	// filter the pods based on namespaces from podAffinityTerm
	// if the NameSpaces is nil considers the given pod's namespace
	// if the Namespaces is empty list then considers all the name spaces
	names := sets.String{}
	if podAffinityTerm.Namespaces == nil {
		names.Insert(pod.Namespace)
	} else if len(podAffinityTerm.Namespaces) != 0 {
		for _, nameSpace := range podAffinityTerm.Namespaces {
			names.Insert(nameSpace.Namespace)
		}
	}
	matched := 1
	unmatched := 1
	flag := false
	filterdPods := predicates.FilterPodsByNameSpaces(names, existingPods)

	// return true if any pod matches the podAffinityTerm
	for _, currpod := range filterdPods {
		if podSelector.Matches(labels.Set(currpod.Labels)) {
			if predicates.CheckTopologyKey(podAffinityTerm.TopologyKey, node) {
				flag = true
				matched++
			} else {
				unmatched++
			}

		} else {
			unmatched++
		}

	}
	if flag {
		return matched, flag, nil
	}

	return unmatched, false, nil
}

// compute a sum by iterating through the elements of weightedPodAffinityTerm and adding
// "weight" to the sum if the corresponding PodAffinityTerm is satisfied for
// that node; the node(s) with the highest sum are the most preferred.
// Symmetry need to be considered for preferredDuringSchedulingIgnoredDuringExecution from podAffinity & podAntiAffinity,
// symmetry need to be considered for hard requirements from podAffinity
func (s *InterPodAffinity) CalculateInterPodAffinityPriority(pod *api.Pod, nodeNameToInfo map[string]*schedulercache.NodeInfo, nodeLister algorithm.NodeLister) (schedulerapi.HostPriorityList, error) {
	var maxCount int
	counts := map[string]int{}
	topologyCounts := map[string]int{}

	nodes, err := nodeLister.List()
	if err != nil {
		return nil, err
	}
	// preferredDuringSchedulingIgnoredDuringExecution from podAffinity
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.PodAffinity != nil {
		// match weightedPodAffinityTerm by Term from preferredDuringSchedulingIgnoredDuringExecution
		for _, weightedpodAffinityTerm := range pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if weightedpodAffinityTerm.Weight == 0 {
				continue
			}
			for _, node := range nodes.Items {
				//get the pods which are there in that particular node
				podsInNode := nodeNameToInfo[node.Name].Pods()
				multiple, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod, podsInNode, &node, weightedpodAffinityTerm.PodAffinityTerm)
				if err == nil && bMatches {
					calculatePriorityBasedOnTopologyKeyForAffinity(&node, weightedpodAffinityTerm.Weight*multiple,
						weightedpodAffinityTerm.PodAffinityTerm.TopologyKey, topologyCounts)
				}
			}
		}
	}

	// preferredDuringSchedulingIgnoredDuringExecution from podAntiAffinity
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.PodAntiAffinity != nil {
		// match weightedPodAffinityTerm by Term from PreferredDuringSchedulingIgnoredDuringExecution
		for _, weightedpodAffinityTerm := range pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if weightedpodAffinityTerm.Weight == 0 {
				continue
			}
			for _, node := range nodes.Items {
				//get the pods which are there in that particular node
				podsInNode := nodeNameToInfo[node.Name].Pods()
				multiple, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod, podsInNode, &node, weightedpodAffinityTerm.PodAffinityTerm)
				if err == nil && !bMatches {
					calculatePriorityBasedOnTopologyKeyForAntiAffinity(&node, weightedpodAffinityTerm.Weight*multiple,
						weightedpodAffinityTerm.PodAffinityTerm.TopologyKey, topologyCounts)
				}
			}
		}
	}

	// Symmetry for hard requirements of podAffinity & preferredDuringSchedulingIgnoredDuringExecution from podAffinity and podAntiAffinity
	// create list of pods by using the input pod for symmetry check
	var pods []*api.Pod
	pods = append(pods, pod)
	for _, node := range nodes.Items {
		//get the pods which are there in that particular node
		podsInNode := nodeNameToInfo[node.Name].Pods()

		for _, currpod := range podsInNode {
			// Symmetry for hard requirements of podAffinity
			if currpod.Spec.Affinity != nil && currpod.Spec.Affinity.PodAffinity != nil {
				var podAffinityTerms []api.PodAffinityTerm
				if len(currpod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
					podAffinityTerms = currpod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution
				}
				if len(currpod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
					podAffinityTerms = append(podAffinityTerms, currpod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
				}
				for _, podAffinityTerm := range podAffinityTerms {
					multiple, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(currpod, pods, &node, podAffinityTerm)
					if err == nil && bMatches {
						calculatePriorityBasedOnTopologyKeyForAffinity(&node, 1*multiple,
							podAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
			// Symmetry for preferredDuringSchedulingIgnoredDuringExecution from pod affinity
			if currpod.Spec.Affinity != nil && currpod.Spec.Affinity.PodAffinity != nil {
				for _, weightedpodAffinityTerm := range currpod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					multiple, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(currpod, pods, &node, weightedpodAffinityTerm.PodAffinityTerm)
					if err == nil && bMatches {
						calculatePriorityBasedOnTopologyKeyForAffinity(&node, weightedpodAffinityTerm.Weight*multiple,
							weightedpodAffinityTerm.PodAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
			// Symmetry for preferredDuringSchedulingIgnoredDuringExecution from pod anti affinity
			if currpod.Spec.Affinity != nil && currpod.Spec.Affinity.PodAntiAffinity != nil {
				for _, weightedpodAffinityTerm := range currpod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					multiple, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(currpod, pods, &node, weightedpodAffinityTerm.PodAffinityTerm)
					if err == nil && !bMatches {
						calculatePriorityBasedOnTopologyKeyForAntiAffinity(&node, weightedpodAffinityTerm.Weight*multiple,
							weightedpodAffinityTerm.PodAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
		}
	}

	//convert the topology key based weights to the node name based weights
	for topologyKey, count := range topologyCounts {
		for _, currnode := range nodes.Items {
			for _, labelVal := range currnode.Labels {
				if labelVal == topologyKey {
					counts[currnode.Name] = counts[currnode.Name] + count
					if counts[currnode.Name] > maxCount {
						maxCount = counts[currnode.Name]
					}
				}
			}
		}
	}
	result := []schedulerapi.HostPriority{}
	for _, node := range nodes.Items {

		fScore := float64(0)
		if maxCount > 0 {
			fScore = 10 * (float64(counts[node.Name]) / float64(maxCount))
		}
		result = append(result, schedulerapi.HostPriority{Host: node.Name, Score: int(fScore)})
		glog.V(10).Infof(
			"%v -> %v: InterPodAffinityPriority, Score: (%d)", pod.Name, node.Name, int(fScore),
		)
	}
	return result, nil
}
