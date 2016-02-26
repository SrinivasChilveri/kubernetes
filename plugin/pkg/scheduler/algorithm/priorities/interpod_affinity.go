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
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

// RequiredDuringScheduling affinity is not symmetric, and there is an implicit PreferredDuringScheduling affinity rule
// corresponding to every RequiredDuringScheduling affinity rule.
// hardPodAffinityImplicitWeight represents the weight of implicit PreferredDuringScheduling affinity rule.

const hardPodAffinityImplicitWeight int = 1

type InterPodAffinity struct {
	nodeLister algorithm.NodeLister
}

func NewInterPodAffinityPriority(nodeLister algorithm.NodeLister) algorithm.PriorityFunction {
	interPodAffinity := &InterPodAffinity{
		nodeLister: nodeLister,
	}
	return interPodAffinity.CalculateInterPodAffinityPriority
}

// if the topology key length is more than zero then just calculate the weight based on node label value as key
// other wise all the node label values has same weight
func calculatePriority(node *api.Node, weight int, topologyKey string, counts map[string]int) {
	if len(topologyKey) == 0 && node.Labels != nil {
		for _, index := range node.Labels {
			counts[index] += weight
		}
	} else if len(topologyKey) > 0 && node.Labels != nil && len(node.Labels[topologyKey]) > 0 {
		index := node.Labels[topologyKey]
		counts[index] += weight
	}
	return
}

// checkAnyPodThatMatchPodAffinityTermAndGetPriority first filters the given pods by given namespaces from podAffinityTerm
// then returns true if any filtered pod matches the given podAffinityTerm, calculates the num of matches & unmatches ,
// then retruns the same in different conditions
func checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod *api.Pod, existingPods []*api.Pod, node *api.Node, podAffinityTerm api.PodAffinityTerm) (int, bool, error) {
	labelSelector, err := unversioned.LabelSelectorAsSelector(podAffinityTerm.LabelSelector)
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
	matchedCount := 0
	flag := false
	filteredPods := predicates.FilterPodsByNameSpaces(names, existingPods)

	// return true if any pod matches the podAffinityTerm
	for _, filteredPod := range filteredPods {
		if labelSelector.Matches(labels.Set(filteredPod.Labels)) {
			if predicates.CheckTopologyKey(podAffinityTerm.TopologyKey, node) {
				flag = true
				matchedCount++
			}
		}
	}
	return matchedCount, flag, nil
}

// compute a sum by iterating through the elements of weightedPodAffinityTerm and adding
// "weight" to the sum if the corresponding PodAffinityTerm is satisfied for
// that node; the node(s) with the highest sum are the most preferred.
// Symmetry need to be considered for preferredDuringSchedulingIgnoredDuringExecution from podAffinity & podAntiAffinity,
// symmetry need to be considered for hard requirements from podAffinity
func (s *InterPodAffinity) CalculateInterPodAffinityPriority(pod *api.Pod, nodeNameToInfo map[string]*schedulercache.NodeInfo, nodeLister algorithm.NodeLister) (schedulerapi.HostPriorityList, error) {
	topologyCounts := map[string]int{}
	nodes, err := nodeLister.List()
	if err != nil {
		return nil, err
	}
	affinity, err := api.GetAffinityFromPodAnnotations(pod.Annotations)
	if err != nil {
		return nil, err
	}
	// preferredDuringSchedulingIgnoredDuringExecution from podAffinity
	if affinity.PodAffinity != nil {
		// match weightedPodAffinityTerm by Term from preferredDuringSchedulingIgnoredDuringExecution
		for _, weightedTerm := range affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if weightedTerm.Weight == 0 {
				continue
			}
			for _, node := range nodes.Items {
				// get the pods which are there in that particular node
				podsInNode := nodeNameToInfo[node.Name].Pods()
				podsMatchedCount, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod, podsInNode, &node, weightedTerm.PodAffinityTerm)
				if err != nil {
					return nil, err
				} else if bMatches {
					calculatePriority(&node, weightedTerm.Weight*podsMatchedCount, weightedTerm.PodAffinityTerm.TopologyKey, topologyCounts)
				}
			}
		}
	}

	// preferredDuringSchedulingIgnoredDuringExecution from podAntiAffinity
	if affinity.PodAntiAffinity != nil {
		// match weightedPodAffinityTerm by Term from PreferredDuringSchedulingIgnoredDuringExecution
		for _, weightedTerm := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if weightedTerm.Weight == 0 {
				continue
			}
			for _, node := range nodes.Items {
				// get the pods which are there in that particular node
				podsInNode := nodeNameToInfo[node.Name].Pods()
				podsMatchedCount, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(pod, podsInNode, &node, weightedTerm.PodAffinityTerm)
				// as an optimization pod matches and topology-key matches then simply adding the (0 - weightedTerm.Weight*podsMatchedCount)
				if err != nil {
					return nil, err
				} else if bMatches {
					calculatePriority(&node, (0 - weightedTerm.Weight*podsMatchedCount), weightedTerm.PodAffinityTerm.TopologyKey, topologyCounts)
				}
			}
		}
	}

	// Symmetry for hard requirements of podAffinity & preferredDuringSchedulingIgnoredDuringExecution from podAffinity and podAntiAffinity
	// create list of pods by using the input pod for symmetry check
	podsForSymmetryChecking := []*api.Pod{pod}
	for _, node := range nodes.Items {
		// get the pods which are there in that particular node
		podsInNode := nodeNameToInfo[node.Name].Pods()

		for _, podInNode := range podsInNode {
			affinity, err := api.GetAffinityFromPodAnnotations(podInNode.Annotations)
			if err != nil {
				return nil, err
			}
			// Symmetry for hard requirements of podAffinity
			if affinity.PodAffinity != nil {
				var podAffinityTerms []api.PodAffinityTerm
				if len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
					podAffinityTerms = affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
				}
				// TODO: Uncomment this block when implement RequiredDuringSchedulingRequiredDuringExecution.
				//if len(affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
				//	podAffinityTerms = append(podAffinityTerms, affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
				//}
				for _, podAffinityTerm := range podAffinityTerms {
					podsMatchedCount, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(podInNode, podsForSymmetryChecking, &node, podAffinityTerm)
					if err != nil {
						return nil, err
					} else if bMatches {
						calculatePriority(&node, hardPodAffinityImplicitWeight*podsMatchedCount, podAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
			// Symmetry for preferredDuringSchedulingIgnoredDuringExecution from pod affinity
			if affinity.PodAffinity != nil {
				for _, weightedTerm := range affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					podsMatchedCount, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(podInNode, podsForSymmetryChecking, &node, weightedTerm.PodAffinityTerm)
					if err != nil {
						return nil, err
					} else if bMatches {
						calculatePriority(&node, weightedTerm.Weight*podsMatchedCount, weightedTerm.PodAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
			// Symmetry for preferredDuringSchedulingIgnoredDuringExecution from pod anti affinity
			if affinity.PodAntiAffinity != nil {
				for _, weightedTerm := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					podsMatchedCount, bMatches, err := checkAnyPodThatMatchPodAffinityTermAndGetPriority(podInNode, podsForSymmetryChecking, &node, weightedTerm.PodAffinityTerm)
					// as an optimization pod matches and topology-key matches then simply adding the (0 - weightedTerm.Weight*podsMatchedCount)
					if err != nil {
						return nil, err
					} else if bMatches {
						calculatePriority(&node, (0 - weightedTerm.Weight*podsMatchedCount), weightedTerm.PodAffinityTerm.TopologyKey, topologyCounts)
					}
				}
			}
		}
	}
	var maxCount int
	var minCount int
	counts := map[string]int{}

	// convert the topology key based weights to the node name based weights
	for countIndex, count := range topologyCounts {
		for _, currnode := range nodes.Items {
			for _, labelVal := range currnode.Labels {
				if labelVal == countIndex {
					counts[currnode.Name] = counts[currnode.Name] + count
					if counts[currnode.Name] > maxCount {
						maxCount = counts[currnode.Name]
					}
					if counts[currnode.Name] < minCount {
						minCount = counts[currnode.Name]
					}
				}
			}
		}
	}
	result := []schedulerapi.HostPriority{}
	for _, node := range nodes.Items {
		fScore := float64(0)
		if (maxCount - minCount) > 0 {
			fScore = 10 * (float64(counts[node.Name]-minCount) / float64(maxCount-minCount))
		}
		result = append(result, schedulerapi.HostPriority{Host: node.Name, Score: int(fScore)})
		glog.V(10).Infof(
			"%v -> %v: InterPodAffinityPriority, Score: (%d)", pod.Name, node.Name, int(fScore),
		)
	}
	return result, nil
}
