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
	"reflect"
	"testing"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

func TestInterPodAffinityPriority(t *testing.T) {
	labels1 := map[string]string{
		"region": "China",
	}
	labels2 := map[string]string{
		"region": "India",
	}
	labels3 := map[string]string{
		"az": "az1",
	}
	labels4 := map[string]string{
		"node": "node1",
	}
	labels5 := map[string]string{
		"region": "China",
		"az":     "az1",
	}
	podLabel1 := map[string]string{
		"security": "S1",
	}
	podLabel2 := map[string]string{
		"security": "S2",
	}
	labelselector1 := api.LabelSelector{
		MatchExpressions: []api.LabelSelectorRequirement{
			{
				Key: "security", Operator: api.LabelSelectorOpIn, Values: []string{"S1"},
			},
		},
	}
	labelselector2 := api.LabelSelector{
		MatchExpressions: []api.LabelSelectorRequirement{
			{
				Key: "security", Operator: api.LabelSelectorOpIn, Values: []string{"S2"},
			},
		},
	}
	labelselector3 := api.LabelSelector{
		MatchExpressions: []api.LabelSelectorRequirement{
			{
				Key: "security", Operator: api.LabelSelectorOpNotIn, Values: []string{"S1"},
			},
			{
				Key: "security", Operator: api.LabelSelectorOpIn, Values: []string{"S2"},
			},
		},
	}
	labelselector4 := api.LabelSelector{
		MatchExpressions: []api.LabelSelectorRequirement{
			{
				Key:      "security",
				Operator: api.LabelSelectorOpExists,
			},
			{
				Key:      "wrongkey",
				Operator: api.LabelSelectorOpDoesNotExist,
			},
		},
	}

	//considered only preferredDuringSchedulingIgnoredDuringExecution in pod affinity
	affinity1 := api.Affinity{
		PodAffinity: &api.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 5,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector1,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "region",
					},
				},
			},
		},
	}
	affinity2 := api.Affinity{
		PodAffinity: &api.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 6,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector2,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "region",
					},
				},
			},
		},
	}
	affinity3 := api.Affinity{
		PodAffinity: &api.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 8,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector3,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "region",
					},
				},
				{Weight: 2,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector4,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "region",
					},
				},
			},
		},
	}
	affinity4 := api.Affinity{
		PodAffinity: &api.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
				LabelSelector: &labelselector1,
				Namespaces:    []api.Namespace{},
				TopologyKey:   "region",
			}},
			RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{
				LabelSelector: &labelselector4,
				Namespaces:    []api.Namespace{},
				TopologyKey:   "region",
			}},
		},
	}
	antiaffinity1 := api.Affinity{
		PodAntiAffinity: &api.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 5,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector1,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "az",
					},
				},
			},
		},
	}

	antiaffinity2 := api.Affinity{
		PodAntiAffinity: &api.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 5,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector2,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "az",
					},
				},
			},
		},
	}
	affinity5 := api.Affinity{
		PodAffinity: &api.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 8,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector1,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "region",
					},
				},
			},
		},
		PodAntiAffinity: &api.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
				{Weight: 5,
					PodAffinityTerm: api.PodAffinityTerm{
						LabelSelector: &labelselector2,
						Namespaces:    []api.Namespace{},
						TopologyKey:   "az",
					},
				},
			},
		},
	}
	tests := []struct {
		pod          *api.Pod
		pods         []*api.Pod
		nodes        []api.Node
		expectedList schedulerapi.HostPriorityList
		test         string
	}{
		{
			pod: &api.Pod{Spec: api.PodSpec{Affinity: nil}},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels3}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 0}, {"machine2", 0}, {"machine3", 0}},
			test:         "all machines are same priority as Affinity is nil",
		},
		// the nodes(machine1) that have the label {"region": "China"} (match the topology key) and that have existing pods that match the labelSelector get high score
		// the nodes(machine3) that don't have the label {"region": "whatever the value is"} (mismatch the topology key) but that have existing pods that match the labelSelector get low score
		// the nodes (machine2) that have the label {"region": "China"} (match the topology key) but that have existing pods that mismatch the labelSelector get low score
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine3"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels3}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 0}, {"machine3", 0}},
			test: "Affinity: pod that matches topology key & pods in nodes will get high score comparing to others" +
				"which doesn;t match either pods in nodes or in topology key",
		},
		// there are 2 regions, say regionChina(machine1,machine3,machine4) and regionIndia(machine2,machine5), both regions have nodes that match the preference.
		// But there are more nodes(actually more existing pods) in regionChina that match the preference than regionIndia.
		// Then, nodes in regionChina get higher score than nodes in regionIndia, and all the nodes in regionChina should get a same score(high score),
		// while all the nodes in regionIndia should get another same score(low score).
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity2}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine3"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine4"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine5"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine4", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine5", Labels: labels2}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 5}, {"machine3", 10}, {"machine4", 10}, {"machine5", 5}},
			test:         "Affinity: nodes in one region has more matching pods comparing to other reqion,so the region which has more macthes will get high score",
		},
		// Test with the different operators and values for pod affinity scheduling preference, including some match failures.
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity3}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine3"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels3}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 2}, {"machine2", 10}, {"machine3", 0}},
			test:         "Affinity: different Label operators and values for pod affinity scheduling preference, including some match failures ",
		},
		// Test the symmetry cases for affinity, the difference between affinity and symmetry is not the pod wants to run together with some existing pods,
		// but the existing pods have the inter pod affinity preference while the pod to schedule satisfy the preference.
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: ""}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1", Affinity: &affinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2", Affinity: &affinity2}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels3}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 0}, {"machine2", 10}, {"machine3", 0}},
			test:         "Affinity symmetry: considred only the preferredDuringSchedulingIgnoredDuringExecution in pod affinity symmetry",
		},
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: ""}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1", Affinity: &affinity4}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2", Affinity: &affinity4}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels3}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 10}, {"machine3", 0}},
			test:         "Affinity symmetry: considred both the RequiredDuringSchedulingRequiredDuringExecution & RequiredDuringSchedulingIgnoredDuringExecution in pod affinity symmetry",
		},
		// The pod to schedule prefer to stay away from some existing pods at node level
		// the nodes that have the label {"node": "bar"} (match the topology key) and that have existing pods that match the labelSelector get low score
		// the nodes that don't have the label {"node": "whatever the value is"} (mismatch the topology key) but that have existing pods that match the labelSelector get high score
		// the nodes that have the label {"node": "bar"} (match the topology key) but that have existing pods that mismatch the labelSelector get high score
		// there are 2 nodes, say node1 and node2, both nodes have pods that match the labelSelector and have topology-key in node.Labels.
		// But there are more pods on node1 that match the preference than node2. Then, node1 get a lower score than node2.
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &antiaffinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels3}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels1}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 0}, {"machine2", 10}},
			test:         "Anti Affinity: pod that doesnot match existing pods in node will get high score ",
		},
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &antiaffinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels3}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels1}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 0}, {"machine2", 10}},
			test:         "Anti Affinity: pod that does not matches topology key & matches the pods in nodes will get more score comparing to others ",
		},
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &antiaffinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 6}, {"machine2", 10}},
			test:         "Anti Affinity: one node has more matching pods comparing to other node,so the node which has more unmacthes will get high score",
		},
		// Test the symmetry cases for anti affinity
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: ""}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1", Affinity: &antiaffinity2}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2", Affinity: &antiaffinity1}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels3}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels1}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 0}, {"machine2", 10}},
			test:         "Anti Affinity symmetry:the existing pods in node which has anti affinity match will get high score ",
		},
		// Test both  affinity and anti-affinity
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity5}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels4}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 3}},
			test:         "Affinity and Anti Affinity: considered only preferredDuringSchedulingIgnoredDuringExecution in both pod affinity & anti affinity",
		},
		// Combined cases considering both affinity and anti-affinity, the pod to schedule and existing pods have the same labels (they are in the same RC/service),
		// the pod prefer to run together with its brother pods in the same region, but wants to stay away from them at node level,
		// so that all the pods of a RC/service can stay in a same region but trying to separate with each other
		// machine-1,machine-3,machine-4 are in ChinaRegion others machin-2,machine-5 are in IndiaRegion
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity5}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine1"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine3"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine3"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
				{Spec: api.PodSpec{NodeName: "machine4"}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine5"}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels5}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels2}},
				{ObjectMeta: api.ObjectMeta{Name: "machine3", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine4", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine5", Labels: labels2}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 1}, {"machine3", 8}, {"machine4", 8}, {"machine5", 1}},
			test:         "Affinity and Anti Affinity: considering both affinity and anti-affinity, the pod to schedule and existing pods have the same labels",
		},
		// Consider Affinity,Anti Affinity and symmetry
		{
			pod: &api.Pod{Spec: api.PodSpec{NodeName: "", Affinity: &affinity5}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
			pods: []*api.Pod{{Spec: api.PodSpec{NodeName: "machine1", Affinity: &affinity5}, ObjectMeta: api.ObjectMeta{Labels: podLabel1}},
				{Spec: api.PodSpec{NodeName: "machine2", Affinity: &affinity5}, ObjectMeta: api.ObjectMeta{Labels: podLabel2}},
			},
			nodes: []api.Node{
				{ObjectMeta: api.ObjectMeta{Name: "machine1", Labels: labels1}},
				{ObjectMeta: api.ObjectMeta{Name: "machine2", Labels: labels4}},
			},
			expectedList: []schedulerapi.HostPriority{{"machine1", 10}, {"machine2", 3}},
			test:         "Affinity and Anti Affinity and symmetry: considered only preferredDuringSchedulingIgnoredDuringExecution in both pod affinity & anti affinity & symmetry",
		},
	}
	for _, test := range tests {
		nodeNameToInfo := schedulercache.CreateNodeNameToInfoMap(test.pods)
		interPodAffinity := InterPodAffinity{nodeLister: algorithm.FakeNodeLister(api.NodeList{Items: test.nodes})}
		list, err := interPodAffinity.CalculateInterPodAffinityPriority(test.pod, nodeNameToInfo, algorithm.FakeNodeLister(api.NodeList{Items: test.nodes}))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(test.expectedList, list) {
			t.Errorf("%s: expected %#v, got %#v", test.test, test.expectedList, list)
		}
	}
}
