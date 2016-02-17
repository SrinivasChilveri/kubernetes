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

package antiaffinity

import (
	"k8s.io/kubernetes/pkg/admission"
	"k8s.io/kubernetes/pkg/api"
	"testing"
)

// ensures the PodAntiAffinity is denied if it defines topology key anything other than node
func TestInterPodAffinityAdmission(t *testing.T) {
	handler := NewInterPodAntiAffinity(nil)
	pod := api.Pod{
		Spec: api.PodSpec{},
	}
	tests := []struct {
		affinity      api.Affinity
		errorExpected bool
	}{
		{
			affinity:      api.Affinity{},
			errorExpected: false,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{},
			},
			errorExpected: false,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{TopologyKey: "node"}},
				},
			},
			errorExpected: false,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{TopologyKey: "node"}},
					RequiredDuringSchedulingIgnoredDuringExecution:  []api.PodAffinityTerm{{TopologyKey: "node"}},
					PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{{
						Weight: 2, PodAffinityTerm: api.PodAffinityTerm{TopologyKey: "node"}},
					}},
			},
			errorExpected: false,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{TopologyKey: "node"}},
					RequiredDuringSchedulingIgnoredDuringExecution:  []api.PodAffinityTerm{{TopologyKey: "node"}},
					PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{{
						Weight: 2, PodAffinityTerm: api.PodAffinityTerm{TopologyKey: "zone"}},
					}},
			},
			errorExpected: false,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{TopologyKey: "node"}},
					RequiredDuringSchedulingIgnoredDuringExecution:  []api.PodAffinityTerm{{TopologyKey: "zone"}},
					PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{{
						Weight: 2, PodAffinityTerm: api.PodAffinityTerm{TopologyKey: "node"}},
					}},
			},
			errorExpected: true,
		},
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{{TopologyKey: "zone"}},
				},
			},
			errorExpected: true,
		},
		// list of RequiredDuringSchedulingRequiredDuringExecution middle element topology is not node
		{
			affinity: api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingRequiredDuringExecution: []api.PodAffinityTerm{
						{TopologyKey: "node"},
						{TopologyKey: "region"},
						{TopologyKey: "node,"},
					},
				},
			},
			errorExpected: true,
		},
	}
	for _, test := range tests {
		pod.Spec.Affinity = &test.affinity
		err := handler.Admit(admission.NewAttributesRecord(&pod, api.Kind("Pod"), "foo", "name", api.Resource("pods"), "", "ignored", nil))

		if test.errorExpected && err == nil {
			t.Errorf("Expected error for Anti Affinity %+v but did not get an error", test.affinity)
		}

		if !test.errorExpected && err != nil {
			t.Errorf("Unexpected error %v for AntiAffinity %+v", err, test.affinity)
		}
	}
}
func TestHandles(t *testing.T) {
	handler := NewInterPodAntiAffinity(nil)
	tests := map[admission.Operation]bool{
		admission.Update:  true,
		admission.Create:  true,
		admission.Delete:  false,
		admission.Connect: false,
	}
	for op, expected := range tests {
		result := handler.Handles(op)
		if result != expected {
			t.Errorf("Unexpected result for operation %s: %v\n", op, result)
		}
	}
}
