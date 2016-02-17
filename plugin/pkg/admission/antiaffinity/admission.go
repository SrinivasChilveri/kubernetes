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
	"fmt"
	"io"
	"strings"

	"k8s.io/kubernetes/pkg/admission"
	"k8s.io/kubernetes/pkg/api"
	apierrors "k8s.io/kubernetes/pkg/api/errors"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
)

func init() {
	admission.RegisterPlugin("InterPodAntiAffinityDeny", func(client clientset.Interface, config io.Reader) (admission.Interface, error) {
		return NewInterPodAntiAffinity(client), nil
	})
}

// plugin contains the client used by the admission controller
type plugin struct {
	*admission.Handler
	client clientset.Interface
}

// NewInterPodAntiAffinity creates a new instance of the InterPodAntiAffinityDeny admission controller
func NewInterPodAntiAffinity(client clientset.Interface) admission.Interface {
	return &plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
		client:  client,
	}
}

// Admit will deny any pod that defines AntiAffinity topology key other than "node" in requiredDuringSchedulingRequiredDuringExecution.
func (p *plugin) Admit(a admission.Attributes) (err error) {
	if a.GetResource() != api.Resource("pods") {
		return nil
	}
	pod, ok := a.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.PodAntiAffinity != nil {
		var podAntiAffinityTerms []api.PodAffinityTerm
		if len(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
			podAntiAffinityTerms = pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution
		}
		if len(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			podAntiAffinityTerms = append(podAntiAffinityTerms, pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		}
		for _, v := range podAntiAffinityTerms {
			if strings.ToLower(v.TopologyKey) != "node" {
				return apierrors.NewForbidden(a.GetResource(), pod.Name, fmt.Errorf("Affinity.PodAntiAffinity has TopologyKey other than node is forbidden"))
			}
		}
	}
	return nil
}
