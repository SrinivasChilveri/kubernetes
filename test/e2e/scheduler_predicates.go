/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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

package e2e

import (
	"fmt"
	"path/filepath"
	"time"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	_ "github.com/stretchr/testify/assert"
)

// Returns a number of currently scheduled and not scheduled Pods.
func getPodsScheduled(pods *api.PodList) (scheduledPods, notScheduledPods []api.Pod) {
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != "" {
			scheduledPods = append(scheduledPods, pod)
		} else {
			notScheduledPods = append(notScheduledPods, pod)
		}
	}
	return
}

// Simplified version of RunRC, that does not create RC, but creates plain Pods and
// requires passing whole Pod definition, which is needed to test various Scheduler predicates.
func startPods(c *client.Client, replicas int, ns string, podNamePrefix string, pod api.Pod) {
	allPods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
	expectNoError(err)
	podsScheduledBefore, _ := getPodsScheduled(allPods)

	for i := 0; i < replicas; i++ {
		podName := fmt.Sprintf("%v-%v", podNamePrefix, i)
		pod.ObjectMeta.Name = podName
		pod.ObjectMeta.Labels["name"] = podName
		pod.Spec.Containers[0].Name = podName
		_, err = c.Pods(ns).Create(&pod)
		expectNoError(err)
	}

	// Wait for pods to start running.  Note: this is a functional
	// test, not a performance test, so the timeout needs to be
	// sufficiently long that it's only triggered if things are
	// completely broken vs. running slowly.
	timeout := 10 * time.Minute
	startTime := time.Now()
	currentlyScheduledPods := 0
	for len(podsScheduledBefore)+replicas != currentlyScheduledPods {
		allPods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
		expectNoError(err)
		scheduledPods := 0
		for _, pod := range allPods.Items {
			if pod.Spec.NodeName != "" {
				scheduledPods += 1
			}
		}
		currentlyScheduledPods = scheduledPods
		Logf("%v pods running", currentlyScheduledPods)
		if startTime.Add(timeout).Before(time.Now()) {
			Logf("Timed out after %v waiting for pods to start running.", timeout)
			break
		}
		time.Sleep(5 * time.Second)
	}
	Expect(currentlyScheduledPods).To(Equal(len(podsScheduledBefore) + replicas))
}

func getRequestedCPU(pod api.Pod) int64 {
	var result int64
	for _, container := range pod.Spec.Containers {
		result += container.Resources.Requests.Cpu().MilliValue()
	}
	return result
}

func verifyResult(c *client.Client, podName string, ns string) {
	allPods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
	expectNoError(err)
	scheduledPods, notScheduledPods := getPodsScheduled(allPods)

	selector := fields.Set{
		"involvedObject.kind":      "Pod",
		"involvedObject.name":      podName,
		"involvedObject.namespace": ns,
		"source":                   api.DefaultSchedulerName,
		"reason":                   "FailedScheduling",
	}.AsSelector()
	options := api.ListOptions{FieldSelector: selector}
	schedEvents, err := c.Events(ns).List(options)
	expectNoError(err)
	// If we failed to find event with a capitalized first letter of reason
	// try looking for one starting with a small one for backward compatibility.
	// If we don't do it we end up in #15806.
	// TODO: remove this block when we don't care about supporting v1.0 too much.
	if len(schedEvents.Items) == 0 {
		selector := fields.Set{
			"involvedObject.kind":      "Pod",
			"involvedObject.name":      podName,
			"involvedObject.namespace": ns,
			"source":                   "scheduler",
			"reason":                   "failedScheduling",
		}.AsSelector()
		options := api.ListOptions{FieldSelector: selector}
		schedEvents, err = c.Events(ns).List(options)
		expectNoError(err)
	}

	printed := false
	printOnce := func(msg string) string {
		if !printed {
			printed = true
			return msg
		} else {
			return ""
		}
	}

	Expect(len(notScheduledPods)).To(Equal(1), printOnce(fmt.Sprintf("Not scheduled Pods: %#v", notScheduledPods)))
	Expect(schedEvents.Items).ToNot(BeEmpty(), printOnce(fmt.Sprintf("Scheduled Pods: %#v", scheduledPods)))
}

func cleanupPods(c *client.Client, ns string) {
	By("Removing all pods in namespace " + ns)
	pods, err := c.Pods(ns).List(api.ListOptions{})
	expectNoError(err)
	opt := api.NewDeleteOptions(0)
	for _, p := range pods.Items {
		expectNoError(c.Pods(ns).Delete(p.ObjectMeta.Name, opt))
	}
}

// Waits until all existing pods are scheduled and returns their amount.
func waitForStableCluster(c *client.Client) int {
	timeout := 10 * time.Minute
	startTime := time.Now()

	allPods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
	expectNoError(err)
	scheduledPods, currentlyNotScheduledPods := getPodsScheduled(allPods)
	for len(currentlyNotScheduledPods) != 0 {
		time.Sleep(2 * time.Second)

		allPods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
		expectNoError(err)
		scheduledPods, currentlyNotScheduledPods = getPodsScheduled(allPods)

		if startTime.Add(timeout).Before(time.Now()) {
			Failf("Timed out after %v waiting for stable cluster.", timeout)
			break
		}
	}
	return len(scheduledPods)
}

var _ = Describe("SchedulerPredicates [Serial]", func() {
	var c *client.Client
	var nodeList *api.NodeList
	var systemPodsNo int
	var totalPodCapacity int64
	var RCName string
	var ns string

	AfterEach(func() {
		rc, err := c.ReplicationControllers(ns).Get(RCName)
		if err == nil && rc.Spec.Replicas != 0 {
			By("Cleaning up the replication controller")
			err := DeleteRC(c, ns, RCName)
			expectNoError(err)
		}
	})

	framework := NewFramework("sched-pred")

	BeforeEach(func() {
		c = framework.Client
		ns = framework.Namespace.Name
		nodeList = ListSchedulableNodesOrDie(c)

		// Every test case in this suite assumes that cluster add-on pods stay stable and
		// cannot be run in parallel with any other test that touches Nodes or Pods.
		// It is so because we need to have precise control on what's running in the cluster.
		systemPods, err := c.Pods(api.NamespaceSystem).List(api.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		systemPodsNo = len(systemPods.Items)

		err = waitForPodsRunningReady(api.NamespaceSystem, systemPodsNo, podReadyBeforeTimeout)
		Expect(err).NotTo(HaveOccurred())
	})

	// This test verifies that max-pods flag works as advertised. It assumes that cluster add-on pods stay stable
	// and cannot be run in parallel with any other test that touches Nodes or Pods. It is so because to check
	// if max-pods is working we need to fully saturate the cluster and keep it in this state for few seconds.
	//
	// Slow PR #13315 (8 min)
	It("validates MaxPods limit number of pods that are allowed to run [Slow]", func() {
		totalPodCapacity = 0

		for _, node := range nodeList.Items {
			podCapacity, found := node.Status.Capacity["pods"]
			Expect(found).To(Equal(true))
			totalPodCapacity += podCapacity.Value()
			Logf("Node: %v", node)
		}

		currentlyScheduledPods := waitForStableCluster(c)
		podsNeededForSaturation := int(totalPodCapacity) - currentlyScheduledPods

		By(fmt.Sprintf("Starting additional %v Pods to fully saturate the cluster max pods and trying to start another one", podsNeededForSaturation))

		startPods(c, podsNeededForSaturation, ns, "maxp", api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   "",
				Labels: map[string]string{"name": ""},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  "",
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})

		podName := "additional-pod"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "additional"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		// Wait a bit to allow scheduler to do its thing
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		verifyResult(c, podName, ns)
		cleanupPods(c, ns)
	})

	// This test verifies we don't allow scheduling of pods in a way that sum of limits of pods is greater than machines capacity.
	// It assumes that cluster add-on pods stay stable and cannot be run in parallel with any other test that touches Nodes or Pods.
	// It is so because we need to have precise control on what's running in the cluster.
	It("validates resource limits of pods that are allowed to run [Conformance]", func() {
		nodeToCapacityMap := make(map[string]int64)
		for _, node := range nodeList.Items {
			capacity, found := node.Status.Capacity["cpu"]
			Expect(found).To(Equal(true))
			nodeToCapacityMap[node.Name] = capacity.MilliValue()
		}
		waitForStableCluster(c)

		pods, err := c.Pods(api.NamespaceAll).List(api.ListOptions{})
		expectNoError(err)
		for _, pod := range pods.Items {
			_, found := nodeToCapacityMap[pod.Spec.NodeName]
			Expect(found).To(Equal(true))
			if pod.Status.Phase == api.PodRunning {
				Logf("Pod %v requesting resource %v on Node %v", pod.Name, getRequestedCPU(pod), pod.Spec.NodeName)
				nodeToCapacityMap[pod.Spec.NodeName] -= getRequestedCPU(pod)
			}
		}

		var podsNeededForSaturation int
		milliCpuPerPod := int64(500)
		for name, leftCapacity := range nodeToCapacityMap {
			Logf("Node: %v has capacity: %v", name, leftCapacity)
			podsNeededForSaturation += (int)(leftCapacity / milliCpuPerPod)
		}

		By(fmt.Sprintf("Starting additional %v Pods to fully saturate the cluster CPU and trying to start another one", podsNeededForSaturation))

		startPods(c, podsNeededForSaturation, ns, "overcommit", api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   "",
				Labels: map[string]string{"name": ""},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  "",
						Image: "gcr.io/google_containers/pause:2.0",
						Resources: api.ResourceRequirements{
							Limits: api.ResourceList{
								"cpu": *resource.NewMilliQuantity(milliCpuPerPod, "DecimalSI"),
							},
							Requests: api.ResourceList{
								"cpu": *resource.NewMilliQuantity(milliCpuPerPod, "DecimalSI"),
							},
						},
					},
				},
			},
		})

		podName := "additional-pod"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "additional"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
						Resources: api.ResourceRequirements{
							Limits: api.ResourceList{
								"cpu": *resource.NewMilliQuantity(milliCpuPerPod, "DecimalSI"),
							},
						},
					},
				},
			},
		})
		expectNoError(err)
		// Wait a bit to allow scheduler to do its thing
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		verifyResult(c, podName, ns)
		cleanupPods(c, ns)
	})

	// Test Nodes does not have any label, hence it should be impossible to schedule Pod with
	// nonempty Selector set.
	It("validates that NodeSelector is respected if not matching [Conformance]", func() {
		By("Trying to schedule Pod with nonempty NodeSelector.")
		podName := "restricted-pod"

		waitForStableCluster(c)

		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "restricted"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				NodeSelector: map[string]string{
					"label": "nonempty",
				},
			},
		})
		expectNoError(err)
		// Wait a bit to allow scheduler to do its thing
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		verifyResult(c, podName, ns)
		cleanupPods(c, ns)
	})

	It("validates that a pod with an invalid NodeAffinity is rejected [Feature:NodeAffinity]", func() {

		By("Trying to launch a pod with an invalid Affinity data.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: podName,
				Annotations: map[string]string{
					api.AffinityAnnotationKey: `
					{"nodeAffinity": {
						"requiredDuringSchedulingRequiredDuringExecution": {
							"nodeSelectorTerms": [{
								"matchExpressions": []
							}]
						},
					}}`,
				},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})

		if err == nil || !errors.IsInvalid(err) {
			Failf("Expect error of invalid, got : %v", err)
		}

		// Wait a bit to allow scheduler to do its thing if the pod is not rejected.
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		cleanupPods(c, ns)
	})

	It("validates that NodeSelector is respected if matching [Conformance]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod without a label to get a node which can launch it.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: podName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		err = c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))
		expectNoError(err)

		By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(util.NewUUID()))
		v := "42"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to relaunch the pod, now with labels.")
		labelPodName := "with-labels"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: labelPodName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  labelPodName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				NodeSelector: map[string]string{
					"kubernetes.io/hostname": nodeName,
					k: v,
				},
			},
		})
		expectNoError(err)
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

	// Test Nodes does not have any label, hence it should be impossible to schedule Pod with
	// non-nil NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
	It("validates that NodeAffinity is respected if not matching [Feature:NodeAffinity]", func() {
		By("Trying to schedule Pod with nonempty NodeSelector.")
		podName := "restricted-pod"

		waitForStableCluster(c)

		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "restricted"},
				Annotations: map[string]string{
					"scheduler.alpha.kubernetes.io/affinity": `
					{"nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": {
						"nodeSelectorTerms": [
							{
								"matchExpressions": [{
									"key": "foo",
									"operator": "In",
									"values": ["bar", "value2"]
								}]
							},
							{
								"matchExpressions": [{
									"key": "diffkey",
									"operator": "In",
									"values": ["wrong", "value2"]
								}]
							}
						]
					}}}`,
				},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		// Wait a bit to allow scheduler to do its thing
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		verifyResult(c, podName, ns)
		cleanupPods(c, ns)
	})

	// Keep the same steps with the test on NodeSelector,
	// but specify Affinity in Pod.Annotations, instead of NodeSelector.
	It("validates that required NodeAffinity setting is respected if matching [Feature:NodeAffinity]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod without a label to get a node which can launch it.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: podName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		err = c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))
		expectNoError(err)

		By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(util.NewUUID()))
		v := "42"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to relaunch the pod, now with labels.")
		labelPodName := "with-labels"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: labelPodName,
				Annotations: map[string]string{
					"scheduler.alpha.kubernetes.io/affinity": `
						{"nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": {
							"nodeSelectorTerms": [
							{
								"matchExpressions": [{
									"key": "kubernetes.io/hostname",
									"operator": "In",
									"values": ["` + nodeName + `"]
								},{
									"key": "` + k + `",
									"operator": "In",
									"values": ["` + v + `"]
								}]
							}
						]
					}}}`,
				},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  labelPodName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

	// Verify that an escaped JSON string of NodeAffinity in a YAML PodSpec works.
	It("validates that embedding the JSON NodeAffinity setting as a string in the annotation value work [Feature:NodeAffinity]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod without a label to get a node which can launch it.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: podName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		err = c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))
		expectNoError(err)

		By("Trying to apply a label with fake az info on the found node.")
		k := "kubernetes.io/e2e-az-name"
		v := "e2e-az1"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to launch a pod that with NodeAffinity setting as embedded JSON string in the annotation value.")
		labelPodName := "with-labels"
		nodeSelectionRoot := filepath.Join(testContext.RepoRoot, "docs/user-guide/node-selection")
		testPodPath := filepath.Join(nodeSelectionRoot, "pod-with-node-affinity.yaml")
		runKubectlOrDie("create", "-f", testPodPath, fmt.Sprintf("--namespace=%v", ns))
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

	// test cases related to the Inter pod affinity & anti affinity topology feature

	It("validates that a pod with an invalid podAffinity is rejected because weight is given zero [Feature:InterPodAffinity]", func() {
		By("Trying to launch a pod with an invalid pod Affinity data.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "without-label"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
							{
								Weight: 0,
								PodAffinityTerm: api.PodAffinityTerm{
									LabelSelector: &api.LabelSelector{
										MatchExpressions: []api.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: api.LabelSelectorOpIn,
												Values:   []string{"securityscan"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
		})

		if err == nil || !errors.IsInvalid(err) {
			Failf("Expect error of invalid, got : %v", err)
		}

		// Wait a bit to allow scheduler to do its thing if the pod is not rejected.
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		cleanupPods(c, ns)
	})

	It("validates that a pod with an invalid podAffinity is rejected because of the LabelSelectorRequirement is invalid [Feature:InterPodAffinity]", func() {
		By("Trying to launch a pod with an invalid pod Affinity data.")
		podName := "without-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "without-label"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{
							{
								LabelSelector: &api.LabelSelector{
									MatchExpressions: []api.LabelSelectorRequirement{
										{
											Key:      "service",
											Operator: api.LabelSelectorOpDoesNotExist,
											Values:   []string{"securityscan"},
										},
									},
								},
								TopologyKey: "region",
							},
						},
					},
				},
			},
		})

		if err == nil || !errors.IsInvalid(err) {
			Failf("Expect error of invalid, got : %v", err)
		}

		// Wait a bit to allow scheduler to do its thing if the pod is not rejected.
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		cleanupPods(c, ns)
	})

	// Test Nodes does not have any pod, hence it should be impossible to schedule Pod with pod affinity
	It("validates that Inter-pod-Affinity is respected if not matching [Feature:InterPodAffinity]", func() {
		By("Trying to schedule Pod with nonempty Pod Affinity.")
		podName := "restricted-pod"

		waitForStableCluster(c)

		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": "restricted"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
							LabelSelector: &api.LabelSelector{
								MatchExpressions: []api.LabelSelectorRequirement{
									{
										Key:      "service",
										Operator: api.LabelSelectorOpIn,
										Values:   []string{"securityscan"},
									}},
							},
							TopologyKey: "region",
						}},
					},
				},
			},
		})
		expectNoError(err)
		// Wait a bit to allow scheduler to do its thing
		// TODO: this is brittle; there's no guarantee the scheduler will have run in 10 seconds.
		Logf("Sleeping 10 seconds and crossing our fingers that scheduler will run in that time.")
		time.Sleep(10 * time.Second)

		verifyResult(c, podName, ns)
		cleanupPods(c, ns)
	})

	// test the pod affinity successful matching scenario
	It("validates that InterPodAffinity is respected if matching [Conformance]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod with a label to get a node which can launch it.")
		podName := "with-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"security": "S1"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		defer c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))

		By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(util.NewUUID()))
		v := "China"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to launch the pod, now with podAffinity.")
		labelPodName := "with-podaffinity"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: labelPodName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  labelPodName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
							LabelSelector: &api.LabelSelector{
								MatchExpressions: []api.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: api.LabelSelectorOpIn,
										Values:   []string{"S1"},
									}},
							},
							TopologyKey: k,
						}},
					},
				},
			},
		})
		expectNoError(err)
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

	// test the pod affinity successful matching scenario with multiple Label Operators
	It("validates that InterPodAffinity is respected if matching with multiple Affinities [Conformance]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod with a label and pod affinity so get a node which can launch it.")
		podName := "with-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"security": "S1"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		defer c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))

		By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(util.NewUUID()))
		v := "China"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to launch the pod, now with multiple pod affinities with diff LabelOperators.")
		labelPodName := "with-podaffinity"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: labelPodName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  labelPodName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
							LabelSelector: &api.LabelSelector{
								MatchExpressions: []api.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: api.LabelSelectorOpIn,
										Values:   []string{"S1"},
									},
									{
										Key:      "security",
										Operator: api.LabelSelectorOpNotIn,
										Values:   []string{"S2"},
									},
									{
										Key:      "wrongkey",
										Operator: api.LabelSelectorOpDoesNotExist,
									},
									{
										Key:      "security",
										Operator: api.LabelSelectorOpExists,
									},
								},
							},
							TopologyKey: k,
						}},
					},
				},
			},
		})
		expectNoError(err)
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

	// test the pod anti affinity successful matching scenario
	It("validates that InterPod Affinity and AntiAffinity is respected if matching [Conformance]", func() {
		// launch a pod to find a node which can launch a pod. We intentionally do
		// not just take the node list and choose the first of them. Depending on the
		// cluster and the scheduler it might be that a "normal" pod cannot be
		// scheduled onto it.
		By("Trying to launch a pod with a label and pod anti affinity to get a node which can launch it.")
		podName := "with-label"
		_, err := c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"security": "S1"},
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  podName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
			},
		})
		expectNoError(err)
		expectNoError(waitForPodRunningInNamespace(c, podName, ns))
		pod, err := c.Pods(ns).Get(podName)
		expectNoError(err)

		nodeName := pod.Spec.NodeName
		defer c.Pods(ns).Delete(podName, api.NewDeleteOptions(0))

		By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(util.NewUUID()))
		v := "China"
		patch := fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, k, v)
		err = c.Patch(api.MergePatchType).Resource("nodes").Name(nodeName).Body([]byte(patch)).Do().Error()
		expectNoError(err)

		node, err := c.Nodes().Get(nodeName)
		expectNoError(err)
		Expect(node.Labels[k]).To(Equal(v))

		By("Trying to launch the pod, now with Pod Anti Affinity.")
		labelPodName := "with-podantiaffinity"
		_, err = c.Pods(ns).Create(&api.Pod{
			TypeMeta: unversioned.TypeMeta{
				Kind: "Pod",
			},
			ObjectMeta: api.ObjectMeta{
				Name: labelPodName,
			},
			Spec: api.PodSpec{
				Containers: []api.Container{
					{
						Name:  labelPodName,
						Image: "gcr.io/google_containers/pause:2.0",
					},
				},
				Affinity: &api.Affinity{
					PodAffinity: &api.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
							LabelSelector: &api.LabelSelector{
								MatchExpressions: []api.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: api.LabelSelectorOpIn,
										Values:   []string{"S1"},
									}},
							},
							TopologyKey: k,
						}},
					},
					PodAntiAffinity: &api.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{{
							LabelSelector: &api.LabelSelector{
								MatchExpressions: []api.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: api.LabelSelectorOpNotIn,
										Values:   []string{"CKY456"},
									}},
							},
							TopologyKey: "wrong-value",
						}},
					},
				},
			},
		})
		expectNoError(err)
		defer c.Pods(ns).Delete(labelPodName, api.NewDeleteOptions(0))

		// check that pod got scheduled. We intentionally DO NOT check that the
		// pod is running because this will create a race condition with the
		// kubelet and the scheduler: the scheduler might have scheduled a pod
		// already when the kubelet does not know about its new label yet. The
		// kubelet will then refuse to launch the pod.
		expectNoError(waitForPodNotPending(c, ns, labelPodName))
		labelPod, err := c.Pods(ns).Get(labelPodName)
		expectNoError(err)
		Expect(labelPod.Spec.NodeName).To(Equal(nodeName))
	})

})
