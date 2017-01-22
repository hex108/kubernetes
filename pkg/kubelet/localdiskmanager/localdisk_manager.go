/*
Copyright 2017 The Kubernetes Authors.

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

package localdiskmanager

import (
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api/v1"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/util/wait"
)

const (
	monitorLoopPeriod    time.Duration = 1 * time.Minute
	mb                                 = 1024 * 1024
	freeDiskThreadHold                 = 1024 // mb
	defaultKillPodPeriod               = 30 * time.Second
)

type LocalDiskManager interface {
	Run(localDisks []v1.LocalDisk, updates chan<- kubetypes.PodUpdate, statusManager status.Manager, stopCh <-chan struct{})
}

func NewLocalDiskManager(podManager kubepod.Manager) LocalDiskManager {
	return &localDiskManager{
		podManager: podManager,
	}
}

type localDiskManager struct {
	localDisks    []v1.LocalDisk
	podManager    kubepod.Manager
	podUpdates    chan<- kubetypes.PodUpdate
	lastKillTime  map[string]time.Time
	statusManager status.Manager
}

func (ldm *localDiskManager) Run(localDisks []v1.LocalDisk, updates chan<- kubetypes.PodUpdate, statusManager status.Manager, stopCh <-chan struct{}) {
	glog.V(2).Infof("Starting Kubelet LocalDisk Manager")

	ldm.localDisks = localDisks
	ldm.podUpdates = updates
	ldm.lastKillTime = make(map[string]time.Time)
	ldm.statusManager = statusManager

	go wait.Until(ldm.Monitor, monitorLoopPeriod, stopCh)

	<-stopCh
	glog.V(2).Infof("Shutting down Kubelet LocalDisk Manager")
}

func (ldm *localDiskManager) Monitor() {
	for _, localDisk := range ldm.localDisks {
		lastKillTime, ok := ldm.lastKillTime[localDisk.LocalDir]
		if ok && time.Now().Sub(lastKillTime) < defaultKillPodPeriod {
			glog.V(4).Infof("Wait for cleanup of previous pod on %s", localDisk.LocalDir)
			continue
		}
		if ldm.NeedCleanupDisk(localDisk) {
			ldm.CleanupDisk(localDisk)
		}
	}
}

func (ldm *localDiskManager) NeedCleanupDisk(localDisk v1.LocalDisk) bool {
	glog.Infof("Check disk %+v", localDisk)
	dir := localDisk.LocalDir
	stat := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &stat); err != nil {
		glog.Errorf("Error getting %s's status: %v", dir, err)
		return false
	}
	availableDiskSpace := stat.Bavail * uint64(stat.Bsize) / mb
	if availableDiskSpace <= freeDiskThreadHold {
		glog.Warningf("Available disk space at %s is only %d M, less than threadhold %d M", dir, availableDiskSpace, freeDiskThreadHold)
		return true
	}

	return false
}

func (ldm *localDiskManager) CleanupDisk(localDisk v1.LocalDisk) {
	glog.Infof("Clean up disk %+v...", localDisk)
	pods := ldm.podManager.GetPods()
	pod := ldm.GetPodToEvict(pods, localDisk.LocalDir)
	if pod == nil {
		glog.Errorf("Failed to find pod running on local disk %s", localDisk.LocalDir)
		return
	}
	glog.Infof("Delete pod %s(%s)", pod.Name, pod.UID)
	var podsToDelete []*v1.Pod
	podsToDelete = append(podsToDelete, pod)
	ldm.podUpdates <- kubetypes.PodUpdate{Pods: podsToDelete, Op: kubetypes.REMOVE, Source: kubetypes.ApiserverSource}
	ldm.statusManager.SetPodStatus(pod, v1.PodStatus{
		Phase:   v1.PodFailed,
		Reason:  "OutOfDisk",
		Message: "Pod used more disk than its request"})
	ldm.lastKillTime[localDisk.LocalDir] = time.Now()
}

func (ldm *localDiskManager) GetPodToEvict(pods []*v1.Pod, localDir string) *v1.Pod {
	var podToEvict *v1.Pod
	var score = 0
	for _, pod := range pods {
		if !isPodOnLocalDisk(pod, localDir) {
			glog.V(8).Infof("Ignore pod(%s, %s), it is not on local disk %s", pod.Name, pod.UID, localDir)
			continue
		}
		s := getScore(pod, localDir)
		glog.V(8).Infof("Score for %s is %d, current max score is %d", pod.Name, s, score)
		if s > score {
			podToEvict = pod
			score = s
		}
	}
	return podToEvict
}

func isPodOnLocalDisk(pod *v1.Pod, localDir string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.LocalDisk != nil && volume.LocalDisk.LocalPath == localDir {
			glog.V(8).Infof("Find pod(%s, %s) on local disk %s", pod.Name, pod.UID, localDir)
			return true
		}
	}
	return false
}

func getScore(pod *v1.Pod, localDir string) int {
	var requestedDiskCapacity uint32
	for _, volume := range pod.Spec.Volumes {
		if volume.LocalDisk != nil && volume.LocalDisk.LocalPath == localDir {
			glog.V(8).Infof("Find pod(%s, %s) on local disk %s, add % to requestedDiskCapacity",
				pod.Name, pod.UID, localDir, volume.LocalDisk.DiskSize)
			requestedDiskCapacity += volume.LocalDisk.DiskSize
		}
	}
	usedDiskCapacity, err := du(path.Join(localDir, string(pod.UID)))
	if err != nil {
		glog.Errorf("Failed to get score for %s(%s)", pod.Name, pod.UID)
		return 0
	}
	glog.V(8).Infof("Pod %s' usedDiskCapacity is %d G, requestedDiskCapacity is %d G", usedDiskCapacity, requestedDiskCapacity)
	return int(usedDiskCapacity) - int(requestedDiskCapacity)
}

func du(path string) (uint32, error) {
	out, err := exec.Command("nice", "-n", "19", "du", "-s", "-B", "1G", path).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed command 'du' ($ nice -n 19 du -s -B 1) on path %s with error %v", path, err)
	}
	diskCapacity, err := strconv.ParseUint(strings.Fields(string(out))[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s in du", string(out))
	}
	return uint32(diskCapacity), nil
}
