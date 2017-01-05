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

package local_disk

import (
	"fmt"
	"os"
	"path"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"path/filepath"
)

// TODO: in the near future, this will be changed to be more restrictive
// and the group will be set to allow containers to use emptyDir volumes
// from the group attribute.
//
// http://issue.k8s.io/2630
const perm os.FileMode = 0777

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&localDiskPlugin{nil},
	}
}

type localDiskPlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &localDiskPlugin{}

const (
	localDiskPluginName = "kubernetes.io/local-disk"
)

func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, strings.EscapeQualifiedNameForDisk(localDiskPluginName), volName)
}

func (plugin *localDiskPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host

	return nil
}

func (plugin *localDiskPlugin) GetPluginName() string {
	return localDiskPluginName
}

func (plugin *localDiskPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("Spec does not reference an LocalDisk volume type")
	}

	// Return user defined volume name, since this is an ephemeral volume type
	return spec.Name(), nil
}

func (plugin *localDiskPlugin) CanSupport(spec *volume.Spec) bool {
	if spec.Volume != nil && spec.Volume.LocalDisk != nil {
		return true
	}
	return false
}

func (plugin *localDiskPlugin) RequiresRemount() bool {
	return false
}

func (plugin *localDiskPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, opts volume.VolumeOptions) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod, plugin.host.GetMounter(), opts)
}

func (plugin *localDiskPlugin) newMounterInternal(spec *volume.Spec, pod *v1.Pod, mounter mount.Interface, opts volume.VolumeOptions) (volume.Mounter, error) {
	return &localDisk{
		pod:             pod,
		volName:         spec.Name(),
		localPath:       spec.Volume.LocalDisk.LocalPath,
		mounter:         mounter,
		plugin:          plugin,
		MetricsProvider: volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host)),
	}, nil
}

func (plugin *localDiskPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	// Inject real implementations here, test through the internal function.
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

func (plugin *localDiskPlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface) (volume.Unmounter, error) {
	ed := &localDisk{
		pod:             &v1.Pod{ObjectMeta: v1.ObjectMeta{UID: podUID}},
		volName:         volName,
		mounter:         mounter,
		plugin:          plugin,
		MetricsProvider: volume.NewMetricsDu(getPath(podUID, volName, plugin.host)),
	}
	return ed, nil
}

func (plugin *localDiskPlugin) ConstructVolumeSpec(volName, mountPath string) (*volume.Spec, error) {
	// TODO: Check whether it works, especially the 'volName'
	localDiskVolume := &v1.Volume{
		Name: volName,
		VolumeSource: v1.VolumeSource{
			LocalDisk: &v1.LocalDiskSource{LocalPath: volName},
		},
	}
	glog.V(3).Infof("ConstructVolumeSpec from volumeName: %s", volName)
	return volume.NewSpecFromVolume(localDiskVolume), nil
}

// LocalDisk volumes are temporary directories exposed to the pod.
// These do not persist beyond the lifetime of a pod.
type localDisk struct {
	pod           *v1.Pod
	volName       string
	localPath     string
	mounter       mount.Interface
	plugin        *localDiskPlugin
	volume.MetricsProvider
}

func (ld *localDisk) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        false,
		Managed:         true,
		SupportsSELinux: true,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *localDisk) CanMount() error {
	return nil
}

// SetUp creates new directory.
func (ld *localDisk) SetUp(fsGroup *int64) error {
	return ld.SetUpAt(ld.GetPath(), fsGroup)
}

// SetUpAt creates new directory.
func (ld *localDisk) SetUpAt(dir string, fsGroup *int64) error {
	notMnt, err := ld.mounter.IsLikelyNotMountPoint(dir)
	// Getting an os.IsNotExist err from is a contingency; the directory
	// may not exist yet, in which case, setup should run.
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// If the plugin readiness file is present for this volume, and the
	// storage medium is the default, then the volume is ready.  If the
	// medium is memory, and a mountpoint is present, then the volume is
	// ready.
	if volumeutil.IsReady(ld.getMetaDir()) && !notMnt {
		return nil
	}
	
	err = ld.setupDir(dir)

	volume.SetVolumeOwnership(ld, fsGroup)

	if err == nil {
		volumeutil.SetReady(ld.getMetaDir())
	}

	return err
}


// setupDir creates the directory with the specified SELinux context and
// the default permissions specified by the perm constant.
func (ld *localDisk) setupDir(dir string) error {
	// Create localPath, then create a link to it
	realPath := filepath.Join(ld.localPath, string(ld.pod.UID), ld.volName)
	glog.V(3).Infof("SetupDir for localDisk volume(%s -> %s)", dir, realPath)
	// Create the directory if it doesn't already exist.
	if err := os.MkdirAll(realPath, perm); err != nil {
		return err
	}
	// stat the directory to read permission bits
	fileinfo, err := os.Lstat(realPath)
	if err != nil {
		return err
	}

	if fileinfo.Mode().Perm() != perm.Perm() {
		// If the permissions on the created directory are wrong, the
		// kubelet is probably running with a umask set.  In order to
		// avoid clearing the umask for the entire process or locking
		// the thread, clearing the umask, creating the dir, restoring
		// the umask, and unlocking the thread, we do a chmod to set
		// the specific bits we need.
		err := os.Chmod(realPath, perm)
		if err != nil {
			return err
		}

		fileinfo, err = os.Lstat(realPath)
		if err != nil {
			return err
		}

		if fileinfo.Mode().Perm() != perm.Perm() {
			glog.Errorf("Expected directory %q permissions to be: %s; got: %s", realPath, perm.Perm(), fileinfo.Mode().Perm())
		}
	}

	// Link dir to ld.localPath
	if err := os.MkdirAll(filepath.Dir(dir), perm); err != nil {
		return err
	}
	if err := os.Symlink(realPath, dir); err != nil {
		return err
	}

	return nil
}

func (ed *localDisk) GetPath() string {
	return getPath(ed.pod.UID, ed.volName, ed.plugin.host)
}

// TearDown simply discards everything in the directory.
func (ed *localDisk) TearDown() error {
	return ed.TearDownAt(ed.GetPath())
}

// TearDownAt simply discards everything in the directory.
func (ld *localDisk) TearDownAt(dir string) error {
	// assume StorageMediumDefault
	return ld.teardownDefault(dir)
}

func (ld *localDisk) teardownDefault(dir string) error {
	// Read link, then delete the link and actual data directory
	realPath, err := os.Readlink(dir)
	if err != nil {
		return nil
	}
	glog.V(3).Infof("TearDown for localDisk volume: %s and %s", dir, realPath)

	err = os.RemoveAll(dir)
	if err != nil {
		return err
	}

	err = os.RemoveAll(realPath)
	if err != nil {
		return err
	}

	// TODO: Rename them first before removing
	//tmpDir, err := volume.RenameDirectory(dir, ld.volName+".deleting~")
	//if err != nil {
	//	return err
	//}
	//err = os.RemoveAll(tmpDir)

	return nil
}

func (ld *localDisk) getMetaDir() string {
	return path.Join(ld.plugin.host.GetPodPluginDir(ld.pod.UID, strings.EscapeQualifiedNameForDisk(localDiskPluginName)), ld.volName)
}

func getVolumeSource(spec *volume.Spec) (*v1.LocalDiskSource, bool) {
	var readOnly bool
	var volumeSource *v1.LocalDiskSource

	if spec.Volume != nil && spec.Volume.LocalDisk != nil {
		volumeSource = spec.Volume.LocalDisk
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}
