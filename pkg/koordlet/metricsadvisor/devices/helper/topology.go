/*
Copyright 2022 The Koordinator Authors.

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

package helper

import (
	"bytes"
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/koordinator-sh/koordinator/pkg/koordlet/util/system"
)

var (
	SysBusPci = "/sys/bus/pci/devices"
	DevDir    = "/dev"

	pcieRegexp = regexp.MustCompile(`pci\d{4}:[0-9a-fA-F]{2}`)
)

func ParsePCIInfo(busID string) (int32, string, string, error) {
	//klog.V(4).Infof("ParsePCIInfo: busID=%s", busID)
	nodeID, err := getNUMANodeID(busID)
	if err != nil {
		//klog.V(4).Infof("ParsePCIInfo: return err:%v busid:", err, busID)
		return 0, "", "", fmt.Errorf("failed to parse NUMA Node ID, err: %w", err)
	}
	//klog.V(4).Infof("ParsePCIInfo: go on")
	pcie, err := getPCIERootComplexID(busID)
	if err != nil {
		//klog.V(4).Infof("failed to parse PCIE ID, err: %v", err)
		return 0, "", "", fmt.Errorf("failed to parse PCIE ID, err: %w", err)
	}
	//klog.V(4).Infof("ParsePCIInfo: nodeID=%s pcie=%s busID=%s", nodeID, pcie, busID)
	return nodeID, pcie, busID, nil
}

func getPCIERootComplexID(bdf string) (string, error) {
	path, err := filepath.EvalSymlinks(filepath.Join(system.GetPCIDeviceDir(), bdf))
	if err != nil {
		return "", err
	}
	return parsePCIEID(path), err
}

func parsePCIEID(path string) string {
	result := pcieRegexp.FindAllStringSubmatch(path, -1)
	if len(result) == 0 || len(result[0]) == 0 {
		return ""
	}
	return result[0][0]
}

func getNUMANodeID(bdf string) (int32, error) {
	path := filepath.Join(system.GetPCIDeviceDir(), bdf, "numa_node")
	klog.V(4).Infof("ParsePCIInfo: path=%s", path)
	data, err := os.ReadFile(filepath.Join(system.GetPCIDeviceDir(), bdf, "numa_node"))
	if err != nil {
		return -1, err
	}
	nodeID, err := strconv.Atoi(string(bytes.TrimSpace(data)))
	if err != nil {
		return 0, err
	}
	if nodeID == -1 {
		nodeID = 0
	}
	return int32(nodeID), nil
}
