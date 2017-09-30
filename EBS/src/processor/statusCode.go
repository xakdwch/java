package processor

import (
	"net/http"
	//"fmt"
	//"strconv"
	"fmt"
)

const (
	statusCreateVolumeErr     = 701
	statusInfoVolumesErr      = 702
	statusDelVolumeErr        = 703
	statusNotFoundErr         = 704
	statusCreatePoolErr       = 705
	statusInfoPoolErr         = 706
	statusInfoPoolIOErr       = 707
	statusDelPoolErr          = 708
	statusModPoolErr          = 709
	statusResizeVolumeErr     = 710
	statusCreateSnapshotErr   = 711
	statusInfoSnapshotErr     = 712
	statusDelSnapshotErr      = 713
	statusExportVolumeErr     = 714
)

var codeDesc = map[int]string {
	statusCreateVolumeErr     : "Create Volume Failed",
	statusInfoVolumesErr      : "Info Volumes Failed",
	statusDelVolumeErr        : "Delete Volume Failed",
	statusResizeVolumeErr     : "Resize Volume Failed",
	statusExportVolumeErr     : "Export Volume Failed",
	statusNotFoundErr         : "Not Found",
	statusCreatePoolErr       : "Create Pool Failed",
	statusInfoPoolErr         : "Info Pool Failed",
	statusInfoPoolIOErr       : "Info Pool IO Failed",
	statusDelPoolErr          : "Delete Pool Failed",
	statusModPoolErr          : "Modify Pool Failed",
	statusCreateSnapshotErr   : "Create Snapshot Failed",
	statusInfoSnapshotErr     : "Info Snapshot Failed",
	statusDelSnapshotErr      : "Delete Snapshot Failed",
}

func SendStatus(w http.ResponseWriter, code int, desc string) {
	if desc == "" {
		http.Error(w, codeDesc[code], code)
	} else {
		http.Error(w, desc, code)
	}
}

func SendResponse(w http.ResponseWriter, code int, payload string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintln(w, payload)
}
