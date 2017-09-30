package route

import (
	"net/http"
	"fmt"
	"os"
	"processor"
)

func Init() {
	http.HandleFunc("/", httpRoute)
}

func httpRoute(w http.ResponseWriter, r *http.Request) {
	if !isValidRequest(r) {
		fmt.Fprintf(os.Stderr, "Method not allowed: %v, expected: %v\n", r.Method, http.MethodGet)
		processor.SendStatus(w, http.StatusMethodNotAllowed, http.StatusText(http.StatusMethodNotAllowed))
		return
	}

	r.ParseForm()
	action := r.FormValue("Action")

	switch {
	case isCreateVolume(action):
		processor.CreateVolume(w, r)
	case isInfoVolumes(action):
		processor.InfoVolumes(w, r)
	case isDelVolume(action):
		processor.DelVolume(w, r)
	case isResizeVolume(action):
		processor.ResizeVolume(w, r)
	case isExportVolume(action):
		processor.ExportVolume(w, r)
	case isCreatePool(action):
		processor.CreatePool(w, r)
	case isInfoPool(action):
		processor.InfoPools(w, r)
	case isInfoPoolIO(action):
		processor.InfoPoolsIO(w, r)
	case isDelPool(action):
		processor.DelPool(w, r)
	case isModPoolReplicaSize(action):
		processor.ModPoolRepSize(w,r )
	case isCreateSnapshot(action):
		processor.CreateSnapshot(w, r)
	case isInfoSnapshot(action):
		processor.InfoSnapshot(w, r)
	case isDelSnapshot(action):
		processor.DelSnapshot(w, r)
	case isTest(action):
		processor.Test(w, r)
	default:
		fmt.Fprintf(os.Stderr, "Unknown request: %v\n", r.RequestURI)
	}
}

func isTest(action string) bool {
	return action == testAction
}

func isValidRequest(r *http.Request) bool {
	return r.Method == http.MethodGet
}

func isCreateVolume(action string) bool {
	return action == createVolumeAction
}

func isInfoVolumes(action string) bool {
	return action == infoVolumesAction
}

func isDelVolume(action string) bool {
	return action == delVolumeAction
}

func isResizeVolume(action string) bool {
	return action == resizeVolumeAction
}

func isExportVolume(action string) bool {
	return action == exportVolumeAction
}

func isCreatePool(action string) bool {
	return action == createPoolAction
}

func isInfoPool(action string) bool {
	return action == infoPoolAction
}

func isInfoPoolIO(action string) bool {
	return action == infoPoolIOAction
}

func isDelPool(action string) bool {
	return action == delPoolAction
}

func isModPoolReplicaSize(action string) bool {
	return action == modPoolRepSizeAction
}

func isCreateSnapshot(action string) bool {
	return action == createSnapshotAction
}

func isInfoSnapshot(action string) bool {
	return action == infoSnapshotAction
}

func isDelSnapshot(action string) bool {
	return action == delSnapshotAction
}