package processor

import (
	"net/http"
	"librados/rados"
	"fmt"
	"os"
	"librados/rbd"
	"strconv"
	"encoding/json"
	"math"
	"sync"
)

const MaxProcessorNumber = 16

/*
GET /?Action=CreateVolume&PoolName={PoolName}&VolumeName={VolumeName}&Size={VolumeSize} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func CreateVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	size := r.FormValue("Size")
	if pool == "" || volume == "" || size == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	volumeSize, err := strconv.ParseUint(size, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusCreateVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusCreateVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusCreateVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed: %v\n", err)
		SendStatus(w, statusCreateVolumeErr, "")
		return
	}
	defer ioctx.Destroy()

	if _, err := rbd.Create(ioctx, volume, volumeSize, 22); err != nil {
		fmt.Fprintf(os.Stderr, "RBD create volume failed: %v\n", err)
		SendStatus(w, statusCreateVolumeErr, err.Error())
		return
	}


	SendStatus(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=InfoVolume&PoolName={PoolName|*} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date
Content-Type: application/json
Content-Length: n

n bytes json result
*/
func InfoVolumes(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	var pools []string
	if pool == "*" {
		if pools, err = conn.ListPools(); err != nil {
			fmt.Fprintf(os.Stderr, "ListPools failed: %v\n", err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}
	} else {
		if err := conn.LookupPool(pool); err != nil {
			fmt.Fprintf(os.Stderr, "LookupPool failed, pool name:%v, %v\n", pool, err)
			SendStatus(w, statusNotFoundErr, "")
			return
		}
		pools = append(pools, pool)
	}

	imageInfo := make(map[string][]map[string]rbd.ImageInfo)
	for i := 0; i < len(pools); i++ {
		ioctx, err := conn.OpenIOContext(pools[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}
		defer ioctx.Destroy()

		images, err := rbd.GetImageNames(ioctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetImageNames failed: %v\n", err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}

		var imageList []map[string]rbd.ImageInfo
		for j := 0; j < len(images); j++ {
			image := rbd.GetImage(ioctx, images[j])
			if err := image.Open(); err != nil {
				fmt.Fprintf(os.Stderr, "Open failed, image name:%v/%v, %v\n", pool, images[j], err)
				SendStatus(w, statusInfoVolumesErr, "")
				return
			}
			defer image.Close()

			info, err := image.Stat()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Stat failed, image name:%v/%v, %v\n", pool, images[j], err)
				SendStatus(w, statusInfoVolumesErr, "")
				return
			}
			infoPair := make(map[string]rbd.ImageInfo);infoPair[images[j]] = *info
			imageList = append(imageList, infoPair)
		}
		imageInfo[pools[i]] = imageList
	}

	payload, err := json.Marshal(imageInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode payload failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	fmt.Fprintf(os.Stderr, "payload: %v\n", string(payload))
	SendResponse(w, http.StatusOK, string(payload))
}

/*
GET /?Action=DelVolume&PoolName={PoolName}&VolumeName={volumeName} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func DelVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	if pool == "" || volume == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Remove(); err != nil {
		fmt.Fprintf(os.Stderr, "Remove failed %v\n", err)
		SendStatus(w, statusDelVolumeErr, err.Error())
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=ResizeVolume&PoolName={PoolName}&VolumeName={volumeName}&Size={newSize} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func ResizeVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	size := r.FormValue("Size")
	if pool == "" || volume == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	newSize, err := strconv.ParseUint(size, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusResizeVolumeErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, err.Error())
		return
	}
	defer image.Close()

	if err := image.Resize(newSize); err != nil {
		fmt.Fprintf(os.Stderr, "Resize failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=ExportVolume&PoolName={PoolName}&VolumeName={volumeName}&OSSBucket={bucket} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func ExportVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	bucket := r.FormValue("OSSBucket")
	if pool == "" || volume == "" || bucket == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	//TODO bucket合法性检查

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}
	defer image.Close()

	info, err := image.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "image Stat failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	//TODO 暂时导出到文件系统中
	fp, err := os.OpenFile(bucket, os.O_RDONLY | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenFile bucket %v failed: %v\n", bucket, err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	period, err := image.GetStripePeriod()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetStripePeriod failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	fmt.Fprintf(os.Stderr, "GetStripePeriod: %v\n", period)

	ch := make(chan int, MaxProcessorNumber)
	wg := &sync.WaitGroup{}
	for offset := uint64(0); offset < info.Size; offset += period {
		length := uint64(math.Min(float64(period), float64(info.Size - offset)))
		ch <- 1
		wg.Add(1)
		go doRead(fp, offset, length)
	}
	wg.Wait()

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}
