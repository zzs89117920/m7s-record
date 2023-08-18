package record

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go"
	m7sdb "github.com/zzs89117920/m7s-db"
	. "m7s.live/engine/v4"
	"m7s.live/engine/v4/util"
)
type MediaRecord struct {
	Id int `gorm:"primaryKey"`
	StreamPath string
	RecordId string
	FileName string
	Status int
	Type int
	CreateTime time.Time
}
type ChannelInfo struct {
	DeviceID     string `gorm:"primaryKey"`// 通道ID
	ParentID     string `gorm:"primaryKey"`
	IsRecord     bool
}

type PullDevice struct {
	Id int `gorm:"primaryKey"`
	IsRecord bool
	StreamPath string
}

func (conf *RecordConfig) API_list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	t := query.Get("type")
	var files []*VideoFileInfo
	var err error
	recorder := conf.getRecorderConfigByType(t)
	if recorder == nil {
		for _, t = range []string{"flv", "mp4", "hls", "raw", "raw_audio"} {
			recorder = conf.getRecorderConfigByType(t)
			var fs []*VideoFileInfo
			if fs, err = recorder.Tree(recorder.Path, 0); err == nil {
				files = append(files, fs...)
			}
		}
	} else {
		files, err = recorder.Tree(recorder.Path, 0)
	}

	if err == nil {
		var bytes []byte
		if bytes, err = json.Marshal(files); err == nil {
			w.Write(bytes)
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (conf *RecordConfig) API_start(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	streamPath := query.Get("streamPath")
	if streamPath == "" {
		http.Error(w, "no streamPath", http.StatusBadRequest)
		return
	}
	t := query.Get("type")
	var id string
	var err error
	switch t {
	case "":
		t = "flv"
		fallthrough
	case "flv":
		var flvRecoder FLVRecorder
		flvRecoder.append = query.Get("append") != ""
		err = flvRecoder.Start(streamPath)
		id = flvRecoder.ID
	case "mp4":
		recorder := NewMP4Recorder()
		err = recorder.Start(streamPath)
		id = recorder.ID
	case "hls":
		var recorder HLSRecorder
		err = recorder.Start(streamPath)
		id = recorder.ID
	case "raw":
		var recorder RawRecorder
		recorder.append = query.Get("append") != ""
		err = recorder.Start(streamPath)
		id = recorder.ID
	case "raw_audio":
		var recorder RawRecorder
		recorder.IsAudio = true
		recorder.append = query.Get("append") != ""
		err = recorder.Start(streamPath)
		id = recorder.ID
	default:
		http.Error(w, "type not supported", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(id))
}

func (conf *RecordConfig) API_list_recording(w http.ResponseWriter, r *http.Request) {
	util.ReturnJson(func() (recordings []any) {
		conf.recordings.Range(func(key, value any) bool {
			recordings = append(recordings, value)
			return true
		})
		return
	}, time.Second, w, r)
}

func (conf *RecordConfig) API_stop(w http.ResponseWriter, r *http.Request) {
	if recorder, ok := conf.recordings.Load(r.URL.Query().Get("id")); ok {
		recorder.(ISubscriber).Stop()
		store := RecordPluginConfig.Store
		if(store.Type == "Minio"){
			minioClient, err := minio.New(store.Endpoint, store.AccessKey, store.SecretKey, false)
			if err == nil {
				r := recorder.(*Recorder)
				dir, _ := os.Getwd()

				bktExist, _ := minioClient.BucketExists(store.Bucket)
				if(!bktExist){
					minioClient.MakeBucket(store.Bucket, "")
				}
				filePath := filepath.Join(dir, RecordPluginConfig.Mp4.Path, r.fileName)
				_ , err1 := minioClient.FPutObject(store.Bucket, r.fileName, filePath, minio.PutObjectOptions{ContentType: "video/mp4"})
				if(err1 == nil){
					os.Remove(filePath)
					var str_arr = strings.Split(r.fileName, ".")
					fileFullName := str_arr[0]
					
					var str_arr1 = strings.Split(fileFullName, "/")
					fileName := str_arr1[len(str_arr1) - 1]
					db := 	m7sdb.MysqlDB()
					fmt.Println("fileName=>"+fileName)
					db.Model(&MediaRecord{}).Where("file_name = ? and stream_path= ?", fileName ,  r.Stream.Path).Update("status", 3)
				}
    	}
		}
		w.Write([]byte("ok"))
		return
	}
	http.Error(w, "no such recorder", http.StatusBadRequest)
}
