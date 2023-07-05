package record

import (
	_ "embed"
	"errors"
	"io"
	"sync"

	m7sdb "github.com/zzs89117920/m7s-db"
	. "m7s.live/engine/v4"
	"m7s.live/engine/v4/codec"
	"m7s.live/engine/v4/config"
	"m7s.live/engine/v4/util"
)

type RecordConfig struct {
	DefaultYaml
	config.Subscribe
	Flv        Record
	Mp4        Record
	Hls        Record
	Raw        Record
	RawAudio   Record
	recordings sync.Map
}

//go:embed default.yaml
var defaultYaml DefaultYaml
var ErrRecordExist = errors.New("recorder exist")
var RecordPluginConfig = &RecordConfig{
	DefaultYaml: defaultYaml,
	Flv: Record{
		Path:          "record/flv",
		Ext:           ".flv",
		GetDurationFn: getFLVDuration,
	},
	Mp4: Record{
		Path: "record/mp4",
		Ext:  ".mp4",
	},
	Hls: Record{
		Path: "record/hls",
		Ext:  ".m3u8",
	},
	Raw: Record{
		Path: "record/raw",
		Ext:  ".", // 默认h264扩展名为.h264,h265扩展名为.h265
	},
	RawAudio: Record{
		Path: "record/raw",
		Ext:  ".", // 默认aac扩展名为.aac,pcma扩展名为.pcma,pcmu扩展名为.pcmu
	},
}

var plugin = InstallPlugin(RecordPluginConfig)

func (conf *RecordConfig) OnEvent(event any) {
	switch v := event.(type) {
	case FirstConfig, config.Config:
		conf.Flv.Init()
		conf.Mp4.Init()
		conf.Hls.Init()
		conf.Raw.Init()
		conf.RawAudio.Init()
	case SEclose:
		streamPath := v.Target.Path
		db := 	m7sdb.MysqlDB()
		db.Model(&MediaRecord{}).Where("stream_path = ?", streamPath).Where("type = ?", 1).Update("type", 2)
		delete(conf.Flv.recording, streamPath)
		delete(conf.Mp4.recording, streamPath)
		delete(conf.Hls.recording, streamPath)
		delete(conf.Raw.recording, streamPath)
		delete(conf.RawAudio.recording, streamPath)
	case SEpublish:
		streamPath := v.Target.Path
		if conf.Flv.NeedRecord(streamPath) {
			var flv FLVRecorder
			conf.Flv.recording[streamPath] = &flv
			go flv.Start(streamPath)
		}else{
			var mediaRecords []*MediaRecord
			db := 	m7sdb.MysqlDB()
			result := db.Where("stream_path = ?", streamPath).Where("type = ?", 2).Find(&mediaRecords)
			if(result.RowsAffected>0){
				for _, item := range mediaRecords {
					var flvRecoder FLVRecorder
					flvRecoder.append = true
					if(flvRecoder.filePath==""){
						flvRecoder.filePath =  item.FilePath
					}
					err := flvRecoder.Start(item.StreamPath)
					if(err == nil){
						db.Model(&MediaRecord{}).Where("record_id = ?", flvRecoder.ID).Update("type", 1)
					}
				}
				
			}
		}
		if conf.Mp4.NeedRecord(streamPath) {
			recoder := NewMP4Recorder()
			conf.Mp4.recording[streamPath] = recoder
			go recoder.Start(streamPath)
		}
		if conf.Hls.NeedRecord(streamPath) {
			var hls HLSRecorder
			conf.Hls.recording[streamPath] = &hls
			go hls.Start(streamPath)
		}
		if conf.Raw.NeedRecord(streamPath) {
			var raw RawRecorder
			conf.Raw.recording[streamPath] = &raw
			go raw.Start(streamPath)
		}
		if conf.RawAudio.NeedRecord(streamPath) {
			var raw RawRecorder
			raw.IsAudio = true
			conf.RawAudio.recording[streamPath] = &raw
			go raw.Start(streamPath)
		}
	}
}
func (conf *RecordConfig) getRecorderConfigByType(t string) (recorder *Record) {
	switch t {
	case "flv":
		recorder = &conf.Flv
	case "mp4":
		recorder = &conf.Mp4
	case "hls":
		recorder = &conf.Hls
	case "raw":
		recorder = &conf.Raw
	case "raw_audio":
		recorder = &conf.RawAudio
	}
	return
}

func getFLVDuration(file io.ReadSeeker) uint32 {
	_, err := file.Seek(-4, io.SeekEnd)
	if err == nil {
		var tagSize uint32
		if tagSize, err = util.ReadByteToUint32(file, true); err == nil {
			_, err = file.Seek(-int64(tagSize)-4, io.SeekEnd)
			if err == nil {
				_, timestamp, _, err := codec.ReadFLVTag(file)
				if err == nil {
					return timestamp
				}
			}
		}
	}
	return 0
}
