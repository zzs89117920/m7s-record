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
	config.HTTP
	config.Subscribe
	Flv        Record
	Mp4        Record
	Hls        Record
	Raw        Record
	RawAudio   Record
	recordings sync.Map
	Store StoreConfig
}
type StoreConfig struct {
	Type string
	Endpoint string
	AccessKey string
	SecretKey string
	Bucket string
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
	Store: StoreConfig{
		Type: "File",
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
		}
		if conf.Mp4.NeedRecord(streamPath) {
			recoder := NewMP4Recorder()
			conf.Mp4.recording[streamPath] = recoder
			go recoder.Start(streamPath)
		}else{
			db := m7sdb.MysqlDB()
			var channelInfos []*ChannelInfo
			result := db.Where("is_record = ?", true).Find(&channelInfos)
			if(result.RowsAffected>0){
				for _, item := range channelInfos {
					recoder := NewMP4Recorder()
					conf.Mp4.recording[streamPath] = recoder
					recoder.Start(item.ParentID+item.DeviceID)
				}
			}
			var pullDevices []*PullDevice
			result1 := db.Where("is_record = ?", true).Find(&pullDevices)
			if(result1.RowsAffected>0){
				for _, item := range pullDevices {
					recoder := NewMP4Recorder()
					conf.Mp4.recording[streamPath] = recoder
					recoder.Start(item.StreamPath)
				}
			}
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
