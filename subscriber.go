package record

import (
	"path/filepath"
	"strconv"
	"time"

	m7sdb "github.com/zzs89117920/m7s-db"
	"go.uber.org/zap"
	. "m7s.live/engine/v4"
)

type Recorder struct {
	Subscriber
	SkipTS  uint32
	*Record `json:"-" yaml:"-"`
	newFile bool // 创建了新的文件
	append  bool // 是否追加模式
	
}

func (r *Recorder) start() {
	RecordPluginConfig.recordings.Store(r.ID, r)
	r.PlayRaw()
	RecordPluginConfig.recordings.Delete(r.ID)
}

func (r *Recorder) cut(absTime uint32) {
	if ts := absTime - r.SkipTS; time.Duration(ts)*time.Millisecond >= r.Fragment {
		r.SkipTS = absTime
		r.newFile = true
	}
}

func (r *Recorder) OnEvent(event any) {
	switch v := event.(type) {
	case ISubscriber:
		timename := time.Now().Format("2006010215") 
		filename := timename 
		if r.Fragment == 0 {
			filename = r.Stream.Path + r.Ext
		} else {
			var count int64
			db := 	m7sdb.MysqlDB()
			db.Model(&MediaRecord{}).Where("stream_path = ?", r.Stream.Path).Count(&count)
			if(count>0){
				filename += "_" + strconv.FormatInt(count, 10)
			}
			filename = filepath.Join(r.Stream.Path, filename) + r.Ext
		}
		if file, err := r.CreateFileFn(filename, r.append); err == nil {
			r.SetIO(file)
			db := 	m7sdb.MysqlDB()
			mr := &MediaRecord{
				CreateTime: time.Now(),
				Status: 1,
				StreamPath: r.Stream.Path,
				FileName: timename,
				Type: 2,
				RecordId: r.ID,
			}
			db.Create(&mr)
		} else {
			r.Error("create file failed", zap.Error(err))
			r.Stop()
		}
	case AudioFrame:
		// 纯音频流的情况下需要切割文件
		if r.Fragment > 0 && r.VideoReader == nil {
			r.cut(v.AbsTime)
		}
	case VideoFrame:
		if r.Fragment > 0 && v.IFrame {
			r.cut(v.AbsTime)
		}
	default:
		r.Subscriber.OnEvent(event)
	}
}
