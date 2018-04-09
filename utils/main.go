package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"
)

var DurationMap = map[string]func(t, num int64) int64{
	"s":  durationS,
	"h":  durationH,
	"d":  durationD,
	"m":  durationM,
	"y":  durationY,
	"sl": durationSL,
	"hl": durationHL,
	"dl": durationDL,
	"ml": durationML,
	"yl": durationYL,
}

var weekdaymap = map[string]int64{
	"Monday":    7,
	"Tuesday":   6,
	"Wednesday": 5,
	"Thursday":  4,
	"Friday":    3,
	"Saturday":  2,
	"Sunday":    1,
}

func durationS(t, num int64) int64 {
	return t - t%num + num
}
func durationH(t, num int64) int64 {
	num = num * 60 * 60
	return t - t%num + num
}
func durationD(t, num int64) int64 {
	sm, _ := time.ParseInLocation("20060102", Sec2Str("20060102", t), time.Local)
	new := sm.AddDate(0, 0, int(num))
	return new.Unix()
}

func durationW(t, num int64) int64 {
	weekday := time.Unix(t, 0).Weekday().String()
	long := weekdaymap[weekday] + 24*60*60 + (num-1)*24*7*60*60
	return t - (t+8*60*60)%24*60*60 + long
}
func durationM(t, num int64) int64 {
	sm, _ := time.ParseInLocation("200601", Sec2Str("200601", t), time.Local)
	new := sm.AddDate(0, int(num), 0)
	return new.Unix()
}
func durationY(t, num int64) int64 {
	sm, _ := time.ParseInLocation("2006", Sec2Str("2006", t), time.Local)
	new := sm.AddDate(int(num), 0, 0)
	return new.Unix()
}
func durationSL(e, num int64) int64 {
	return e - num
}
func durationHL(e, num int64) int64 {
	return e - num*60*60
}
func durationDL(e, num int64) int64 {
	return e - num*60*60*24
}
func durationWL(e, num int64) int64 {
	return e - num*7*60*60*24
}
func durationML(e, num int64) int64 {
	sm := time.Unix(e, 0)
	new := sm.AddDate(0, int(-num), 0)
	return new.Unix()
}
func durationYL(e, num int64) int64 {
	sm := time.Unix(e, 0)
	new := sm.AddDate(int(-num), 0, 0)
	return new.Unix()
}

type Timer struct {
	start   time.Time
	n       int64
	ts      int64
	tsone   int64
	AutoEnd int64
	Name    string
}

func (t *Timer) Start() {
	t.start = time.Now()
}
func (t *Timer) End() {
	t.n += 1
	t.ts += time.Since(t.start).Nanoseconds()
	if t.AutoEnd != 0 {
		if t.n%t.AutoEnd == 0 {
			t.tsone = t.ts / t.n
			Log.ERROR.Printf("name:%s,ts:%vms,tsone:%vus", t.Name, t.ts/1000000, t.tsone/1000)
			t.n = 0
			t.ts = 0
		}
	}
}
func (t *Timer) Stop() {
}
func (t *Timer) Count() int64 {
	return t.n
}

var (
	b   []byte
	err error
)

/**
解压json []byte数组
将json解析成对象或者interface
传入obj为引用地址
解析失败obj为空,返回error
*/
func JsonDecode(data []byte, obj interface{}) error {
	return json.Unmarshal(data, obj)
}

/**
把interface压缩成json结构
返回json []byte组
如果压缩失败,返回空[]byte
*/
func JsonEncode(obj interface{}, pretty bool) []byte {
	if pretty {
		b, err = json.MarshalIndent(obj, "", "    ")
	} else {
		b, err = json.Marshal(obj)
	}
	return b
}

/**
   检查文件或目录是否存在
  如果由 filename 指定的文件或目录存在则返回 true，否则返回 false
*/
func FileOrPathIsExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

/*
  遍历目录下所有文件
  path 目录
  recur 是否递归查询
  files 文件列表
  err错误信息
*/
func WalkDir(path string, recur bool) (files []string, err error) {
	files = []string{}
	if !FileOrPathIsExist(path) {
		err = errors.New("file:" + path + " not found")
		return
	}
	if dirs, err := ioutil.ReadDir(path); err == nil {
		for _, f := range dirs {
			if f.IsDir() {
				if recur {
					tl, _ := WalkDir(f.Name(), recur)
					files = append(files, tl...)
				}
			} else {
				files = append(files, f.Name())
			}
		}
	}
	return

}

/*
  递归创建目录
  os.MkdirAll(path string, perm FileMode) error

  path  目录名及子目录
  perm  目录权限位
  error 如果成功返回nil，如果目录已经存在默认什么都不做
*/
func CreatePathAll(path string) error {
	return os.MkdirAll(path, 0777)
}

//整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int(x)
}

//整形转换成字节
func Int64ToBytes(n int64) []byte {
	x := int64(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt64(b []byte) int64 {
	bytesBuffer := bytes.NewBuffer(b)

	var x int64
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int64(x)
}

//返回当前系统时间戳
func GetNowSec() int64 {
	return time.Now().Unix()
}

/**
  根据指定格式字符串转化为时间戳
*/
func Str2Sec(layout, str string) int64 {
	tm2, _ := time.ParseInLocation(layout, str, time.Local)
	return tm2.Unix()
}

/**
  时间戳转化为指定格式字符串
*/
func Sec2Str(layout string, sec int64) string {
	t := time.Unix(sec, 0)
	nt := t.Format(layout)
	return nt
}

/**
  interface 2 float64
*/
func TFloat64(i interface{}) (f float64) {
	switch n := i.(type) {
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case float32:
		f = float64(n)
	case float64:
		f = n
	default:
		f = 0.0
	}
	return f
}

/**
  interface 2 int64
*/
func TInt64(i interface{}) (f int64) {
	switch n := i.(type) {
	case int:
		f = int64(n)
	case int64:
		f = n
	case float32:
		f = int64(n)
	case float64:
		f = int64(n)
	default:
		f = 0
	}
	return f
}
