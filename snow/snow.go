package snow

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SnowSys struct {
	*utils.SnowKey
	RedisConn *utils.RedisConn
	Tag, Term string
	Now       int64
}

var snowlock rwmutex

type rwmutex struct {
	//m map[string]*sync.RWMutex
	l *sync.Mutex
}

func init() {
	//snowlock = rwmutex{m: map[string]*sync.RWMutex{}}
	snowlock = rwmutex{l: new(sync.Mutex)}
}

func NeedRotate(snowsys *SnowSys, snow models.Snow) (bl bool) {
	now := snowsys.Now
	b, _ := snowsys.RedisConn.Dos("HGET", snowsys.Key, "e_time")
	if b != nil {
		endt, _ := strconv.ParseInt(string(b.([]byte)), 10, 64)
		if endt < now {
			bl = true
			snowlock.l.Lock()
			snowsys.RedisConn.Dos("RENAME", snowsys.Key, snowsys.Key+"_rotate")
			if !snowsys.SnowKey.KeyCheck {
				end := utils.DurationMap[snow.InterValDuration](now, snow.Interval)
				start := utils.DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
				snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
			}
			snowlock.l.Unlock()
		} else {
			return
		}
	} else if !snowsys.SnowKey.KeyCheck {
		end := utils.DurationMap[snow.InterValDuration](now, snow.Interval)
		start := utils.DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
		snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
	}
	return

}

func Rotate(snowsys *SnowSys, snows []models.Snow) {
	snowsys.RedisConn = utils.NewRedisConn(snowsys.Tag)
	defer snowsys.RedisConn.Close()
	tag := snowsys.Tag
	term := snowsys.Term
	if len(snows) == 0 || !NeedRotate(snowsys, snows[0]) {
		return
	}
	b, _ := snowsys.RedisConn.Dos("HGETALL", snowsys.Key+"_rotate")
	if b == nil {
		return
	}
	tb1 := b.([]interface{})
	if len(tb1) == 0 {
		return
	}
	defer snowsys.RedisConn.Dos("DEL", snowsys.Key+"_rotate")
	go func(tb []interface{}) {
		dm := map[string]interface{}{}
		for i := 0; i < len(tb); i = i + 2 {
			dm[string(tb[i].([]uint8))], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
		}
		now := snowsys.Now
		session := utils.DB(tag)
		data := NewModel(tag, term)
		var lasttime int64
		retatedata := ModelDatas{}
		for sk, s := range snows {
			key := snowsys.Key + "_" + fmt.Sprintf("%d", s.Interval) + "_" + s.InterValDuration
			if sk == 0 {
				session.Where("key = ?", key).Find(&data)
				data.EndTime = dm["e_time"].(int64)
				data.StartTime = utils.DurationMap[s.TimeoutDuration+"l"](data.EndTime, s.Timeout)
				td := ModelDatas{}
				data.Data = append(data.Data, dm)
				retatedata = data.Data
				lasttime = data.StartTime
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.StartTime {
							td = data.Data[k:]
							retatedata = data.Data[:k]
							break
						}
					}
				}
				data.Data = td
				if data.Key == "" {
					data.Index = snowsys.Index
					data.Key = key
					data.Tag = tag
					data.Term = term
				}
				//cinfo, err := mc.Upsert(bson.M{"s_key": key}, bson.M{"$set": bson.M{"s_time": data.STime, "e_time": data.ETime, "tag": tag, "term": term, "data": td, "index": snowsys.Index}})
				//mc.Upsert(bson.M{"s_key": key}, data)
				if err := session.Where("key = ?", key).Save(&data).Error; err != nil {
					panic(err)
				}
				if len(retatedata) == 0 {
					break
				}
			} else {
				data = NewModel(tag, term)
				session.Where("key = ?", key).Find(&data)
				data.EndTime = lasttime
				data.StartTime = utils.DurationMap[s.TimeoutDuration+"l"](data.EndTime, s.Timeout)
				lasttime = data.StartTime
				ttt := retatedata
				td := ModelDatas{}
				retatedata = data.Data
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.StartTime {
							td = data.Data[k:]
							retatedata = data.Data[:k]
							break
						}
					}
				}

				for _, v := range ttt {
					o := false
					tmpsnow := snows[sk]
					v["e_time"] = utils.DurationMap[tmpsnow.InterValDuration](utils.TInt64(v["e_time"]), tmpsnow.Interval)
					v["s_time"] = utils.DurationMap[tmpsnow.InterValDuration+"l"](utils.TInt64(v["e_time"]), tmpsnow.Interval)
					lasttime = utils.TInt64(v["e_time"])
					for k1, v1 := range td {
						if v["s_time"].(int64) >= v1["s_time"].(int64) && v["e_time"].(int64) <= v1["e_time"].(int64) {
							for tk, tv := range v {
								if tk != "s_time" && tk != "e_time" {
									if v2, ok := v1[tk]; ok {
										v1[tk] = utils.TFloat64(v2) + utils.TFloat64(tv)
									} else {
										v1[tk] = tv
									}
								}
							}
							td[k1] = v1
							o = true
						}
					}
					if !o {
						if v["s_time"].(int64) >= data.StartTime {
							td = append(td, v)
						} else {
							retatedata = append(retatedata, v)
						}

					}
				}
				data.Data = td
				data.Indexs(snowsys.Index)
				if err := session.Where("key = ?", key).Save(&data).Error; err != nil {
					panic(err)
				}
				if len(retatedata) == 0 {
					break
				}
			}
		}
		if len(retatedata) > 0 {

			data = NewModel(tag, term)
			if err := session.Where("key = ?", snowsys.Key).Find(&data).Error; err != nil {
				panic(err)
			}

			for _, v := range retatedata {
				for k1, v1 := range v {
					if k1 == "s_time" || k1 == "e_time" {
						continue
					}
					// if v2, ok := tmp[k1]; ok {
					// 	tmp[k1] = utils.TFloat64(v2) + utils.TFloat64(v1)
					// } else {
					// 	tmp[k1] = v1
					// }
					data.Data[0][k1] = utils.TFloat64(data.Data[0][k1]) + utils.TFloat64(v1)
				}
			}
			data.EndTime = now
			data.Indexs(snowsys.Index)
			if err := session.Where("key = ?", snowsys.Key).Save(&data).Error; err != nil {
				panic(err)
			}
			// mc.Upsert(bson.M{"s_key": snowsys.Key}, bson.M{"$inc": tmp, "$set": bson.M{
			// 	"e_time": now, "tag": tag, "term": term, "index": snowsys.Index}})

		}
	}(tb1)
}

//rds key rotate
func ClearRedisKey(tag string) {
	for {
		now := utils.GetNowSec()
		if utils.Sec2Str("15", now) == "04" {
			utils.Log.INFO.Println("Do rds'key rollback", utils.Sec2Str("2006-01-02 15:04", now))
			rdsconn := utils.NewRedisConn(tag)
			defer rdsconn.Close()

			keys, err := rdsconn.Dos("KEYS", "fs_*")
			if err != nil {
				continue
			}
			var index map[string]interface{}
			var ks, tl []string
			var tk string
			for _, k := range keys.([]interface{}) {
				tk = string(k.([]byte))
				tl = strings.Split(tk, "_")
				tag = tl[1]
				ks = []string{}
				index = map[string]interface{}{}
				for i := 2; i < len(tl[1:]); i = i + 2 {
					ks = append(ks, tl[i])
					index[tl[i][1:]] = tl[i+1]
				}
				for tag, terms := range models.TermConfigMap {
					for term, config := range terms {
						if fmt.Sprintf("%v", config.Key) == fmt.Sprintf("%v", ks) {
							newSnow := &SnowSys{
								&utils.SnowKey{
									tk, index,
									true,
								},
								nil,
								tag,
								term,
								now,
							}
							Rotate(newSnow, config.Snow)
						}
					}
				}
			}
		}
		time.Sleep(1 * time.Hour)
	}
}
