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
		//开始归档
		dm := map[string]interface{}{}
		//tb为redis 数据
		//把tb 转化成map
		for i := 0; i < len(tb); i = i + 2 {
			dm[string(tb[i].([]uint8))], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
		}
		now := snowsys.Now
		session := utils.DB(tag)
		data := NewModel(tag, term)
		//存储归档集合的开始时间，用作下一个归档集合的结束时间
		var lasttime int64
		retatedata := ModelDatas{}
		session := utils.MgoSessionDupl(tag)
		defer session.Close()

		for sk, s := range snows {
			//key = fs_shop_@shopid_xxxx_1_m
			key := snowsys.Key + "_" + fmt.Sprintf("%d", s.Interval) + "_" + s.InterValDuration
			//如果为第一个归档,表示redis 入mongo
			if sk == 0 {
				//获取第一个归档mongo数据集合
				//第一归档表示从redis归档到mongo，时间跨度
				session.Where("key = ?", key).Find(&data)
				//重置mongo第一个归档数据集合的截止时间,为redis数据的截止时间
				data.EndTime = dm["e_time"].(int64)
				//根据第一归档数据集合的存储时间总长度，计算当前集合的开始时间
				data.StartTime = utils.DurationMap[s.TimeoutDuration+"l"](data.EndTime, s.Timeout)
				td := ModelDatas{}
				//将最新redis数据 append到第一归档数据集合-默认redis数据的时间间隔为第一归档数据集合单位数据的时间跨度
				data.Data = append(data.Data, dm)
				retatedata = data.Data
				//复制当前归档集合开始时间
				lasttime = data.StartTime
				//循环第一归档内所有单位数据，判断是否超过此集合时间限制
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.StartTime {
							//因为归档数据所有单位是按照时间先后进行append的，如果找到第一个不超期时间，剩余皆不超期
							td = data.Data[k:]
							retatedata = data.Data[:k]
							break
						}
					}
				}
				//重新复制归档数据集合
				data.Data = td
				if data.Key == "" {
					//如果第一归档数据不存在，进行初始化
					data.Index = snowsys.Index
					data.Key = key
					data.Tag = tag
					data.Term = term
				}
				//cinfo, err := mc.Upsert(bson.M{"s_key": key}, bson.M{"$set": bson.M{"s_time": data.STime, "e_time": data.ETime, "tag": tag, "term": term, "data": td, "index": snowsys.Index}})
				//mc.Upsert(bson.M{"s_key": key}, data)
				//第一归档数据upsert，确保一定至少有一条，不存在则写入
				if err := session.Where("key = ?", key).Save(&data).Error; err != nil {
					panic(err)
				}
				if len(retatedata) == 0 {
					//如果不存在超期数据，结束循环
					break
				}
				//如果有，继续下一次归档
			} else {
				data = NewModel(tag, term)
				//查询第sk个归档数据集合
				session.Where("key = ?", key).Find(&data)
				//重置mongo第sk个归档数据集合的截止时间,为上一个归档集合的开始时间
				data.EndTime = lasttime
				//根据集合的存储时间总长度，计算当前集合的开始时间
				data.StartTime = utils.DurationMap[s.TimeoutDuration+"l"](data.EndTime, s.Timeout)
				//复制当前归档集合开始时间
				lasttime = data.StartTime
				//赋值上一个集合所剩超期需归档数据集合
				ttt := retatedata
				td := ModelDatas{}
				retatedata = data.Data
				//循环第sk归档内所有单位数据，判断是否超过此集合时间限制
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.StartTime {
							//因为归档数据所有单位是按照时间先后进行append的，如果找到第一个不超期时间，剩余皆不超期
							td = data.Data[k:]
							retatedata = data.Data[:k]
							break
						}
					}
				}
				//循环上个归档集合所遗留的超期集合
				for _, v := range ttt {
					o := false
					v["e_time"] = utils.DurationMap[s.InterValDuration](utils.TInt64(v["e_time"]), s.Interval)
					v["s_time"] = utils.DurationMap[s.InterValDuration+"l"](utils.TInt64(v["e_time"]), s.Interval)
					/*lasttime = utils.TInt64(v["e_time"])*/
					for k1, v1 := range td {
						//判断超期集合的元素是否属于当前集合中的一个子项，如果是累加到子项里面
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
						//如果不是且被当前归档集合时间包含，在当前集合新增一个子项
						if v["s_time"].(int64) >= data.StartTime {
							td = append(td, v)
						} else {
							//放到过期集合，进行下一个归档
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
