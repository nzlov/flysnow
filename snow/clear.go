package snow

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
)

type ClearReq struct {
	TagTerms map[string][]string    `json:"tag_terms" `
	Query    map[string]interface{} `json:"query"`
}
type clearList struct {
	Tag, Term string
	RdsKey    string
	Query     map[string]interface{}
}

func Clear(body []byte) (error, int) {
	req := ClearReq{}
	list := []clearList{}
	err := utils.JsonDecode(body, &req)
	if err != nil {
		return err, models.ErrData
	}
	//解析需要清理的统计项
	var find bool
	var rdskey string
	var query map[string]interface{}
	for tag, terms := range req.TagTerms {
		for _, term := range terms {
			if termconfig, ok := models.TermConfigMap[tag][term]; ok {
				rdskey = fmt.Sprintf("%s_%s_*", models.RedisKT, tag)
				query = map[string]interface{}{}
				for key, value := range req.Query {
					find = false
					for _, k := range termconfig.Key {
						if "@"+key == k {
							find = true
							rdskey += fmt.Sprintf("@%s_%s", key, value)
							query["index."+key] = value
						}
					}
					if !find {
						return models.ErrNew(fmt.Sprintf("%s-%s key:%s not found", tag, term, key)), models.ErrClear
					}
				}
				rdskey += "*"
				list = append(list, clearList{tag, term, rdskey, query})
			} else {
				return models.ErrNew(fmt.Sprintf("%s-%s not found", tag, term)), models.ErrClear
			}
		}
	}
	var key string

	for _, clear := range list {
		session := utils.DB(clear.Tag)
		//clear redis
		rdsconn := utils.NewRedisConn(clear.Tag)
		keys, _ := rdsconn.Dos("KEYS", clear.RdsKey)
		for _, k := range keys.([]interface{}) {
			key = string(k.([]byte))
			rdsconn.Dos("DEL", key)
		}
		rdsconn.Close()
		//clear mongo
		if err := session.Where("index <@ ?::jsonb", string(utils.JsonEncode(clear.Query, false))).Delete(NewModel(clear.Tag, clear.Term)).Error; err != nil {
			panic(err)
		}
	}
	return nil, 0
}
