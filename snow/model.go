package snow

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"flysnow/models"
	"flysnow/utils"

	"github.com/nzlov/jsonql"
	"gopkg.in/mgo.v2/bson"
)

type Model struct {
	Key       string `json:"s_key" gorm:"primary_key"`
	StartTime int64  `json:"s_time"`
	EndTime   int64  `json:"e_time"`

	Tag  string
	Term string

	Index ModelData  `gorm:"type:jsonb"`
	Data  ModelDatas `json:"data" gorm:"type:jsonb"`
}

func NewModel(tag, term string) Model {
	m := Model{
		Tag:  tag,
		Term: term,

		Index: ModelData{},
		Data:  ModelDatas{},
	}
	utils.DB(tag).AutoMigrate(&m)

	return m
}
func (model Model) Indexs(m map[string]interface{}) {
	for k, v := range m {
		model.Index[k] = v
	}
}

func (m Model) TableName() string {
	return models.MongoDT + "_" + m.Tag + "_" + m.Term
}

type ModelData map[string]interface{}
type ModelDatas []ModelData

func (j ModelData) Value() (driver.Value, error) {
	return json.Marshal(&j)
}

// Scan scan value into Jsonb
func (j *ModelData) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	return json.Unmarshal(bytes, j)
}
func (j ModelDatas) Value() (driver.Value, error) {
	return json.Marshal(&j)
}

// Scan scan value into Jsonb
func (j *ModelDatas) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	return json.Unmarshal(bytes, j)
}

func (b *ModelDatas) Check(s bson.M) {

	data, err := json.Marshal(b)
	if err != nil {
		fmt.Println("Err0:", err)
		return
	}
	parser, err := jsonql.NewStringQuery(string(data))

	if err != nil {
		fmt.Println("Err1:", err)
		return
	}
	ssss := BsonToQuery(s)
	str, err := parser.Query(ssss)
	if err != nil {
		fmt.Println("Err2:", err)
		return
	}

	data, err = json.Marshal(str)
	if err != nil {
		fmt.Println("Err3:", err)
		return
	}
	fmt.Println("JSON:", string(data))
	err = json.Unmarshal(data, b)
	if err != nil {
		fmt.Println("Err4:", err)
	}
}

func MapToBson(m interface{}) bson.M {
	b := bson.M{}
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Println("Err0:", err)
		return b
	}
	json.Unmarshal(data, b)
	return b
}

func BsonToQuery(s bson.M) string {
	str := ""

	for k, v := range s {
		switch k {
		case "$or":
			switch t := v.(type) {
			case []bson.M:
				for _, v1 := range t {
					str += fmt.Sprintf("|| %v", BsonToQuery(v1))
				}
			case []interface{}:
				for _, v1 := range t {
					str += fmt.Sprintf("|| %v", BsonToQuery(MapToBson(v1)))
				}
			}
			str = strings.Trim(str, "||")
		case "$ne":
			return fmt.Sprintf("!= %v ", v)
		case "$gt":
			return fmt.Sprintf(">%v ", v)
		case "$lt":
			return fmt.Sprintf("<%v ", v)
		case "$gte":
			return fmt.Sprintf(">=%v ", v)
		case "$lte":
			return fmt.Sprintf("<=%v ", v)
		default:
			switch t := v.(type) {
			case bson.M:
				str += fmt.Sprintf("&& %v%v ", k, BsonToQuery(t))
			default:
				str += fmt.Sprintf("&& %v=%v ", k, t)
			}

		}

	}
	return strings.Trim(str, "&&")
}
