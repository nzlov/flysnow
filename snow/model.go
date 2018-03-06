package snow

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"flysnow/models"
	"flysnow/utils"

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

func (j ModelData) Check(d bson.M) bool {
	if len(j) >= len(d) {
		for k, v := range d {
			if jv, ok := j[k]; !ok {
				return false
			} else if v != jv {
				return false
			}
		}
		return true
	}
	return false
}
