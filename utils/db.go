package utils

import (
	"github.com/nzlov/gorm"
	_ "github.com/nzlov/gorm/dialects/postgres"
)

var dbs = map[string]*gorm.DB{}

func DBInit(tag string) {
	FSConfig.SetMod(tag)
	db := FSConfig.StringDefault("db", "postgres")
	dburl := FSConfig.StringDefault("dburl", "postgres://:@localhost/flysnow?sslmode=disable")
	d, err := gorm.Open(db, dburl)
	if err != nil {
		Log.ERROR.Print("Could not connect to DB. Error: %s", err)
		return
	}
	d.LogMode(true)
	dbs[tag] = d
}

func DB(tag string) *gorm.DB {
	return dbs[tag]
}
