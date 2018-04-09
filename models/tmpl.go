package models

type FSFuncStruct struct {
	Paramstype []string
	ReturnType []string
	FuncBody   string
	Name       string
}

var FSFuncMap = map[string]FSFuncStruct{
	"filter": FSFuncStruct{
		Name:       "filter",
		Paramstype: []string{"string"},
		ReturnType: []string{"bool"},
		FuncBody: `
type {{name}} struct{
  *utils.FilterBtree
  offset int64
  whence int
  name string
  duration string
  }
var {{names}}  func(key string,t int64)bool
func (f *{{name}})Do(key string,t int64)bool{
  res,update:=f.GetSet(utils.FilterBtreeItem{Key:f.name+key,T:t})
  if !update || f.offset==0{
    return !update
    }
  if f.whence==0{
    return res.T<utils.DurationMap[f.duration+"l"](utils.DurationMap[f.duration](t,f.offset),f.offset)
  }else{
    return res.T<utils.DurationMap[f.duration+"l"](t,f.offset)
  }
}
    `,
	},
	"rangesum": FSFuncStruct{
		Name:       "rangesum",
		Paramstype: []string{"rangelist"},
		FuncBody: `
    for _,i:=range d.req.{{name}}{
		  commands.Commands = append(commands.Commands, RdsCommand{Cmd: "HINCRBYFLOAT", V: []interface{}{i.Key, i.Value}})
      }
    `,
	},
}
