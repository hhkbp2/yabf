package binding

import (
	"github.com/hhkbp2/yabf"
)

func AddBindings() {
	yabf.Databases["mysql"] = func() yabf.DB {
		return NewMysqlDB()
	}
	yabf.Databases["tikv"] = func() yabf.DB {
		return NewTikvDB()
	}
}
