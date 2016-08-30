package binding

import (
	"github.com/hhkbp2/yabf"
)

func AddBindings() {
	yabf.Databases["mysql"] = func() yabf.DB {
		return NewMysqlDB()
	}
}
