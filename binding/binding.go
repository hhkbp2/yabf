package binding

import (
	"github.com/hhkbp2/yabf"
)

func AddBindings() {
	yabf.Databases["mysql"] = func() yabf.DB {
		return NewMysqlDB()
	}
	yabf.Databases["cloudtable"] = func() yabf.DB {
		// TODO impl this
		return nil
	}
}
