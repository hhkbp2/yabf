package yabf

type Client struct {
}

type Shell struct {
	Args *Arguemnts
}

func NewShell(Args *Arguemnts) *Shell {
	return &Shell{
		Args: Args,
	}
}

func (self *Shell) main() {

}

func (self *Shell) help() {
}
