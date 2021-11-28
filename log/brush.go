package log

// Brush is pretty string
type Brush func(string) string

// NewBrush is the constructor of Brush
func NewBrush(color string) Brush {
	pre := "\033["
	reset := "\033[0m"
	return func(text string) string {
		return pre + color + "m" + text + reset
	}
}

var colors = []Brush{
	NewBrush("1;36"), // debug			cyan
	NewBrush("1;32"), // info
	NewBrush("1;33"), // warn    yellow
	NewBrush("1;31"), // error      red
	NewBrush("1;35"), // fatal   magenta
}
