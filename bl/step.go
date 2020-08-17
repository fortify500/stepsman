package bl

type Step struct {
	Description   string
	Type          string
	Action        string
	Configuration Configuration
}

type Configuration struct {
	Cmd string
	Arguments []string
}
