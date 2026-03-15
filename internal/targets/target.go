package targets

type Target struct {
	ID         string
	Namespace  string
	PodName    string
	PodIP      string
	RemotePort int
	Labels     map[string]string
}
