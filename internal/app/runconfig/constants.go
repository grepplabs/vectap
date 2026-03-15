package runconfig

const (
	SourceTypeDirect     = "direct"
	SourceTypeKubernetes = "kubernetes"

	FormatText = "text"
	FormatJSON = "json"
	FormatYAML = "yaml"

	DefaultNamespace   = "default"
	DefaultSelector    = "app.kubernetes.io/name=vector"
	DefaultDirectURL   = "http://127.0.0.1:8686/graphql"
	DefaultVectorPort  = 8686
	DefaultTapInterval = 500
	DefaultTapLimit    = 100
	DefaultIncludeMeta = true
)
