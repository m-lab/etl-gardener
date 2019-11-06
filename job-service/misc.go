package job

// Config provides a config for a job service.
type Config struct {
	Bucket       string
	TypePrefixes []string // Prefixes of all dates to be handled.
}
