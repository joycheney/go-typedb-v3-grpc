package typedbclient

import (
	"net/url"
	"strconv"
	"time"
)

// Options TypeDB client configuration options (follows Go standard library conventions)
type Options struct {
	// Connection configuration
	Address  string // TypeDB server address (default: 127.0.0.1:1729)
	Username string // Username (default: admin)
	Password string // Password (default: password)

	// Performance configuration
	KeepAliveTime    time.Duration // TCP keep-alive time (default: 30s)
	KeepAliveTimeout time.Duration // TCP keep-alive timeout (default: 10s)
	MaxRecvMsgSize   int           // Maximum receive message size (default: 16MB)
	MaxSendMsgSize   int           // Maximum send message size (default: 16MB)
}

// DefaultOptions returns default configuration options
func DefaultOptions() *Options {
	return &Options{
		Address:          "127.0.0.1:1729",
		Username:         "admin",
		Password:         "password",
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
		MaxRecvMsgSize:   16 * 1024 * 1024, // 16MB
		MaxSendMsgSize:   16 * 1024 * 1024, // 16MB
	}
}

// LocalOptions returns configuration options for local development environment
func LocalOptions() *Options {
	opts := DefaultOptions()
	opts.Address = "127.0.0.1:1729"
	return opts
}

// ParseURL parses configuration options from URL string
// Supported format: typedb://username:password@host:port?keepalive=30s&timeout=10s
func ParseURL(rawurl string) (*Options, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	opts := DefaultOptions()

	// Parse address
	if u.Host != "" {
		opts.Address = u.Host
	}

	// Parse authentication information
	if u.User != nil {
		opts.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			opts.Password = password
		}
	}

	// Parse query parameters
	query := u.Query()
	if keepalive := query.Get("keepalive"); keepalive != "" {
		if duration, err := time.ParseDuration(keepalive); err == nil {
			opts.KeepAliveTime = duration
		}
	}
	if timeout := query.Get("timeout"); timeout != "" {
		if duration, err := time.ParseDuration(timeout); err == nil {
			opts.KeepAliveTimeout = duration
		}
	}
	if maxrecv := query.Get("max_recv_size"); maxrecv != "" {
		if size, err := strconv.Atoi(maxrecv); err == nil {
			opts.MaxRecvMsgSize = size
		}
	}
	if maxsend := query.Get("max_send_size"); maxsend != "" {
		if size, err := strconv.Atoi(maxsend); err == nil {
			opts.MaxSendMsgSize = size
		}
	}

	return opts, nil
}

// fillDefaults fills missing default values
func (opts *Options) fillDefaults() {
	defaults := DefaultOptions()

	if opts.Address == "" {
		opts.Address = defaults.Address
	}
	if opts.Username == "" {
		opts.Username = defaults.Username
	}
	if opts.Password == "" {
		opts.Password = defaults.Password
	}
	if opts.KeepAliveTime == 0 {
		opts.KeepAliveTime = defaults.KeepAliveTime
	}
	if opts.KeepAliveTimeout == 0 {
		opts.KeepAliveTimeout = defaults.KeepAliveTimeout
	}
	if opts.MaxRecvMsgSize == 0 {
		opts.MaxRecvMsgSize = defaults.MaxRecvMsgSize
	}
	if opts.MaxSendMsgSize == 0 {
		opts.MaxSendMsgSize = defaults.MaxSendMsgSize
	}
}