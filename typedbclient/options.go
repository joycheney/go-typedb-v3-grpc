package typedbclient

import (
	"net/url"
)

// Options TypeDB client configuration options (simplified for ease of use)
type Options struct {
	// Core connection configuration (user must configure these)
	Address  string // TypeDB server address (default: 127.0.0.1:1729)
	Username string // Username (default: admin)
	Password string // Password (default: password)

	// Note: Other gRPC parameters (KeepAliveTime, KeepAliveTimeout, MaxRecvMsgSize, MaxSendMsgSize)
	// are now internal and use official TypeDB standards to reduce configuration complexity
}

// DefaultOptions returns default configuration options
func DefaultOptions() *Options {
	return &Options{
		Address:  "127.0.0.1:1729",
		Username: "admin",
		Password: "password",
		// gRPC parameters are now internal and use TypeDB official standards
	}
}

// LocalOptions returns configuration options for local development environment
func LocalOptions() *Options {
	opts := DefaultOptions()
	opts.Address = "127.0.0.1:1729"
	return opts
}

// ParseURL parses configuration options from URL string
// Supported format: typedb://username:password@host:port
// Note: Advanced gRPC parameters are now internal to simplify configuration
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

	// Advanced gRPC parameters are no longer configurable via URL to reduce complexity
	// They use TypeDB official standards internally

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
	// gRPC parameters are now handled internally with TypeDB official standards
}