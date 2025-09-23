package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Client TypeDB gRPC client (lock-free design, consistent with HTTP client architecture)
type Client struct {
	address    string
	username   string
	password   string
	conn       *grpc.ClientConn
	grpcClient pb.TypeDBClient // Generated gRPC client

	// Lock-free authentication state management
	tokenValue     atomic.Value // Store JWT token (string)
	authInProgress atomic.Bool  // Mark if authentication is in progress (lock-free CAS operation)

	// Connection management
	connMu         sync.RWMutex // Only for connection management, other operations are lock-free
	reconnecting   atomic.Bool
	lastConnTime   atomic.Value // time.Time
}

// Config client configuration
type Config struct {
	Address  string
	Username string
	Password string

	// gRPC specific configuration
	KeepAliveTime    time.Duration
	KeepAliveTimeout time.Duration
	MaxRecvMsgSize   int
	MaxSendMsgSize   int
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Address:          "127.0.0.1:1729", // TypeDB gRPC default port
		Username:         "admin",
		Password:         "password",
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
		MaxRecvMsgSize:   16 * 1024 * 1024, // 16MB
		MaxSendMsgSize:   16 * 1024 * 1024, // 16MB
	}
}

// NewClient creates new TypeDB gRPC client
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Fill default values
	if config.Address == "" {
		config.Address = "127.0.0.1:1729"
	}
	if config.Username == "" {
		config.Username = "admin"
	}
	if config.Password == "" {
		config.Password = "password"
	}
	if config.KeepAliveTime == 0 {
		config.KeepAliveTime = 30 * time.Second
	}
	if config.KeepAliveTimeout == 0 {
		config.KeepAliveTimeout = 10 * time.Second
	}
	if config.MaxRecvMsgSize == 0 {
		config.MaxRecvMsgSize = 16 * 1024 * 1024 // 16MB
	}
	if config.MaxSendMsgSize == 0 {
		config.MaxSendMsgSize = 16 * 1024 * 1024 // 16MB
	}

	client := &Client{
		address:  config.Address,
		username: config.Username,
		password: config.Password,
	}

	// Establish gRPC connection
	if err := client.connect(config); err != nil {
		return nil, fmt.Errorf("failed to connect to TypeDB: %w", err)
	}

	// Execute initial authentication
	if err := client.authenticate(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	return client, nil
}

// connect establishes gRPC connection
func (c *Client) connect(config *Config) error {
	// gRPC connection options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                config.KeepAliveTime,
			Timeout:             config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(config.MaxSendMsgSize),
		),
	}

	// Establish connection
	conn, err := grpc.Dial(c.address, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.grpcClient = pb.NewTypeDBClient(conn) // Create gRPC client
	c.connMu.Unlock()

	c.lastConnTime.Store(time.Now())

	return nil
}

// authenticate executes authentication (lock-free design)
func (c *Client) authenticate() error {
	// Use CAS operation to prevent concurrent authentication
	if !c.authInProgress.CompareAndSwap(false, true) {
		// Other goroutine is authenticating, wait for completion
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			if !c.authInProgress.Load() {
				// Check if authentication succeeded
				if token := c.getToken(); token != "" {
					return nil
				}
				break
			}
		}
		return fmt.Errorf("authentication timeout")
	}
	defer c.authInProgress.Store(false)

	// Use ConnectionOpen to establish connection and get token
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build connection request
	req := &pb.Connection_Open_Req{
		Version:       pb.Version_VERSION, // Use version 7
		DriverLang:    "go",
		DriverVersion: "3.0.0",
		Authentication: &pb.Authentication_Token_Create_Req{
			Credentials: &pb.Authentication_Token_Create_Req_Password_{
				Password: &pb.Authentication_Token_Create_Req_Password{
					Username: c.username,
					Password: c.password,
				},
			},
		},
	}

	c.connMu.RLock()
	client := c.grpcClient
	c.connMu.RUnlock()

	if client == nil {
		return fmt.Errorf("gRPC client not initialized")
	}

	// Call ConnectionOpen instead of AuthenticationTokenCreate
	resp, err := client.ConnectionOpen(ctx, req)
	if err != nil {
		return fmt.Errorf("connection open failed: %w", err)
	}

	if resp.Authentication == nil || resp.Authentication.Token == "" {
		return fmt.Errorf("received empty authentication token")
	}

	// Store token (lock-free)
	c.tokenValue.Store(resp.Authentication.Token)

	return nil
}

// getToken safely gets token (lock-free access)
func (c *Client) getToken() string {
	if val := c.tokenValue.Load(); val != nil {
		if token, ok := val.(string); ok {
			return token
		}
	}
	return ""
}

// withAuth adds authentication info to context
func (c *Client) withAuth(ctx context.Context) context.Context {
	token := c.getToken()
	if token == "" {
		return ctx
	}

	// Add Bearer token to metadata
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(ctx, md)
}

// tryReconnect attempts reconnection (lock-free design)
func (c *Client) tryReconnect() error {
	// Use CAS to prevent concurrent reconnection
	if !c.reconnecting.CompareAndSwap(false, true) {
		// Other goroutine is reconnecting, wait
		for i := 0; i < 30; i++ {
			time.Sleep(100 * time.Millisecond)
			if !c.reconnecting.Load() {
				return nil
			}
		}
		return fmt.Errorf("reconnection timeout")
	}
	defer c.reconnecting.Store(false)

	// Close old connection
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	// Reconnect
	config := &Config{
		Address:  c.address,
		Username: c.username,
		Password: c.password,
	}

	if err := c.connect(config); err != nil {
		return err
	}

	// Re-authenticate
	return c.authenticate()
}

// Close closes client connection
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetConn gets gRPC connection (internal use)
func (c *Client) GetConn() *grpc.ClientConn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// executeWithRetry executor with retry (unified request execution pattern, similar to HTTP client's executeRequest)
func (c *Client) executeWithRetry(ctx context.Context, operation func(context.Context) error) error {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Add authentication info
		authCtx := c.withAuth(ctx)

		// Execute operation
		err := operation(authCtx)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if re-authentication is needed
		if isAuthError(err) {
			// Attempt re-authentication
			if authErr := c.authenticate(); authErr != nil {
				return fmt.Errorf("reauthentication failed: %w", authErr)
			}
			continue
		}

		// Check if reconnection is needed
		if isConnectionError(err) {
			// Attempt reconnection
			if reconnErr := c.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnErr)
			}
			continue
		}

		// Other errors, retry with exponential backoff
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", maxRetries, lastErr)
}

// isAuthError checks if it's an authentication error
func isAuthError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code - according to document line 230 specification: UNAUTHENTICATED status code
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Unauthenticated {
			return true
		}
	}

	// Check error message content - according to document line 230 specification: "Invalid token"
	errMsg := err.Error()
	return strings.Contains(errMsg, "Invalid token") ||
		   strings.Contains(errMsg, "token expired") ||
		   strings.Contains(errMsg, "authentication failed") ||
		   strings.Contains(errMsg, "unauthorized")
}

// isConnectionError checks if it's a connection error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code - connection-related error status
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted:
			return true
		}
	}

	// Check error message content - connection-related error info
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "connection") ||
		   strings.Contains(errMsg, "network") ||
		   strings.Contains(errMsg, "dial") ||
		   strings.Contains(errMsg, "timeout") ||
		   strings.Contains(errMsg, "unavailable") ||
		   strings.Contains(errMsg, "refused") ||
		   strings.Contains(errMsg, "broken") ||
		   strings.Contains(errMsg, "reset")
}

// CreateAuthToken creates authentication token (independent authentication API)
func (c *Client) CreateAuthToken(ctx context.Context, username, password string) (string, error) {
	var token string

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		req := &pb.Authentication_Token_Create_Req{
			Credentials: &pb.Authentication_Token_Create_Req_Password_{
				Password: &pb.Authentication_Token_Create_Req_Password{
					Username: username,
					Password: password,
				},
			},
		}

		resp, err := client.AuthenticationTokenCreate(ctx, req)
		if err != nil {
			return err
		}

		token = resp.Token
		return nil
	})

	return token, err
}

// Server server information
type Server struct {
	Address string
}

// ListServers lists all servers
func (c *Client) ListServers(ctx context.Context) ([]*Server, error) {
	var servers []*Server

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		req := &pb.ServerManager_All_Req{}
		resp, err := client.ServersAll(ctx, req)
		if err != nil {
			return err
		}

		servers = make([]*Server, 0, len(resp.Servers))
		for _, pbServer := range resp.Servers {
			server := &Server{
				Address: pbServer.Address,
			}
			servers = append(servers, server)
		}
		return nil
	})

	return servers, err
}