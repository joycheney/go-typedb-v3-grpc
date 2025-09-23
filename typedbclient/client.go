package typedbclient

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


// NewClient creates a new TypeDB gRPC client
// Follows Go standard library conventions, uses Options pattern instead of Builder pattern
func NewClient(opts *Options) (*Client, error) {
	if opts == nil {
		opts = DefaultOptions()
	} else {
		// Fill missing default values
		opts.fillDefaults()
	}

	client := &Client{
		address:  opts.Address,
		username: opts.Username,
		password: opts.Password,
	}

	// Establish gRPC connection
	if err := client.connect(opts); err != nil {
		return nil, fmt.Errorf("failed to connect to TypeDB: %w", err)
	}

	// Perform initial authentication
	if err := client.authenticate(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	return client, nil
}

// connect establishes gRPC connection
func (c *Client) connect(opts *Options) error {
	// gRPC connection options
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                opts.KeepAliveTime,
			Timeout:             opts.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(opts.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(opts.MaxSendMsgSize),
		),
	}

	// Establish connection
	conn, err := grpc.Dial(c.address, dialOpts...)
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

// authenticate performs initial authentication (used for first connection)
func (c *Client) authenticate() error {
	// Use CAS operation to prevent concurrent authentication
	if !c.authInProgress.CompareAndSwap(false, true) {
		// Another goroutine is authenticating, wait for completion
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)

			// 先检查是否还在认证中
			if c.authInProgress.Load() {
				continue
			}

			// 认证已结束，再次检查token（添加短暂延迟确保token已被设置）
			time.Sleep(10 * time.Millisecond)
			if token := c.getToken(); token != "" {
				return nil
			}

			// 如果没有token，说明其他goroutine的认证失败了
			return fmt.Errorf("authentication failed by other goroutine")
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

	// Call ConnectionOpen for initial connection
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

// refreshToken refreshes authentication token (used for token expiration)
func (c *Client) refreshToken() error {
	// When token expires completely, we cannot use it to refresh
	// Instead, we must re-authenticate from scratch
	// This avoids the paradox where we need a valid token to get a new token
	return c.authenticate()
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

// withAuth adds authentication information to context
func (c *Client) withAuth(ctx context.Context) context.Context {
	token := c.getToken()
	if token == "" {
		return ctx
	}

	// Add Bearer token to metadata
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(ctx, md)
}

// tryReconnect tries to reconnect (lock-free design)
func (c *Client) tryReconnect() error {
	// Use CAS to prevent concurrent reconnection
	if !c.reconnecting.CompareAndSwap(false, true) {
		// Another goroutine is reconnecting, wait
		for i := 0; i < 30; i++ {
			time.Sleep(100 * time.Millisecond)

			// 检查是否还在重连中
			if c.reconnecting.Load() {
				continue
			}

			// 重连已完成，检查是否成功（通过检查token）
			time.Sleep(10 * time.Millisecond)
			if token := c.getToken(); token != "" {
				return nil
			}

			// 重连失败
			return fmt.Errorf("reconnection failed by other goroutine")
		}
		return fmt.Errorf("reconnection timeout")
	}
	defer c.reconnecting.Store(false)

	// 清空旧token，确保重新认证
	c.tokenValue.Store("")

	// Close old connection
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	// Reconnect
	opts := &Options{
		Address:  c.address,
		Username: c.username,
		Password: c.password,
	}
	opts.fillDefaults()

	if err := c.connect(opts); err != nil {
		return err
	}

	// Reauthenticate（authenticate已有CAS保护，防止并发）
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
		// Add authentication information
		authCtx := c.withAuth(ctx)

		// Execute operation
		err := operation(authCtx)
		if err == nil {
			return nil
		}

		lastErr = err

		// 检查协议错误（token过期时服务器返回HTTP而非gRPC）
		// 协议错误通常表示token已完全失效，需要重新连接
		if isProtocolError(err) {
			// 直接重连并重新认证，避免token刷新悖论
			// tryReconnect内部已有CAS保护，确保只有一个goroutine执行重连
			if reconnErr := c.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection after protocol error failed: %w", reconnErr)
			}
			continue
		}

		// 检查认证错误
		if isAuthError(err) {
			// 认证错误也需要重新连接获取新token
			// refreshToken现在直接调用authenticate，有CAS保护
			if refreshErr := c.refreshToken(); refreshErr != nil {
				// 如果刷新失败，尝试完全重连
				if reconnErr := c.tryReconnect(); reconnErr != nil {
					return fmt.Errorf("reconnection after auth error failed: %w", reconnErr)
				}
			}
			continue
		}

		// Check if reconnection is needed for network issues
		if isConnectionError(err) {
			// Try to reconnect
			if reconnErr := c.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnErr)
			}
			continue
		}

		// Other errors, use exponential backoff retry
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

	// Check gRPC status code - according to documentation line 230: UNAUTHENTICATED status code
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Unauthenticated {
			return true
		}
	}

	// Check error message content
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "invalid token") ||
		   strings.Contains(errMsg, "token expired") ||
		   strings.Contains(errMsg, "authentication failed") ||
		   strings.Contains(errMsg, "unauthorized")
}

// isProtocolError checks if it's a protocol mismatch error (e.g., HTTP response to gRPC request)
// 当token过期时，服务器可能返回HTTP响应而不是gRPC，导致协议不匹配错误
func isProtocolError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "malformed header") ||
		   strings.Contains(errMsg, "unexpected http") ||
		   strings.Contains(errMsg, "missing http content-type") ||
		   strings.Contains(errMsg, "unimplemented") ||
		   strings.Contains(errMsg, "404 (not found)")
}

// isConnectionError checks if it's a connection error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status code - connection-related error states
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted:
			return true
		}
	}

	// Check error message content - connection-related error messages
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


// Remove type aliases: explicitly use *Client and *Transaction pointer types
// This avoids user confusion caused by type aliases hiding pointer semantics

// Connect convenience function to directly connect to TypeDB server
// Follows standard library conventions, uses Options pattern
func Connect(address, username, password string) (*Client, error) {
	return NewClient(&Options{
		Address:  address,
		Username: username,
		Password: password,
	})
}

// ConnectDefault connects to specified address using default credentials
func ConnectDefault(address string) (*Client, error) {
	return Connect(address, "admin", "password")
}

// ConnectLocal connects to local TypeDB server (127.0.0.1:1729)
func ConnectLocal() (*Client, error) {
	return Connect("127.0.0.1:1729", "admin", "password")
}