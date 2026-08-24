package server

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go-breaker/breaker"
)

type Option func(*options)

type options struct {
	addr            string
	staticDir       string
	embedFS         *embed.FS
	embedPath       string
	metricsInterval time.Duration
}

type Server struct {
	registry *breaker.Registry
	options  options
	mux      *http.ServeMux
}

func defaultServerOptions() options {
	return options{addr: ":8080", metricsInterval: 5 * time.Second}
}

func WithAddr(addr string) Option {
	return func(o *options) {
		if addr != "" {
			o.addr = addr
		}
	}
}

func WithStaticDir(directory string) Option {
	return func(o *options) { o.staticDir = directory }
}

func WithEmbedFS(files embed.FS, path string) Option {
	return func(o *options) {
		o.embedFS = &files
		o.embedPath = path
	}
}

func WithMetricsInterval(duration time.Duration) Option {
	return func(o *options) {
		if duration > 0 {
			o.metricsInterval = duration
		}
	}
}

func New(registry *breaker.Registry, opts ...Option) *Server {
	if registry == nil {
		registry = breaker.NewRegistry()
	}
	settings := defaultServerOptions()
	for _, option := range opts {
		if option != nil {
			option(&settings)
		}
	}
	server := &Server{
		registry: registry,
		options:  settings,
		mux:      http.NewServeMux(),
	}
	server.registerAPI()
	server.registerStatic()
	return server
}

func (s *Server) Handler() http.Handler {
	return securityHeaders(s.mux)
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func (s *Server) HandleFunc(pattern string, handler http.HandlerFunc) {
	s.mux.HandleFunc(pattern, handler)
}

func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{
		Addr:              s.options.addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()
	ticker := time.NewTicker(s.options.metricsInterval)
	defer ticker.Stop()
	for {
		select {
		case err := <-errCh:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return httpServer.Shutdown(shutdownCtx)
		case now := <-ticker.C:
			s.registry.PublishMetricSnapshots(now.UTC())
		}
	}
}

func (s *Server) registerStatic() {
	var files http.FileSystem
	if s.options.embedFS != nil {
		sub, err := fs.Sub(*s.options.embedFS, s.options.embedPath)
		if err == nil {
			files = http.FS(sub)
		}
	}
	if files == nil && s.options.staticDir != "" {
		if info, err := os.Stat(s.options.staticDir); err == nil && info.IsDir() {
			absolute, _ := filepath.Abs(s.options.staticDir)
			files = http.Dir(absolute)
		}
	}
	if files == nil {
		s.mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/" {
				http.NotFound(writer, request)
				return
			}
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = writer.Write([]byte("go-breaker API is running"))
		})
		return
	}
	s.mux.Handle("/", http.FileServer(files))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(writer, request)
	})
}
