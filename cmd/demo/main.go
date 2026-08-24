package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	assets "go-breaker"
	"go-breaker/breaker"
	"go-breaker/middleware"
	"go-breaker/server"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	registry := breaker.NewRegistry()
	registerBreakers(registry)
	simulator := NewSimulator(registry)
	app := server.New(
		registry,
		server.WithAddr(*addr),
		server.WithEmbedFS(assets.Web, "web"),
	)
	registerDemoAPI(app, simulator)
	registerBusinessRoutes(app, registry, simulator)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("go-breaker is listening on %s", *addr)
	if err := app.Run(ctx); err != nil {
		log.Fatalf("server stopped with error: %v", err)
	}
}

func registerBreakers(registry *breaker.Registry) {
	registry.GetOrCreate(
		"getUser",
		breaker.WithErrorThreshold(0.5),
		breaker.WithMinRequests(5),
		breaker.WithSleepWindow(5*time.Second),
		breaker.WithResultEvents(true),
	)
	registry.GetOrCreate(
		"payOrder",
		breaker.WithErrorThreshold(0.5),
		breaker.WithMinRequests(5),
		breaker.WithSleepWindow(5*time.Second),
		breaker.WithMaxConcurrency(20),
		breaker.WithResultEvents(true),
	)
	registry.GetOrCreate(
		"searchGoods",
		breaker.WithErrorThreshold(0.6),
		breaker.WithMinRequests(8),
		breaker.WithSleepWindow(4*time.Second),
		breaker.WithResultEvents(true),
	)
}

func registerDemoAPI(app *server.Server, simulator *Simulator) {
	app.HandleFunc("/api/demo/simulate", simulator.SimulateHandler)
	app.HandleFunc("/api/demo/stop", simulator.StopHandler)
	app.HandleFunc("/api/demo/status", simulator.StatusHandler)
}

func registerBusinessRoutes(app *server.Server, registry *breaker.Registry, simulator *Simulator) {
	resources := []string{"getUser", "payOrder", "searchGoods"}
	for _, resource := range resources {
		resource := resource
		handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			value, err := simulator.Call(request.Context(), resource)
			if err != nil {
				writeDemoError(writer, http.StatusInternalServerError, err.Error())
				return
			}
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(writer).Encode(value)
		})
		guard := middleware.New(
			registry,
			middleware.WithResourceFunc(func(*http.Request) string { return resource }),
			middleware.WithDefaultBody([]byte(fmt.Sprintf("%s is temporarily degraded", resource))),
		)
		app.Handle("/demo/"+resource, guard.Wrap(handler))
	}
}
