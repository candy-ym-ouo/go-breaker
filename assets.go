package assets

import "embed"

// Web contains the dashboard assets used by the demo binary.
//
//go:embed web/*
var Web embed.FS
