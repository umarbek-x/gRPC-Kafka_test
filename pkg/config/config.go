// Package config reads typed configuration from environment variables.
//
// Twelve-Factor App rule: config that varies between environments (DSNs,
// broker addresses, pool sizes) lives in the environment, not in code.
// Each service declares its own Config struct and loads it with these helpers;
// invalid values fail fast at startup instead of at 3am in production.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Loader accumulates errors so all bad variables are reported at once.
type Loader struct {
	errs []string
}

func (l *Loader) String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Required returns the variable or records an error when it is missing.
func (l *Loader) Required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		l.errs = append(l.errs, key+" is required")
	}
	return v
}

func (l *Loader) Int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: invalid int %q", key, v))
		return def
	}
	return n
}

func (l *Loader) Float(key string, def float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: invalid float %q", key, v))
		return def
	}
	return f
}

func (l *Loader) Duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: invalid duration %q (e.g. 500ms, 2s)", key, v))
		return def
	}
	return d
}

// Strings parses a comma-separated list.
func (l *Loader) Strings(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Err returns all accumulated errors, or nil.
func (l *Loader) Err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %s", strings.Join(l.errs, "; "))
}
