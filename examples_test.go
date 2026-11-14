// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shippedCollectorConfigs lists every collector configuration that ships with
// the repository. Demo configs get copied into real deployments, so they are
// part of the security surface (issues #40, #41).
var shippedCollectorConfigs = []string{
	"collector.yaml",
	filepath.Join("examples", "quickstart", "collector.yaml"),
}

func readShipped(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "shipped file %s must exist", path)
	return string(data)
}

// TestShippedConfigsDoNotDumpDetailedTelemetry covers #40: the debug exporter at
// verbosity: detailed writes every resource and span attribute - group keys,
// user and session IDs, log bodies - to collector stdout.
func TestShippedConfigsDoNotDumpDetailedTelemetry(t *testing.T) {
	for _, cfg := range shippedCollectorConfigs {
		body := readShipped(t, cfg)
		assert.NotContains(t, body, "verbosity: detailed",
			"%s must not ship the debug exporter at detailed verbosity", cfg)
		assert.Contains(t, body, "verbosity: normal",
			"%s should show the copy-paste-safe debug verbosity", cfg)
	}
}

// TestShippedConfigsTreatInsecureTLSAsLoopbackOnly covers #41: insecure: true is
// only defensible against 127.0.0.1, and the shipped configs must say so where
// an operator will read it.
func TestShippedConfigsTreatInsecureTLSAsLoopbackOnly(t *testing.T) {
	insecure := regexp.MustCompile(`(?m)^\s*insecure:\s*true\s*$`)
	endpoint := regexp.MustCompile(`(?m)^\s*endpoint:\s*(\S+)\s*$`)

	for _, cfg := range shippedCollectorConfigs {
		body := readShipped(t, cfg)
		if !insecure.MatchString(body) {
			continue
		}
		assert.Contains(t, strings.ToLower(body), "loopback",
			"%s enables insecure TLS, so it must explain that this is loopback-only", cfg)

		for _, match := range endpoint.FindAllStringSubmatch(body, -1) {
			value := match[1]
			if strings.Contains(value, "${env:") {
				continue
			}
			assert.True(t,
				strings.Contains(value, "127.0.0.1") || strings.Contains(value, "localhost"),
				"%s sends to %q with insecure TLS; only a loopback endpoint may do that", cfg, value)
		}
	}
}

// TestQuickstartDocumentsTLSOverride covers the second half of #41: the
// documented cross-host override must not teach a plaintext hop.
func TestQuickstartDocumentsTLSOverride(t *testing.T) {
	body := readShipped(t, filepath.Join("examples", "quickstart", "collector.yaml"))
	assert.NotContains(t, body, "http://host.docker.internal",
		"the documented cross-host override must use https, not plaintext")
	assert.Contains(t, body, "https://host.docker.internal",
		"the quickstart must show the https form of the override")
}

// TestQuickstartDockerfileIsReproducible covers #43: mutable base images make
// the same commit produce different images.
func TestQuickstartDockerfileIsReproducible(t *testing.T) {
	body := readShipped(t, filepath.Join("examples", "quickstart", "Dockerfile"))

	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(line), "FROM ") {
			continue
		}
		assert.Contains(t, line, "@sha256:",
			"every base image must be pinned by digest, got %q", line)
	}
	assert.Contains(t, body, "GOTOOLCHAIN=local",
		"the builder must not download a different Go toolchain mid-build")
}

// TestQuickstartBuildsTheWorkingTree covers the correctness half of #43: the
// example must not silently build the last published module instead of the code
// in this checkout.
func TestQuickstartBuildsTheWorkingTree(t *testing.T) {
	body := readShipped(t, filepath.Join("examples", "quickstart", "manifest.yaml"))
	assert.Contains(t, body, "replaces:",
		"the quickstart manifest must replace the published module with this tree")
	assert.Contains(t, body, "=> ../",
		"the replace path is relative to output_path, so ../ is the repo root")
}

// TestRepositoryHygiene covers #45: the internal planning document must not be
// back, and the sample log must live with the examples.
func TestRepositoryHygiene(t *testing.T) {
	_, err := os.Stat("fix-order.txt")
	assert.True(t, os.IsNotExist(err), "the internal planning document must not be committed")

	_, err = os.Stat(filepath.Join("examples", "plaintext", "input.log"))
	assert.NoError(t, err, "the sample log belongs under examples/")

	body := readShipped(t, "collector.yaml")
	assert.Contains(t, body, "examples/plaintext/input.log",
		"collector.yaml must point at the sample log's new location")
}

// TestSecurityPolicyExists covers #36: a published component needs a private
// disclosure route, not a public issue template.
func TestSecurityPolicyExists(t *testing.T) {
	body := readShipped(t, "SECURITY.md")
	assert.Contains(t, body, "/security/advisories/new",
		"SECURITY.md must point at GitHub's private advisory route")

	readme := readShipped(t, "README.md")
	assert.Contains(t, readme, "SECURITY.md", "the README must link the security policy")
}

// TestCIWorkflowIsLeastPrivilege covers #37 and #46: the workflow must not run
// unreviewed pull-request code with a write token, actions must be pinned by
// SHA, and dependency/vulnerability scanning must exist.
func TestCIWorkflowIsLeastPrivilege(t *testing.T) {
	body := readShipped(t, filepath.Join(".github", "workflows", "ci.yaml"))

	assert.Contains(t, body, "permissions:\n  contents: read",
		"CI must declare least-privilege permissions")

	uses := regexp.MustCompile(`(?m)^\s*-?\s*uses:\s*(\S+)`)
	for _, match := range uses.FindAllStringSubmatch(body, -1) {
		ref := match[1]
		if !strings.Contains(ref, "@") {
			continue
		}
		pin := ref[strings.LastIndex(ref, "@")+1:]
		assert.Regexp(t, `^[0-9a-f]{40}$`, pin,
			"action %q must be pinned to a commit SHA (with the version in a comment)", ref)
	}

	assert.Contains(t, body, "govulncheck", "CI must run a vulnerability scan")
}

// TestDependabotCoversSupplyChainSurfaces covers #46: pins are only useful if
// something refreshes them.
func TestDependabotCoversSupplyChainSurfaces(t *testing.T) {
	body := readShipped(t, filepath.Join(".github", "dependabot.yml"))
	for _, ecosystem := range []string{"gomod", "github-actions", "docker"} {
		assert.Contains(t, body, "package-ecosystem: "+ecosystem,
			"Dependabot must watch %s", ecosystem)
	}
}
