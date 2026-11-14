// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectorLines maps a distribution component minor (v0.16N.x) to the core
// library minor (v1.7X.x) released alongside it. Collector core and the
// collector/contrib distributions are released in lockstep with different
// version numbers, so "same release line" cannot be read off the numbers.
//
// Extend this table deliberately when moving the whole distribution forward.
// If a dependency bump changes go.mod without the pins in builder-config.yaml
// (or the other way around) following, this test is what fails: nothing else
// notices, because OCB happily builds a distribution whose libraries come from
// two different release lines.
var collectorLines = map[string]string{
	"0.162": "1.68",
	"0.163": "1.69",
	"0.164": "1.70",
	"0.165": "1.71",
}

var (
	gomodPin    = regexp.MustCompile(`gomod:\s*"([^\s"]+)\s+v(\d+\.\d+\.\d+)"`)
	requireLine = regexp.MustCompile(`^\s*(go\.opentelemetry\.io/collector/[^\s]+)\s+v(\d+\.\d+\.\d+)`)
	builderPin  = regexp.MustCompile(`cmd/builder@v(\d+\.\d+\.\d+)`)
)

// The builder binary generates the distribution's go.mod, including its
// requirement on the collector service (otelcol), so a stale builder pins the
// whole distribution to an older service line no matter what the manifest says.
var builderPinFiles = []string{
	".github/workflows/ci.yaml",
	"examples/quickstart/Dockerfile",
}

// files that pin component versions and must all agree.
var pinFiles = []string{
	"builder-config.yaml",
	"examples/quickstart/manifest.yaml",
	"README.md",
}

func minorOf(version string) string {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return version
	}
	return parts[0] + "." + parts[1]
}

type pinnedModule struct {
	file    string
	module  string
	version string
}

// pinnedModules scans every place a component version is written down.
func pinnedModules(t *testing.T) []pinnedModule {
	t.Helper()
	var out []pinnedModule
	for _, name := range pinFiles {
		body, err := os.ReadFile(filepath.Join(".", name))
		require.NoErrorf(t, err, "reading %s", name)
		for _, m := range gomodPin.FindAllStringSubmatch(string(body), -1) {
			out = append(out, pinnedModule{file: name, module: m[1], version: m[2]})
		}
	}
	require.NotEmpty(t, out, "no gomod pins found; has the manifest format changed?")
	return out
}

func goModCoreVersion(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("go.mod")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(body), "\n") {
		if m := requireLine.FindStringSubmatch(line); len(m) > 0 &&
			m[1] == "go.opentelemetry.io/collector/component" {
			return m[2]
		}
	}
	t.Fatal("go.mod does not require go.opentelemetry.io/collector/component")
	return ""
}

// TestPinsUseOneCollectorLine is the alarm that was missing when a Dependabot
// bump moved the module to collector 0.165 while builder-config.yaml stayed on
// 0.162: the distribution then built, tested and released with libraries from
// two release lines.
func TestPinsUseOneCollectorLine(t *testing.T) {
	pins := pinnedModules(t)

	coreMinors := map[string][]pinnedModule{}
	distMinors := map[string][]pinnedModule{}
	contribMinors := map[string][]pinnedModule{}

	for _, p := range pins {
		switch {
		case strings.HasPrefix(p.module, "github.com/open-telemetry/"):
			contribMinors[minorOf(p.version)] = append(contribMinors[minorOf(p.version)], p)
		case strings.HasPrefix(p.module, "go.opentelemetry.io/collector/"):
			if strings.HasPrefix(p.version, "1.") {
				coreMinors[minorOf(p.version)] = append(coreMinors[minorOf(p.version)], p)
			} else {
				distMinors[minorOf(p.version)] = append(distMinors[minorOf(p.version)], p)
			}
		}
	}

	describe := func(m map[string][]pinnedModule) string {
		var lines []string
		for v, ps := range m {
			for _, p := range ps {
				lines = append(lines, fmt.Sprintf("%s pins v%s", p.file, p.version))
			}
			_ = v
		}
		sort.Strings(lines)
		return strings.Join(lines, ", ")
	}

	require.Len(t, coreMinors, 1, "collector core pins disagree: %s", describe(coreMinors))
	require.Len(t, distMinors, 1, "collector component pins disagree: %s", describe(distMinors))

	var coreMinor, distMinor, contribMinor string
	for v := range coreMinors {
		coreMinor = v
	}
	for v := range distMinors {
		distMinor = v
	}

	if len(contribMinors) > 1 {
		t.Fatalf("contrib pins disagree: %s", describe(contribMinors))
	}
	if len(contribMinors) == 1 {
		for v := range contribMinors {
			contribMinor = v
		}
		assert.Equalf(t, distMinor, contribMinor,
			"contrib is on %s but collector components are on %s; both must come from one release line",
			contribMinor, distMinor)
	}

	want, known := collectorLines[distMinor]
	require.Truef(t, known, "collector line v%s is not in collectorLines; add the core minor it was released with", distMinor)
	assert.Equalf(t, want, coreMinor,
		"distribution pins are on v%s (core v%s) but the module requires core v%s; move both lines together",
		distMinor, want, coreMinor)
}

// TestGoModMatchesPins is the same check one level deeper: the connector's
// own requirements, not just the files a human edits.
func TestGoModMatchesPins(t *testing.T) {
	pins := pinnedModules(t)
	coreModInGoMod := goModCoreVersion(t)

	var pinnedDistMinor string
	for _, p := range pins {
		if strings.HasPrefix(p.module, "go.opentelemetry.io/collector/") &&
			!strings.HasPrefix(p.version, "1.") {
			pinnedDistMinor = minorOf(p.version)
		}
	}
	require.NotEmpty(t, pinnedDistMinor, "no distribution component pins found")

	want, known := collectorLines[pinnedDistMinor]
	require.Truef(t, known, "collector line v%s is not in collectorLines", pinnedDistMinor)
	assert.Equalf(t, want, minorOf(coreModInGoMod),
		"builder-config.yaml pins the v%s line (core v%s) but go.mod requires core v%s; "+
			"a dependency bump moved one and not the other",
		pinnedDistMinor, want, coreModInGoMod)
}

// TestBuilderPinsMatchComponents keeps the OCB version used by CI and by the
// quickstart Dockerfile on the same release line as the components it builds.
// OCB writes the dist go.mod, so a stale builder drags otelcol back a line even
// when builder-config.yaml is correct.
func TestBuilderPinsMatchComponents(t *testing.T) {
	var pinnedDistMinor string
	for _, p := range pinnedModules(t) {
		if strings.HasPrefix(p.module, "go.opentelemetry.io/collector/") &&
			!strings.HasPrefix(p.version, "1.") {
			pinnedDistMinor = minorOf(p.version)
		}
	}
	require.NotEmpty(t, pinnedDistMinor, "no distribution component pins found")

	for _, name := range builderPinFiles {
		body, err := os.ReadFile(filepath.Join(".", name))
		require.NoErrorf(t, err, "reading %s", name)
		m := builderPin.FindStringSubmatch(string(body))
		require.NotNilf(t, m, "%s does not pin an OCB version (cmd/builder@vX.Y.Z)", name)
		assert.Equalf(t, pinnedDistMinor, minorOf(m[1]),
			"%s builds with OCB v%s but the components are pinned to v%s; OCB generates the dist go.mod, "+
				"so its own version has to follow the component line",
			name, m[1], pinnedDistMinor)
	}
}

// TestReleaseVersionIsConsistent keeps the version a release will be tagged
// with in step with every place that names it.
func TestReleaseVersionIsConsistent(t *testing.T) {
	body, err := os.ReadFile("builder-config.yaml")
	require.NoError(t, err)

	versionRE := regexp.MustCompile(`(?m)^\s+version:\s*(\d+\.\d+\.\d+)\s*$`)
	m := versionRE.FindStringSubmatch(string(body))
	require.NotNil(t, m, "builder-config.yaml has no dist.version")
	distVersion := m[1]

	for _, name := range pinFiles {
		body, err := os.ReadFile(filepath.Join(".", name))
		require.NoErrorf(t, err, "reading %s", name)
		if !strings.Contains(string(body), "logs-to-spans-otel") {
			continue
		}
		assert.Containsf(t, string(body), "logs-to-spans-otel v"+distVersion,
			"%s does not reference the connector at v%s (builder-config.yaml dist.version)", name, distVersion)
	}
}
