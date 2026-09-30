// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

// TestConfigFromTestdata loads every sample in testdata/config.yaml and checks
// it both unmarshals and passes Validate. It keeps the shipped samples honest
// as the option surface changes.
func TestConfigFromTestdata(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	for _, name := range []string{"default", "attributes_only", "all_options"} {
		t.Run(name, func(t *testing.T) {
			sub, err := cm.Sub(name)
			require.NoError(t, err)

			cfg := createDefaultConfig()
			require.NoError(t, sub.Unmarshal(cfg))
			require.NoError(t, cfg.Validate())
		})
	}
}
