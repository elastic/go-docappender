// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package docappender_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/go-docappender/docappendertest"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/elastic/go-docappender"
)

func TestAppenderIntegration(t *testing.T) {
	switch strings.ToLower(os.Getenv("INTEGRATION_TESTS")) {
	case "1", "true":
	default:
		t.Skip("Skipping integration test, export INTEGRATION_TESTS=1 to run")
	}

	config := elasticsearch.Config{}
	config.Username = "admin"
	config.Password = "changeme"
	client, err := elasticsearch.NewClient(config)
	require.NoError(t, err)
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Second})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	index := "logs-generic-testing"
	deleteIndex := func() {
		resp, err := esapi.IndicesDeleteDataStreamRequest{Name: []string{index}}.Do(context.Background(), client)
		require.NoError(t, err)
		defer resp.Body.Close()
	}
	deleteIndex()
	defer deleteIndex()

	const N = 100
	for i := 0; i < N; i++ {
		encoded, err := json.Marshal(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)})
		require.NoError(t, err)
		err = indexer.Add(context.Background(), bytes.NewReader(encoded), 1)
		require.NoError(t, err)
	}

	// Closing the indexer flushes enqueued events.
	err = indexer.Close(context.Background())
	require.NoError(t, err)

	// Check that docs are indexed.
	resp, err := esapi.IndicesRefreshRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	resp.Body.Close()

	var result struct {
		Count int
	}
	resp, err = esapi.CountRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, N, result.Count)
}
