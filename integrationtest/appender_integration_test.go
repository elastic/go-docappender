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

package integrationtest

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

	"github.com/elastic/go-docappender/v2"
	"github.com/elastic/go-docappender/v2/docappendertest"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	esapi7 "github.com/elastic/go-elasticsearch/v7/esapi"
	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	esapi8 "github.com/elastic/go-elasticsearch/v8/esapi"
)

const N = 100

func sendEvents(t *testing.T, indexer *docappender.Appender, index string) {
	for i := 0; i < N; i++ {
		encoded, err := json.Marshal(map[string]any{"@timestamp": time.Now().Format(docappendertest.TimestampFormat)})
		require.NoError(t, err)
		err = indexer.Add(context.Background(), index, bytes.NewReader(encoded))
		require.NoError(t, err)
	}

	// Closing the indexer flushes enqueued events.
	err := indexer.Close(context.Background())
	require.NoError(t, err)
}

func TestAppenderIntegrationV8(t *testing.T) {
	switch strings.ToLower(os.Getenv("INTEGRATION_TESTS")) {
	case "1", "true":
	default:
		t.Skip("Skipping integration test, export INTEGRATION_TESTS=1 to run")
	}

	const index = "logs-generic-testing.v8"

	config := elasticsearch8.Config{}
	config.Username = "admin"
	config.Password = "changeme"
	client, err := elasticsearch8.NewClient(config)
	require.NoError(t, err)
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Second})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	deleteIndex := func() {
		resp, err := esapi8.IndicesDeleteDataStreamRequest{Name: []string{index}}.Do(context.Background(), client)
		require.NoError(t, err)
		defer resp.Body.Close()
	}
	deleteIndex()
	defer deleteIndex()

	sendEvents(t, indexer, index)

	// Check that docs are indexed.
	resp, err := esapi8.IndicesRefreshRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	resp.Body.Close()

	var result struct {
		Count int
	}
	resp, err = esapi8.CountRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, N, result.Count)
}

func TestAppenderIntegrationV7(t *testing.T) {
	switch strings.ToLower(os.Getenv("INTEGRATION_TESTS")) {
	case "1", "true":
	default:
		t.Skip("Skipping integration test, export INTEGRATION_TESTS=1 to run")
	}

	const index = "logs-generic-testing.v7"

	config := elasticsearch7.Config{}
	config.Username = "admin"
	config.Password = "changeme"
	client, err := elasticsearch7.NewClient(config)
	require.NoError(t, err)
	indexer, err := docappender.New(client, docappender.Config{FlushInterval: time.Second})
	require.NoError(t, err)
	defer indexer.Close(context.Background())

	deleteIndex := func() {
		resp, err := esapi7.IndicesDeleteDataStreamRequest{Name: []string{index}}.Do(context.Background(), client)
		require.NoError(t, err)
		defer resp.Body.Close()
	}
	deleteIndex()
	defer deleteIndex()

	sendEvents(t, indexer, index)

	// Check that docs are indexed.
	resp, err := esapi7.IndicesRefreshRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	resp.Body.Close()

	var result struct {
		Count int
	}
	resp, err = esapi7.CountRequest{Index: []string{index}}.Do(context.Background(), client)
	require.NoError(t, err)
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, N, result.Count)
}
