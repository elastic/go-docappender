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

// Package esapi contains a stripped down version of https://github.com/elastic/go-elasticsearch/tree/main/esapi
// which exists to maintain compatibility with v7 and v8 clients for go-docappender usage.
package esapi

import (
	"net/http"
	"strconv"
	"time"
)

// Transport defines the interface for an API client.
type Transport interface {
	Perform(*http.Request) (*http.Response, error)
}

// formatDuration converts duration to a string in the format
// accepted by Elasticsearch.
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return strconv.FormatInt(int64(d), 10) + "nanos"
	}
	return strconv.FormatInt(int64(d)/int64(time.Millisecond), 10) + "ms"
}
