// Copyright 2010 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package twitterstream implements the basic functionality for accessing the
// Twitter streaming APIs. See http://dev.twitter.com/pages/streaming_api for
// information on the Twitter streaming APIs.
//
// The following example shows how to handle dropped connections. If it's
// important for the application to see every tweet in the stream, then the
// application should backfill the stream using the Twitter search API after
// each connection attempt.
//
//  waitUntil := time.Now()
//  for {
//      // Rate limit connection attempts to once every 30 seconds.
//      if d := waitUntil.Sub(time.Now()); d > 0 {
//          time.Sleep(d)
//      }
//      waitUntil = time.Now().Add(30 * time.Second)
//
//      ts, err := twitterstream.Open(client, cred, url, params)
//      if err != nil {
//          log.Println("error opening stream: ", err)
//          continue
//      }
//
//      // Loop until stream has a permanent error.
//      for ts.Err() == nil {
//          var t MyTweet
//          if err := ts.UnmarshalNext(&t); err != nil {
//              log.Println("error reading tweet: ", err)
//              continue
//          }
//          process(&t)
//      }
//      ts.Close()
//  }
//
package twitterstream

import (
	"bufio"
	"encoding/json"
	"github.com/garyburd/go-oauth/oauth"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
)

// Stream manages the connection to a Twitter streaming endpoint.
type Stream struct {
	chunkRemaining int64
	chunkState     int
	body           io.ReadCloser
	r              *bufio.Reader
	err            error
}

// HTTPStatusError represents an HTTP error return from the Twitter streaming
// API endpoint.
type HTTPStatusError struct {
	// HTTP status code.
	StatusCode int

	// Response body.
	Message string
}

func (err HTTPStatusError) Error() string {
	return "twitterstream: status=" + strconv.Itoa(err.StatusCode) + " " + err.Message
}

var responseLineRegexp = regexp.MustCompile("^HTTP/[0-9.]+ ([0-9]+) ")

// Open opens a new stream.
func Open(oauthClient *oauth.Client, accessToken *oauth.Credentials, urlStr string, params url.Values) (*Stream, error) {
	ts := new(Stream)

	// Setup request body.
	pcopy := url.Values{}
	for key, values := range params {
		pcopy[key] = values
	}
	oauthClient.SignParam(accessToken, "POST", urlStr, pcopy)

	// send request
	resp, err := http.PostForm(urlStr, pcopy)
	if err != nil {
		return nil, ts.fatal(err)
	}

	ts.body = resp.Body

	// // Must connect in 60 seconds.
	// err = ts.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	// if err != nil {
	// 	return nil, ts.fatal(err)
	// }

	ts.r = bufio.NewReaderSize(resp.Body, 8192)

	if resp.StatusCode != 200 {
		p, _ := ioutil.ReadAll(ts.r)
		return nil, HTTPStatusError{resp.StatusCode, string(p)}
	}

	return ts, nil
}

func (ts *Stream) fatal(err error) error {
	if ts.body != nil {
		ts.body.Close()
	}
	if ts.err == nil {
		ts.err = err
	}
	return err
}

// Close releases the resources used by the stream.
func (ts *Stream) Close() error {
	if ts.err != nil {
		return ts.err
	}
	return ts.body.Close()
}

// Err returns a non-nil value if the stream has a permanent error.
func (ts *Stream) Err() error {
	return ts.err
}

// Next returns the next line from the stream. The returned slice is
// overwritten by the next call to Next.
func (ts *Stream) Next() ([]byte, error) {
	if ts.err != nil {
		return nil, ts.err
	}
	for {
		// // Twitter sends at least one ine of text every 30 seconds.
		// err := ts.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		// if err != nil {
		// 	return nil, ts.fatal(err)
		// }

		p, err := ts.r.ReadSlice('\r')
		if err != nil {
			return nil, ts.fatal(err)
		}

		if len(p) <= 2 {
			continue // ignore keepalive line
		}

		return p, nil
	}
	panic("should not get here")
}

// UnmarshalNext reads the next line of from the stream and decodes the line as
// JSON to data. This is a convenience function for streams with homogeneous
// entity types.
func (ts *Stream) UnmarshalNext(data interface{}) error {
	p, err := ts.Next()
	if err != nil {
		return err
	}
	return json.Unmarshal(p, data)
}
