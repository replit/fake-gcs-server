// Copyright 2023 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFlatObjectNamespace(t *testing.T) {
	tests := map[string][]string{
		"manifest first": {"repl-id", "repl-id/offset/block-id"},
		"block first":    {"repl-id/offset/block-id", "repl-id"},
	}

	for name, objectNames := range tests {
		t.Run(name, func(t *testing.T) {
			storage, err := NewStorageFS(nil, t.TempDir())
			noError(t, err)

			for _, objectName := range objectNames {
				content := []byte(objectName)
				created, err := storage.CreateObject(Object{
					ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: objectName},
					Content:     content,
				}.StreamingObject(), NoConditions{})
				noError(t, err)
				created.Close()

				stored, err := storage.GetObject("bucket", objectName)
				noError(t, err)
				got, err := io.ReadAll(stored.Content)
				noError(t, err)
				stored.Close()
				if diff := cmp.Diff(content, got); diff != "" {
					t.Fatalf("object content differs (-want +got):\n%s", diff)
				}
			}

			objects, err := storage.ListObjects("bucket", "", false)
			noError(t, err)
			got := make([]string, 0, len(objects))
			for _, object := range objects {
				got = append(got, object.Name)
			}
			sort.Strings(got)
			want := append([]string(nil), objectNames...)
			sort.Strings(want)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("object names differ (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetAttributes(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	testBucket := filepath.Join(tempDir, "some-bucket")
	bucketAttrs := BucketAttrs{
		DefaultEventBasedHold: false,
		VersioningEnabled:     true,
	}
	data, _ := json.Marshal(bucketAttrs)
	err := os.WriteFile(testBucket+bucketMetadataSuffix, data, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	notABucket := filepath.Join(tempDir, "not-a-bucket")
	err = os.WriteFile(notABucket+bucketMetadataSuffix, []byte("this is not valid json"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		inputPath     string
		expectedAttrs BucketAttrs
		expectErr     bool
	}{
		{
			name:      "file not found",
			inputPath: filepath.Join(tempDir, "unknown-bucket"),
		},
		{
			name:          "existing bucket",
			inputPath:     testBucket,
			expectedAttrs: bucketAttrs,
		},
		{
			name:      "invalid file",
			inputPath: notABucket,
			expectErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			attrs, err := getBucketAttributes(test.inputPath)
			if test.expectErr && err == nil {
				t.Fatal("expected error, but got <nil>")
			}

			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(attrs, test.expectedAttrs); diff != "" {
				t.Errorf("incorrect attributes returned\nwant: %#v\ngot:  %#v\ndiff: %s", test.expectedAttrs, attrs, diff)
			}
		})
	}
}
