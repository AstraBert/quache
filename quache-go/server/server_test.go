package server

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/AstraBert/quache/quache-go/core"
	"github.com/stretchr/testify/assert"
)

func TestPostRequesSuccess(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	reqBodyJson := SetRequest{Value: 1, Key: "hello", Ttl: nil}
	reqBody, err := json.Marshal(reqBodyJson)
	if err != nil {
		t.Fatalf("An error occurred while marshaling: %s", err.Error())
	}
	request := httptest.NewRequest("POST", "/kv", bytes.NewReader(reqBody))
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 201, "Response code should be 201")
}

func TestPostRequestBadMethod(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	reqBodyJson := SetRequest{Value: 1, Key: "hello", Ttl: nil}
	reqBody, err := json.Marshal(reqBodyJson)
	if err != nil {
		t.Fatalf("An error occurred while marshaling: %s", err.Error())
	}
	request := httptest.NewRequest("POST", "/kv/hello", bytes.NewReader(reqBody))
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 405, "Response code should be 405 (method not allowed)")
}

func TestPostRequestBadRequest(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	reqBodyJson := map[string]any{"hello": "key", "1": "value", "none": "ttl"}
	reqBody, err := json.Marshal(reqBodyJson)
	if err != nil {
		t.Fatalf("An error occurred while marshaling: %s", err.Error())
	}
	request := httptest.NewRequest("POST", "/kv", bytes.NewReader(reqBody))
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 400, "Response code should be 400 (bad request)")
	assert.Contains(t, responseRecorder.Body.String(), "An error occurred while reading your request:")
}

func TestGetRequestSuccess(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	kvStore.Put("hello", 1, nil)
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/kv/hello", nil)
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 200, "Response code should be 200")
	var responseBody GetResponse
	err := json.Unmarshal(responseRecorder.Body.Bytes(), &responseBody)
	assert.Nil(t, err, "there should not be any error when unmarshaling the response body")
	assert.Equal(t, responseBody.Value, float64(1), "Value should be equal to 1")
}

func TestGetRequestBadMethod(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/kv", nil)
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 405, "Response code should be 405 (method not allowed)")
}

func TestGetRequestNotFound(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/kv/hello", nil)
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 404, "Response code should be 404 (not found)")
	assert.Contains(t, responseRecorder.Body.String(), "not found")
}

func TestDeleteRequestExistingKey(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	kvStore.Put("hello", 1, nil)
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest("DELETE", "/kv/hello", nil)
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 204, "Response code should be 204 (no content)")
	_, err := kvStore.Get("hello")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDeleteRequestNonExistingKey(t *testing.T) {
	kvStore := core.NewKVStore(5, ".quache-test/")
	handler := CreateServerMux(kvStore)
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest("DELETE", "/kv/hello", nil)
	handler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, 204, "Response code should be 204 (no content)")
}
