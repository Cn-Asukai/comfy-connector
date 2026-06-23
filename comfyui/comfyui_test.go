package comfyui_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cn-asukai/comfy-connector/comfyui"
)

func TestNewClient(t *testing.T) {
	c := comfyui.NewClient("http://localhost:8188")

	assert.Equal(t, "http://localhost:8188", c.Host())
	assert.NotEmpty(t, c.ClientID())
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, c.ClientID())
}

func TestClientID(t *testing.T) {
	c1 := comfyui.NewClient("http://a.com")
	c2 := comfyui.NewClient("http://b.com")

	assert.NotEmpty(t, c1.ClientID())
	assert.NotEmpty(t, c2.ClientID())
	assert.NotEqual(t, c1.ClientID(), c2.ClientID())
}

func TestHost(t *testing.T) {
	c := comfyui.NewClient("http://example.com:1234")
	assert.Equal(t, "http://example.com:1234", c.Host())
}

func TestSubmitPrompt_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/prompt", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var body map[string]any
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		assert.NotEmpty(t, body["client_id"])
		assert.NotNil(t, body["prompt"])

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prompt_id":   "prompt-123",
			"number":      42,
			"node_errors": map[string]any{},
		})
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	resp, err := c.SubmitPrompt(map[string]any{"key": "value"})
	require.NoError(t, err)
	assert.Equal(t, "prompt-123", resp.PromptID)
	assert.Equal(t, 42, resp.Number)
}

func TestSubmitPrompt_Non200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	_, err := c.SubmitPrompt(map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	assert.Contains(t, err.Error(), "server error")
}

func TestSubmitPrompt_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	_, err := c.SubmitPrompt(map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode")
}

func TestSubmitPromptWithExtra(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)

		assert.Equal(t, "extra-value", body["extra_key"])
		assert.NotNil(t, body["prompt"])
		assert.NotEmpty(t, body["client_id"])

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prompt_id":   "prompt-456",
			"number":      1,
			"node_errors": map[string]any{},
		})
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	resp, err := c.SubmitPromptWithExtra(map[string]any{"k": "v"}, map[string]any{"extra_key": "extra-value"})
	require.NoError(t, err)
	assert.Equal(t, "prompt-456", resp.PromptID)
}

func TestGetHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/history/")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"prompt-123": map[string]any{"status": "completed"},
		})
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	result, err := c.GetHistory("prompt-123")
	require.NoError(t, err)
	assert.Contains(t, result, "prompt-123")
}

func TestGetHistory_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	_, err := c.GetHistory("prompt-123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestGetQueueRemaining(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/prompt/queue_remaining", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"queue_remaining": 5,
			"queue_size":      10,
		})
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	resp, err := c.GetQueueRemaining()
	require.NoError(t, err)
	assert.Equal(t, 5, resp.QueueRemaining)
	assert.Equal(t, 10, resp.QueueSize)
}

func TestInterrupt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/interrupt", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	resp, err := c.Interrupt()
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestInterrupt_Non200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("unavailable"))
	}))
	defer server.Close()

	c := comfyui.NewClient(server.URL)
	_, err := c.Interrupt()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "503")
}

func TestGetImageURL(t *testing.T) {
	c := comfyui.NewClient("http://localhost:8188")
	url := c.GetImageURL("test.png", "sub", "output")

	assert.Contains(t, url, "filename=test.png")
	assert.Contains(t, url, "subfolder=sub")
	assert.Contains(t, url, "type=output")
	assert.True(t, strings.HasPrefix(url, "http://localhost:8188/view"))
}

func TestConnectWS_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		conn.WriteMessage(websocket.TextMessage, msg)
	}))
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)
	c := comfyui.NewClient(wsURL)

	conn, err := c.ConnectWS(context.Background())
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
}

func TestConnectWS_Failure(t *testing.T) {
	c := comfyui.NewClient("http://127.0.0.1:19999")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.ConnectWS(ctx)
	require.Error(t, err)
}

func TestWSConnection_ReadMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"status","data":{"sid":"test-sid"}}`))
		_, _, err = conn.ReadMessage()
	}))
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)
	c := comfyui.NewClient(wsURL)

	conn, err := c.ConnectWS(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	msg, err := conn.ReadMessage(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "status", msg.Type)
	assert.JSONEq(t, `{"sid":"test-sid"}`, string(msg.Data))
}

func TestWSConnection_ReadMessage_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.WriteMessage(websocket.TextMessage, []byte(`not json`))
		_, _, err = conn.ReadMessage()
	}))
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)
	c := comfyui.NewClient(wsURL)

	conn, err := c.ConnectWS(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ReadMessage(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestWSConnection_ReadMessageRaw(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.WriteMessage(websocket.TextMessage, []byte(`raw data`))
		_, _, err = conn.ReadMessage()
	}))
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1)
	c := comfyui.NewClient(wsURL)

	conn, err := c.ConnectWS(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	msgType, data, err := conn.ReadMessageRaw()
	require.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, msgType)
	assert.Equal(t, []byte(`raw data`), data)
}
