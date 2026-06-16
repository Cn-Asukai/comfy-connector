package comfyui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/gorilla/websocket"
)

type WSMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type WSStatusData struct {
	Status *struct {
		ExecInfo *struct {
			QueueRemaining int `json:"queue_remaining"`
		} `json:"exec_info"`
	} `json:"status"`
	SID string `json:"sid"`
}

type WSExecutingData struct {
	Node       string `json:"node"`
	PromptID   string `json:"prompt_id"`
	DisplayNode string `json:"display_node"`
}

type WSExecutedData struct {
	Node     string `json:"node"`
	PromptID string `json:"prompt_id"`
	Output   struct {
		Images []struct {
			Filename  string `json:"filename"`
			Subfolder string `json:"subfolder"`
			Type      string `json:"type"`
		} `json:"images"`
	} `json:"output"`
}

type WSExecutionErrorData struct {
	PromptID         string `json:"prompt_id"`
	ExceptionMessage string `json:"exception_message"`
	ExceptionType    string `json:"exception_type"`
	Traceback        string `json:"traceback"`
}

type WSConnection struct {
	conn     *websocket.Conn
	clientID string
	host     string
}

func (c *Client) ConnectWS(ctx context.Context) (*WSConnection, error) {
	u, err := url.Parse(c.host)
	if err != nil {
		return nil, fmt.Errorf("parse host: %w", err)
	}

	var wsScheme string
	if u.Scheme == "https" {
		wsScheme = "wss"
	} else {
		wsScheme = "ws"
	}

	wsURL := fmt.Sprintf("%s://%s/ws?clientId=%s", wsScheme, u.Host, c.clientID)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial ws: %w", err)
	}

	return &WSConnection{
		conn:     conn,
		clientID: c.clientID,
		host:     c.host,
	}, nil
}

func (w *WSConnection) ReadMessage(ctx context.Context) (*WSMessage, error) {
	_, raw, err := w.conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read ws message: %w", err)
	}

	var msg WSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal ws message: %w", err)
	}

	return &msg, nil
}

func (w *WSConnection) ReadMessageRaw() (int, []byte, error) {
	return w.conn.ReadMessage()
}

func (w *WSConnection) Close() error {
	return w.conn.Close()
}
