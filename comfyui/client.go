package comfyui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const defaultTimeout = 30 * time.Second

type Client struct {
	host       string
	httpClient *http.Client
	clientID   string
}

func NewClient(host string) *Client {
	return &Client{
		host: host,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		clientID: uuid.New().String(),
	}
}

func (c *Client) ClientID() string {
	return c.clientID
}

func (c *Client) Host() string {
	return c.host
}

type PromptResponse struct {
	PromptID   string          `json:"prompt_id"`
	Number     int             `json:"number"`
	NodeErrors map[string]any  `json:"node_errors"`
}

type QueueRemainingResponse struct {
	QueueRemaining int `json:"queue_remaining"`
	QueueSize      int `json:"queue_size"`
}

func (c *Client) SubmitPrompt(workflow map[string]any) (*PromptResponse, error) {
	body := map[string]any{
		"client_id": c.clientID,
		"prompt":    workflow,
	}

	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal prompt: %w", err)
	}

	url := fmt.Sprintf("%s/prompt", c.host)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("post prompt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result PromptResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode prompt response: %w", err)
	}

	return &result, nil
}

func (c *Client) SubmitPromptWithExtra(workflow map[string]any, extraData map[string]any) (*PromptResponse, error) {
	body := map[string]any{
		"client_id": c.clientID,
		"prompt":    workflow,
	}
	for k, v := range extraData {
		body[k] = v
	}

	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal prompt: %w", err)
	}

	url := fmt.Sprintf("%s/prompt", c.host)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("post prompt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result PromptResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode prompt response: %w", err)
	}

	return &result, nil
}

func (c *Client) GetHistory(promptID string) (map[string]any, error) {
	url := fmt.Sprintf("%s/history/%s", c.host, promptID)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get history: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode history: %w", err)
	}

	return result, nil
}

func (c *Client) GetQueueRemaining() (*QueueRemainingResponse, error) {
	url := fmt.Sprintf("%s/prompt/queue_remaining", c.host)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get queue remaining: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result QueueRemainingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode queue remaining: %w", err)
	}

	return &result, nil
}

type InterruptResponse struct{}

func (c *Client) Interrupt() (*InterruptResponse, error) {
	url := fmt.Sprintf("%s/interrupt", c.host)
	resp, err := c.httpClient.Post(url, "application/json", nil)
	if err != nil {
		return nil, fmt.Errorf("post interrupt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return &InterruptResponse{}, nil
}

func (c *Client) GetImageURL(filename, subfolder, folderType string) string {
	url := fmt.Sprintf("%s/view?filename=%s&subfolder=%s&type=%s", c.host, filename, subfolder, folderType)
	return url
}
