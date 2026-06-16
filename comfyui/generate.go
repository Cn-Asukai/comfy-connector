package comfyui

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type ImageOutput struct {
	Filename  string `json:"filename"`
	Subfolder string `json:"subfolder"`
	Type      string `json:"type"`
	URL       string `json:"url"`
}

type GenerationResult struct {
	PromptID     string
	ImagesByNode map[string][]ImageOutput
	Errors       []string
}

func (c *Client) GenerateImage(ctx context.Context, workflow map[string]any) (*GenerationResult, error) {
	submitResp, err := c.SubmitPrompt(workflow)
	if err != nil {
		return nil, fmt.Errorf("submit prompt: %w", err)
	}

	promptID := submitResp.PromptID
	log.Printf("[comfyui] prompt submitted: %s", promptID)

	ws, err := c.ConnectWS(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect ws: %w", err)
	}
	defer ws.Close()

	result := &GenerationResult{
		PromptID:     promptID,
		ImagesByNode: make(map[string][]ImageOutput),
	}

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pingTicker.C:
			if err := ws.conn.WriteMessage(1, []byte("ping")); err != nil {
				return nil, fmt.Errorf("write ping: %w", err)
			}
		default:
		}

		msg, err := ws.ReadMessage(ctx)
		if err != nil {
			return result, fmt.Errorf("read ws: %w", err)
		}

		switch msg.Type {
		case "executed":
			var data WSExecutedData
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				log.Printf("[comfyui] unmarshal executed: %v", err)
				continue
			}

			images := make([]ImageOutput, 0, len(data.Output.Images))
			for _, img := range data.Output.Images {
				images = append(images, ImageOutput{
					Filename:  img.Filename,
					Subfolder: img.Subfolder,
					Type:      img.Type,
					URL:       c.GetImageURL(img.Filename, img.Subfolder, img.Type),
				})
			}
			if len(images) > 0 {
				result.ImagesByNode[data.Node] = images
				log.Printf("[comfyui] node %s executed, %d images", data.Node, len(images))
			}

		case "execution_error":
			var data WSExecutionErrorData
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				log.Printf("[comfyui] unmarshal execution_error: %v", err)
				continue
			}
			errMsg := fmt.Sprintf("execution error [%s]: %s", data.ExceptionType, data.ExceptionMessage)
			result.Errors = append(result.Errors, errMsg)
			log.Printf("[comfyui] %s", errMsg)
			return result, fmt.Errorf("execution failed: %s", errMsg)

		case "execution_start":
			var data struct {
				PromptID string `json:"prompt_id"`
			}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				log.Printf("[comfyui] unmarshal execution_start: %v", err)
				continue
			}
			if data.PromptID == promptID {
				log.Printf("[comfyui] execution started for %s", promptID)
			}
		}
	}
}
