package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cn-asukai/comfy-connector/comfyui"
	"github.com/cn-asukai/comfy-connector/queue"
)

// 生图任务处理
type GenerateImageHandler struct {
	comfyuiCli *comfyui.Client
}

func NewGenerateImageHandler(comfyuiClient *comfyui.Client) *GenerateImageHandler {
	return &GenerateImageHandler{
		comfyuiCli: comfyuiClient,
	}
}

func (h *GenerateImageHandler) GetHandlerName() string {
	return "GenerateImage"
}

func (h *GenerateImageHandler) Execute(ctx context.Context, job *queue.Job) (string, error) {
	workflow := job.Payload.(map[string]any)
	result, err := h.comfyuiCli.SubmitPrompt(workflow)
	if err != nil {
		return "", err
	}
	if len(result.NodeErrors) > 0 {
		return "", fmt.Errorf("submit prompt error: %v", result.NodeErrors)
	}
	data, err := json.Marshal(map[string]any{
		"prompt_id": result.PromptID,
	})
	if err != nil {
		return "", err
	}

	return string(data), nil
}
