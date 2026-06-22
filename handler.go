package main

import (
	"context"
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

func (h *GenerateImageHandler) Execute(ctx context.Context, job *queue.Job) (fmt.Stringer, error) {
	return nil, nil
}
