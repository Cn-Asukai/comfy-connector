package main

import (
	"comfyui_connector/comfyui"
	"context"
	"fmt"
)

func main() {
	client := comfyui.NewClient("http://127.0.0.1:8000")
	fmt.Printf("Client connected to %s (client_id: %s)\n", client.Host(), client.ClientID())

	workflow := map[string]any{
		// TODO: define your ComfyUI workflow
	}

	ctx := context.Background()
	result, err := client.GenerateImage(ctx, workflow)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Prompt ID: %s\n", result.PromptID)
	for nodeID, images := range result.ImagesByNode {
		for _, img := range images {
			fmt.Printf("  Node %s: %s\n", nodeID, img.URL)
		}
	}
	for _, e := range result.Errors {
		fmt.Printf("  Error: %s\n", e)
	}
}
