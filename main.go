package main

import (
	"fmt"
	"log"
	"os"

	"github.com/dotenv-org/godotenvvault"
)

func main() {
	err := godotenvvault.Load()
	if err != nil {
		log.Println("Error loading .env file:", err)
	}

	secretName := fmt.Sprintf("%s/imaginereplay", os.Getenv("ENVIRONMENT"))

	err = processJobs(secretName)
	if err != nil {
		log.Println("Erro ao processar jobs:", err)
	}
}
