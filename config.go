package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/bytedance/sonic"
	"google.golang.org/api/option"
)

// GetBigQueryClient conecta ao AWS Secrets Manager para recuperar as credenciais e cria um cliente BigQuery.
func GetBigQueryClient(secretName string) (*bigquery.Client, error) {
	projectID, err := GetSecret(secretName, "bigquery_project_id")
	if err != nil {
		log.Println("Erro ao recuperar 'bigquery_project_id' do AWS Secrets Manager")
		return nil, err
	}

	bigQueryPemStr, err := GetSecret(secretName, "bigquery_project_secret_pem")
	if err != nil {
		log.Println("Erro ao recuperar 'bigquery_project_secret_pem' do AWS Secrets Manager")
		return nil, err
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(bigQueryPemStr)))
	if err != nil {
		log.Println("Erro ao inicializar o cliente BigQuery")
		return nil, err
	}

	return client, nil
}

// GetSecret recupera um valor específico do AWS Secrets Manager.
func GetSecret(secretName, secretKey string) (string, error) {
	svc, err := CreateSecretsManagerSession()
	if err != nil {
		log.Println("Erro ao criar sessão para o AWS Secrets Manager")
		return "", err
	}

	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		log.Println("Erro ao recuperar valor do segredo no AWS Secrets Manager")
		return "", err
	}

	var secretString string
	var secretData map[string]interface{}

	if result.SecretString != nil {
		secretString = *result.SecretString
	} else {
		decodedBinarySecretBytes := make([]byte, base64.StdEncoding.DecodedLen(len(result.SecretBinary)))
		length, err := base64.StdEncoding.Decode(decodedBinarySecretBytes, result.SecretBinary)
		if err != nil {
			log.Println("Erro ao decodificar segredo binário do AWS Secrets Manager")
			return "", err
		}
		secretString = string(decodedBinarySecretBytes[:length])
	}

	if err := sonic.Unmarshal([]byte(secretString), &secretData); err != nil {
		log.Println("Erro ao deserializar dados do segredo do AWS Secrets Manager")
		return "", err
	}

	secretValue, ok := secretData[secretKey].(string)
	if !ok {
		log.Printf("Chave '%s' não encontrada no AWS Secrets Manager", secretKey)
		return "", fmt.Errorf("chave '%s' não encontrada", secretKey)
	}

	return secretValue, nil
}

// CreateSecretsManagerSession cria uma sessão para o AWS Secrets Manager.
func CreateSecretsManagerSession() (*secretsmanager.SecretsManager, error) {
	sess, err := CreateAWSSession()
	if err != nil {
		log.Println("Erro ao criar sessão AWS")
		return nil, err
	}
	return secretsmanager.New(sess), nil
}

// CreateAWSSession cria e retorna uma sessão AWS configurada para a região 'us-east-1'.
func CreateAWSSession() (*session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		log.Println("Erro ao criar sessão AWS")
	}
	return sess, err
}
