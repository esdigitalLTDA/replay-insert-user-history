package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/api/iterator"
)

type JobDataRow struct {
	ChunkID                  float64 `bigquery:"CHUNK_ID"`
	JobID                    string  `bigquery:"JOB_ID"`
	TotalDuration            int64   `bigquery:"totalDuration"`
	TotalRewardsConsumer     float64 `bigquery:"totalRewardsConsumer"`
	TotalRewardsContentOwner float64 `bigquery:"totalRewardsContentOwner"`
	UserID                   string  `bigquery:"userId"`
}

func processJobs(secretName string) error {
	fmt.Println("Processando jobs...")

	client, err := GetBigQueryClient(secretName)
	if err != nil {
		log.Println("Falha ao criar cliente BigQuery: ", err)
		return err
	}
	defer client.Close()

	queryStr := `
		SELECT
			CHUNK_ID,
			JOB_ID,
			totalDuration,
			totalRewardsConsumer,
			totalRewardsContentOwner,
			userId
		FROM
			replay-353318.replayAnalytics.view_blockchain_historical_data
	`

	query := client.Query(queryStr)

	rows, err := query.Read(context.Background())
	if err != nil {
		log.Println("Falha ao executar a query: ", err)
		return err
	}

	var jobs []JobDataRow
	var count int

	for {
		var row JobDataRow
		err := rows.Next(&row)
		if errors.Is(err, iterator.Done) {
			if count == 0 {
				log.Println("A query retornou um conjunto vazio.")
			} else {
				log.Printf("Total de jobs lidos: %d", count)
			}
			break
		}
		if err != nil {
			log.Println("Falha ao ler os resultados: ", err)
			return err
		}

		jobs = append(jobs, row)
		count++
	}

	userHistories, err := prepareDataForBlockchain(jobs)
	if err != nil {
		log.Println("Erro ao preparar dados para a blockchain:", err)
		return err
	}

	err = addToBlockchain(userHistories)
	if err != nil {
		log.Println("Erro ao inserir dados na blockchain:", err)
		return err
	}

	fmt.Println("Todos os jobs foram processados e inseridos na blockchain com sucesso")

	return nil
}
