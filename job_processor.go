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
	fmt.Println("Processing jobs...")

	client, err := GetBigQueryClient(secretName)
	if err != nil {
		log.Println("Failed to create BigQuery client: ", err)
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
		log.Println("Failed to execute query: ", err)
		return err
	}

	var jobs []JobDataRow
	var count int

	for {
		var row JobDataRow
		err := rows.Next(&row)
		if errors.Is(err, iterator.Done) {
			if count == 0 {
				log.Println("Query returned an empty set.")
			} else {
				log.Printf("Total jobs read: %d", count)
			}
			break
		}
		if err != nil {
			log.Println("Failed to read results: ", err)
			return err
		}

		jobs = append(jobs, row)
		count++
	}

	userHistories, err := prepareDataForBlockchain(jobs)
	if err != nil {
		log.Println("Error preparing data for blockchain:", err)
		return err
	}

	err = addToBlockchain(userHistories)
	if err != nil {
		log.Println("Error inserting data into blockchain:", err)
		return err
	}

	fmt.Println("All jobs have been successfully processed and inserted into the blockchain")

	return nil
}
