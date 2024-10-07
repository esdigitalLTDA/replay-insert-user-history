package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

type UserHistory struct {
	UserID                   string   `json:"userId"`
	JobID                    string   `json:"jobId"`
	TotalDuration            *big.Int `json:"totalDuration"`
	TotalRewardsConsumer     *big.Int `json:"totalRewardsConsumer"`
	TotalRewardsContentOwner *big.Int `json:"totalRewardsContentOwner"`
}

func prepareDataForBlockchain(jobs []JobDataRow) ([]UserHistory, error) {
	var userHistories []UserHistory

	for _, job := range jobs {
		userHistory := UserHistory{
			UserID:                   job.UserID,
			JobID:                    job.JobID,
			TotalDuration:            big.NewInt(job.TotalDuration),
			TotalRewardsConsumer:     ToWei(job.TotalRewardsConsumer),
			TotalRewardsContentOwner: ToWei(job.TotalRewardsContentOwner),
		}

		userHistories = append(userHistories, userHistory)
	}

	log.Printf("Total records prepared for insertion to blockchain: %d", len(userHistories))
	return userHistories, nil
}

func addToBlockchain(userHistories []UserHistory) error {
	client, err := ethclient.Dial(os.Getenv("RPC_URL"))
	if err != nil {
		log.Printf("Error connecting to Ethereum client: %v", err)
		return err
	}

	key := os.Getenv("DEPLOYER_PRIVATE_KEY")
	if strings.HasPrefix(key, "0x") {
		key = key[2:]
	}
	privateKey, err := crypto.HexToECDSA(key)
	if err != nil {
		log.Printf("Error converting private key: %v", err)
		return err
	}

	fromAddress := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Obtain initial nonce
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		return err
	}

	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return err
	}

	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		return err
	}

	contractAddress := common.HexToAddress(os.Getenv("CONTRACT_ADDRESS"))
	parsedABI, err := abi.JSON(strings.NewReader(ABI))
	if err != nil {
		log.Printf("Error parsing ABI: %v", err)
		return err
	}

	contract := bind.NewBoundContract(contractAddress, parsedABI, client, client, client)

	batchSize := 50
	var failedBatches []UserHistory

	for i := 0; i < len(userHistories); i += batchSize {
		end := i + batchSize
		if end > len(userHistories) {
			end = len(userHistories)
		}

		batch := userHistories[i:end]

		var userIds []string
		var totalDurations []*big.Int
		var totalRewardsConsumers []*big.Int
		var totalRewardsContentOwners []*big.Int

		for _, history := range batch {
			userIds = append(userIds, history.UserID)
			totalDurations = append(totalDurations, history.TotalDuration)
			totalRewardsConsumers = append(totalRewardsConsumers, history.TotalRewardsConsumer)
			totalRewardsContentOwners = append(totalRewardsContentOwners, history.TotalRewardsContentOwner)
		}

		auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
		if err != nil {
			return err
		}
		auth.Nonce = big.NewInt(int64(nonce))
		auth.Value = big.NewInt(0)
		auth.GasLimit = uint64(30000000)
		auth.GasPrice = gasPrice

		tx, err := contract.Transact(auth, "insertUserHistory", userIds, totalDurations, totalRewardsConsumers, totalRewardsContentOwners)
		if err != nil {
			log.Printf("Error sending transaction: %v", err)
			failedBatches = append(failedBatches, batch...)
			continue
		}

		if tx == nil {
			log.Printf("Returned transaction is nil")
			failedBatches = append(failedBatches, batch...)
			continue
		}

		// Wait for transaction confirmation
		receipt, err := waitForConfirmation(client, tx.Hash())
		if err != nil {
			log.Printf("Error waiting for transaction confirmation. Hash: %s, Error: %v", tx.Hash().Hex(), err)
			failedBatches = append(failedBatches, batch...)
			continue
		}

		if receipt.Status == 1 {
			log.Printf("Transaction successfully confirmed! Hash: %s", tx.Hash().Hex())
		} else {
			fmt.Printf("Transaction failed with status: %d, error: %v", receipt.Status, err)
			failedBatches = append(failedBatches, batch...)
			continue
		}

		fmt.Printf("Batch %d to %d processed successfully\n", i+1, end)

		// Increment nonce for next transaction
		nonce++
	}

	if len(failedBatches) > 0 {
		saveFailedBatches(failedBatches)
	}

	return nil
}

func saveFailedBatches(failedBatches []UserHistory) {
	file, err := os.Create("failed_batches.json")
	if err != nil {
		log.Printf("Error creating failed batches file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(failedBatches); err != nil {
		log.Printf("Error saving failed batches to file: %v", err)
	}
}

func waitForConfirmation(client *ethclient.Client, txHash common.Hash) (*types.Receipt, error) {
	for {
		receipt, err := client.TransactionReceipt(context.Background(), txHash)
		if errors.Is(err, ethereum.NotFound) {
			time.Sleep(time.Second * 2)
			continue
		} else if err != nil {
			return nil, err
		}

		return receipt, nil
	}
}

func ToWei(value float64) *big.Int {
	weiValue := new(big.Float).Mul(big.NewFloat(value), big.NewFloat(1e18))
	weiBigInt := new(big.Int)
	weiValue.Int(weiBigInt)
	return weiBigInt
}
