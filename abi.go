package main

const ABI = `[
    {
      "inputs": [],
      "stateMutability": "nonpayable",
      "type": "constructor"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "previousOwner",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "newOwner",
          "type": "address"
        }
      ],
      "name": "OwnershipTransferred",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "Paused",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "previousAdminRole",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "newAdminRole",
          "type": "bytes32"
        }
      ],
      "name": "RoleAdminChanged",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "account",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "sender",
          "type": "address"
        }
      ],
      "name": "RoleGranted",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "account",
          "type": "address"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "sender",
          "type": "address"
        }
      ],
      "name": "RoleRevoked",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "string",
          "name": "userId",
          "type": "string"
        },
        {
          "indexed": true,
          "internalType": "uint256",
          "name": "day",
          "type": "uint256"
        },
        {
          "indexed": true,
          "internalType": "uint256",
          "name": "month",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "year",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "string",
          "name": "assetId",
          "type": "string"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "totalDuration",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "totalRewardsConsumer",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "totalRewardsContentOwner",
          "type": "uint256"
        }
      ],
      "name": "TransactionAdded",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "Unpaused",
      "type": "event"
    },
    {
      "inputs": [],
      "name": "ADMIN_ROLE",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "DEFAULT_ADMIN_ROLE",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "components": [
            {
              "internalType": "string",
              "name": "userId",
              "type": "string"
            },
            {
              "internalType": "uint256",
              "name": "day",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "month",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "year",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            },
            {
              "internalType": "string",
              "name": "assetId",
              "type": "string"
            }
          ],
          "internalType": "struct ReplayLibrary.Transaction[]",
          "name": "transactions",
          "type": "tuple[]"
        }
      ],
      "name": "batchInsertRecords",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        },
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "dailyTransactions",
      "outputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        },
        {
          "internalType": "uint256",
          "name": "day",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "month",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "year",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "totalDuration",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "totalRewardsConsumer",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "totalRewardsContentOwner",
          "type": "uint256"
        },
        {
          "internalType": "string",
          "name": "assetId",
          "type": "string"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        }
      ],
      "name": "getRoleAdmin",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        },
        {
          "internalType": "uint256",
          "name": "day",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "month",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "year",
          "type": "uint256"
        },
        {
          "internalType": "string",
          "name": "assetId",
          "type": "string"
        }
      ],
      "name": "getTransactionsByDay",
      "outputs": [
        {
          "components": [
            {
              "internalType": "string",
              "name": "userId",
              "type": "string"
            },
            {
              "internalType": "uint256",
              "name": "day",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "month",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "year",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            },
            {
              "internalType": "string",
              "name": "assetId",
              "type": "string"
            }
          ],
          "internalType": "struct ReplayLibrary.Transaction[]",
          "name": "",
          "type": "tuple[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        },
        {
          "internalType": "uint256",
          "name": "day",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "month",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "year",
          "type": "uint256"
        }
      ],
      "name": "getTransactionsByUserAndDate",
      "outputs": [
        {
          "components": [
            {
              "internalType": "string",
              "name": "userId",
              "type": "string"
            },
            {
              "internalType": "uint256",
              "name": "day",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "month",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "year",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            },
            {
              "internalType": "string",
              "name": "assetId",
              "type": "string"
            }
          ],
          "internalType": "struct ReplayLibrary.Transaction[]",
          "name": "",
          "type": "tuple[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        }
      ],
      "name": "getTransactionsByUserId",
      "outputs": [
        {
          "components": [
            {
              "internalType": "string",
              "name": "userId",
              "type": "string"
            },
            {
              "internalType": "uint256",
              "name": "day",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "month",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "year",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            },
            {
              "internalType": "string",
              "name": "assetId",
              "type": "string"
            }
          ],
          "internalType": "struct ReplayLibrary.Transaction[]",
          "name": "",
          "type": "tuple[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        },
        {
          "internalType": "string",
          "name": "assetId",
          "type": "string"
        }
      ],
      "name": "getTransactionsByUserIdAndAssetId",
      "outputs": [
        {
          "components": [
            {
              "internalType": "string",
              "name": "userId",
              "type": "string"
            },
            {
              "internalType": "uint256",
              "name": "day",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "month",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "year",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            },
            {
              "internalType": "string",
              "name": "assetId",
              "type": "string"
            }
          ],
          "internalType": "struct ReplayLibrary.Transaction[]",
          "name": "",
          "type": "tuple[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "userId",
          "type": "string"
        }
      ],
      "name": "getUserHistories",
      "outputs": [
        {
          "components": [
            {
              "internalType": "uint256",
              "name": "totalDuration",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsConsumer",
              "type": "uint256"
            },
            {
              "internalType": "uint256",
              "name": "totalRewardsContentOwner",
              "type": "uint256"
            }
          ],
          "internalType": "struct ReplayTrackingContractV3.UserHistory[]",
          "name": "",
          "type": "tuple[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "grantRole",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "hasRole",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string[]",
          "name": "userIds",
          "type": "string[]"
        },
        {
          "internalType": "uint256[]",
          "name": "totalDurations",
          "type": "uint256[]"
        },
        {
          "internalType": "uint256[]",
          "name": "totalRewardsConsumers",
          "type": "uint256[]"
        },
        {
          "internalType": "uint256[]",
          "name": "totalRewardsContentOwners",
          "type": "uint256[]"
        }
      ],
      "name": "insertUserHistory",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "string",
          "name": "",
          "type": "string"
        }
      ],
      "name": "nonces",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "owner",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "pause",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "paused",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "renounceOwnership",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "renounceRole",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "role",
          "type": "bytes32"
        },
        {
          "internalType": "address",
          "name": "account",
          "type": "address"
        }
      ],
      "name": "revokeRole",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes4",
          "name": "interfaceId",
          "type": "bytes4"
        }
      ],
      "name": "supportsInterface",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "transactionKeys",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "address",
          "name": "newOwner",
          "type": "address"
        }
      ],
      "name": "transferOwnership",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "unpause",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        },
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "name": "userHistories",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "totalDuration",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "totalRewardsConsumer",
          "type": "uint256"
        },
        {
          "internalType": "uint256",
          "name": "totalRewardsContentOwner",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    }
  ]`
