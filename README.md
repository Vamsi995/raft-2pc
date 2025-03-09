# Raft-2PC: Distributed Transaction Processing with Raft and Two-Phase Commit

## Overview

This project implements a **fault-tolerant distributed transaction processing system** using a combination of **Raft consensus** and **Two-Phase Commit (2PC)**. The system is designed for a **simple banking application** where clients initiate transactions involving data partitions (shards) replicated across multiple clusters of servers.

- **Intra-shard transactions** are handled using the **Raft protocol** for consensus within a shard.
- **Cross-shard transactions** are coordinated using the **Two-Phase Commit (2PC) protocol** to ensure atomicity across multiple shards.

## System Architecture

The system partitions data into **shards**, each managed by a **cluster** of servers:

- **Three clusters (C1, C2, C3)**, each managing a distinct **data shard**.
- **Each shard is replicated** across all servers within a cluster for **fault tolerance**.
- **Clients act as transaction coordinators** and submit **intra-shard** or **cross-shard** transactions.

<p align="center">
<img  width="608" alt="image" src="https://github.com/user-attachments/assets/d6300ab2-027e-46a8-be4b-2b44c6843d96" />
</p>
<p align="center">
<img width="936" alt="image" src="https://github.com/user-attachments/assets/37148f50-98c9-4c10-8802-e03342844839" />
</p>

## Transaction Handling

### **1. Intra-Shard Transactions (Raft Consensus)**
For transactions within the same shard:
- A client sends a request to a leader in the cluster.
- The leader checks balance availability and locks necessary data items.
- The **Raft protocol** is used to achieve consensus within the shard.
- Once committed, the transaction is applied to the **data store**.

### **2. Cross-Shard Transactions (Two-Phase Commit)**
For transactions across multiple shards:
- The client acts as a **2PC coordinator**, contacting leaders of the involved clusters.
- Each cluster executes **Raft consensus** for their portion of the transaction.
- If all clusters are prepared, the coordinator sends a **commit** message.
- If any cluster aborts, the entire transaction is **rolled back**.

<p align="center">
<img width="585" alt="image" src="https://github.com/user-attachments/assets/9635b550-4a96-4c90-ba4f-158edeed89d5" />
</p>


## Features

✔ **Supports concurrent intra-shard and cross-shard transactions**  
✔ **Implements Raft consensus for fault tolerance**  
✔ **Implements Two-Phase Commit (2PC) for cross-shard atomicity**  
✔ **Lock management to prevent conflicts**  
✔ **Performance monitoring: throughput & latency measurement**  
✔ **Transaction logging and failure handling**  

## Running the Project

### **Dependencies**
- Python (latest version)

### **How to Run**
1. Clone the repository:
   ```sh
   git clone https://github.com/Vamsi995/raft-2pc.git
   cd raft-2pc
   ```
2. Starting Clusters:
   To start the first cluster we need to start the proxy communication manager, that acts as a proxy between the three servers in one cluster. This is made to simulate network partitions in the cluster.
   ```sh
   python communication_manager -cluster 1 -port 8080
   ```
   To start the servers in the cluster execute the below command
   ```sh
   python server1.py -cluster <cluster number> -port <port - should be the same one as the network proxy> candidate_id <server_number>
   ```
3. Reset the server metadata and datastore:
   ```sh
   make clean
   ```
4. Running the client
   ```sh
   python client.py
   ```

<p align="center">
<img width="294" alt="image" src="https://github.com/user-attachments/assets/7be95fac-6dbd-4d10-bbbc-8966b54c1a4d" />
</p>

5. Running the transactions: The transactions are kept in the `transactions.csv` file, in the format `<source_accountid>,<destination_accountid>,<amount>`

## Commands & Functions

### **Transaction Handling**
- `PrintBalance(clientID)`: Prints the balance of a given client.
- `PrintDatastore()`: Displays all committed transactions.
- `Performance()`: Prints throughput and latency metrics.

## Example Transactions

### **Intra-shard Transaction**
```sh
100,200,3  # Transfers 3 units from client 100 to 200 in the same cluster
```
<p align="center">
<img width="469" alt="image" src="https://github.com/user-attachments/assets/9570895e-84cd-4d23-bcb9-dc2363d2299f" />
</p>

### **Cross-shard Transaction**
```sh
100,1500,3  # Transfers 3 units from client 100 (Cluster C1) to 1500 (Cluster C2)
```
<p align="center">
  <img width="463" alt="image" src="https://github.com/user-attachments/assets/3722374f-a2c8-4997-966a-e19036e30a53" />
</p>


### Demo of Intra Shard & Cross Shard Transactions

- [Intra Shard & Cross Shard Demo](https://drive.google.com/file/d/1eB9-yZYa1qxGck20GrIhgZz-zcU6ISww/view?usp=sharing)

## Failure Handling
- Transactions are aborted if:
  - Insufficient balance
  - Lock conflicts
  - Failure to reach consensus in Raft
  - A cluster votes to abort during 2PC


## Contributors
- [Sai Vamsi Alisetti](https://github.com/Vamsi995) 
