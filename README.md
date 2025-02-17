# Distributed NYC Taxi Data Analysis

This is a project implemented as part of a course.

This project utilizes the MapReduce paradigm to, at a high level, obtain the top-k drivers from a publicly available NYC Taxi dataset with respect to their revenue per minute. On top of MapReduce, I gained a better understanding of multithreading, parallelization, networking, and communication between nodes in a distributed system. At the same time, I demonstrated proficiency in Java, and learned to run this cluster through Google Cloud.

Link to Dataset: https://chriswhong.com/open-data/foil_nyc_taxi/

# High Level Design

At a high level, we have mapper servers that clean and validate unique partitions of data, and send unique records from the dataset to each reducer node. Then, the reducers process data concurrently from each mapper, and in parallel to one another to aggregate the top K drivers by revenue per minute. Then, a final 'merger' server in the cluster concurrently merges the top K
drivers from both reducers.

<img src="https://github.com/user-attachments/assets/e3a4eba4-0d47-49e5-8c96-c80c98b917f4" width="600">

# Running on Google Cloud

#### Provisioned VM Instances
<img width="1512" alt="Screenshot 2025-02-16 at 10 30 10 PM" src="https://github.com/user-attachments/assets/2ff44fed-706a-44cc-9fe5-023d4c92a31d" />


#### Results from Each Machine

##### Mapper Client

From the perspective of the mapper, querying took just over an hour! Most of the time was spent on reading from disk, though multithreading definitely helps.
<img width="1256" alt="Screenshot 2025-02-16 at 10 28 33 PM" src="https://github.com/user-attachments/assets/91cc44cd-ba07-40af-83b7-d506aba228c2" />

#### Reducer Server
<img width="1007" alt="Screenshot 2025-02-16 at 10 29 18 PM" src="https://github.com/user-attachments/assets/c8165b31-1eaf-45df-ae3d-2684b867053c" />

#### Merger Server
<img width="826" alt="Screenshot 2025-02-16 at 10 29 02 PM" src="https://github.com/user-attachments/assets/874685ec-3d2f-483a-b44d-e86c7d78f4c0" />

#### Results

The records for each record are duplicated here, though I used the same dataset for each mapper so this is to be expected.
<img width="906" alt="Screenshot 2025-02-16 at 10 29 42 PM" src="https://github.com/user-attachments/assets/4459c7be-fa02-4c31-81af-3043b38ca3aa" />


# Notes & Learnings

### Sockets

I became extremely familiar with sockets through this project, as well as tradeoffs involved and managing communication between them. It is important to note that there was another viable design for this system, as given below. However, given that network buffers are allocated at the level of a socket, we can significantly increase throughput if we concurrently send data through two sockets at the same time to the reducer layer of the cluster.

<img src="https://github.com/user-attachments/assets/e57fe252-b647-4fd3-8b10-d80863ec0818" width="600">

### Multi-Threading

I additionally learned to apply multi-threading in a new context as well beyond at the system level from previous courses. Given that the mapper nodes are reading data from disk, there is potential to optimize through multi-threading in a producer-consumer manner. By having a thread read data from disk and publish that information to a message queue, then having subscribers read from that queue concurrently, we can take advantage of the blocking I/O operations of the producer thread to allow consumers to do work.

On my local machine, this even reduced runtime of solely data processing from the mapper layer by ~40%. The following images display a single-threaded and multi-threaded workflow, respectively.

Single-Threaded
![image](https://github.com/user-attachments/assets/83e90e7a-823a-4d91-b848-8a8168b6ab19)

Multi-Threaded
![image](https://github.com/user-attachments/assets/c6be5d1b-30b5-485b-89d3-0277662984ef)

### Future Work

While the TCP protocol ensures reliable transmission, for fault tolerance, there could definitely be work done to identify and handle errors during transmission at the application layer. Especially in the case of a potential node failure during transmission. Timeouts could be useful for this.







# How to compile the project

This project was created using Apache Maven, which you will need to install on your system to run.

Apache Maven may be installed from here (https://maven.apache.org/), though I've found (https://sdkman.io/) helpful for installing it.

Type on the command line: 

```bash
mvn clean compile
```

# How to Run the project

You will need to first run the merge server, then two reducer servers, then the two mapper clients to set up the system correctly.

key for ambiguous parameters:
- server-port: The port you want the process to be available at
- host-name: the IP address of a machine you're trying to connected to

Merge Server
```bash
mvn clean compile exec:java@mergeserver -Dexec.args="<server-port> <output-file-name>"
```

Reducer Server
```bash
mvn clean compile exec:java@reducerserver -Dexec.args="<server-port> <host-name> <host-port>"
```
Mapper Client
```bash
mvn clean compile exec:java@mapperclient -Dexec.args="<host-1-name> <host-1-port> <host-2-name> <host-2-port> <file-name>"
```

## Example for running on localhost

Note: It is important to run these commands in order, and in separate terminals.

If running on different machines, it's fine to reuse the same port. however, the servers must run on different ports if running on the same machine.

1. ```mvn clean compile exec:java@mergeserver -Dexec.args="33333 output" ```
2. ```mvn clean compile exec:java@reducerserver -Dexec.args="33334 localhost 33333"```
3. ```mvn clean compile exec:java@reducerserver -Dexec.args="33335 localhost 33333"```
4. ```mvn clean compile exec:java@mapperclient -Dexec.args="localhost 33334 localhost 33335 [dataset-name-on-your-machine]"```
5. ```mvn clean compile exec:java@mapperclient -Dexec.args="localhost 33334 localhost 33335 [dataset-name-on-your-machine]"```








