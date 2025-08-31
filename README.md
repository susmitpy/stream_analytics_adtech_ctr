# Real-Time Click-Through Rate (CTR) Analysis with Flink & Kafka

This project demonstrates a complete, real-time stream processing pipeline to calculate the Click-Through Rate (CTR) for advertising campaigns. It uses a Go-based data producer to simulate user impressions and clicks, Kafka as the message bus, and a PyFlink job to process the data in real-time.

Demo Video: [Demo Video](https://youtu.be/BsgQgPNZHYc)

## 📑 Table of Contents

- [Objective](#objective)
- [Tech Stack](#tech-stack)
- [Usage](#usage)
  - [Prerequisites](#prerequisites)
  - [Running the Demo](#running-the-demo)
  - [Viewing the Results](#viewing-the-results)
- [Components](#components)
  - [1. Go-based Kafka Producer](#1-go-based-kafka-producer)
  - [2. Flink Processing](#2-flink-processing)
    - [2.1. Two Sources](#21-two-sources)
    - [2.2. Interval Join](#22-interval-join)
    - [2.3. Windowed Aggregation](#23-windowed-aggregation)
    - [2.4. Sink](#24-sink)
- [My Learnings](#my-learnings)
  - [1. My First Go Project](#1-my-first-go-project)
  - [2. Crucial Bash Learnings](#2-crucial-bash-learnings)
  - [3. Flink Insights and Gotchas](#3-flink-insights-and-gotchas)
  - [4. AI-Assisted Development](#4-ai-assisted-development)

## Objective

The primary goal of this project is to build a robust data pipeline that can:
1.  Ingest two separate streams of data: `impressions` and `clicks`.
2.  Join these streams in real-time based on a time-bound condition (an impression must be followed by a click within a specific interval).
3.  Aggregate the results over tumbling time windows to calculate the number of impressions, clicks, and the final CTR for each campaign.
4.  Persist the aggregated results to the filesystem for further analysis.

## Tech Stack

*   **Stream Processing:** Apache Flink (PyFlink)
*   **Messaging Platform:** Apache Kafka
*   **Data Producer:** Go
*   **Containerization:** Docker & Docker Compose
*   **Scripting & Tooling:** Bash, Python (Pandas)

## Usage

Follow these steps to get the demo up and running on your local machine.

### Prerequisites

1.  **Go:** You must have the Go programming language installed on your local machine to run the data producer.
2.  **Docker:** You must have Docker and Docker Compose installed to build and run the containerized infrastructure (Kafka, Flink).

### Running the Demo

The entire process is automated with a single script. Open your terminal and run:

```bash
bash run_demo.sh
```

This script will perform the following actions:
1.  **Start Infrastructure:** It launches the Kafka and Flink (JobManager, TaskManager) containers in the background using `docker compose up -d`.
2.  **Create Kafka Topics:** It waits for Kafka to be ready and then creates the necessary topics: `impressions` and `clicks`.
3.  **Submit Flink Job:** The `ctr.py` PyFlink script is submitted to the Flink cluster to start the stream processing.
4.  **Launch Data Producer:** It starts the Go-based data producer, which will begin sending impression and click events to Kafka.
5.  **Monitor:** You can monitor the Flink job's progress via the Flink UI at `http://localhost:8081`.

To stop the demo and clean up all resources, simply press `Ctrl+C` in the terminal where the script is running. The script has a cleanup trap that will automatically shut down and remove the Docker containers.

### Viewing the Results

The Flink job writes its output as CSV files to the `./output/ctr_results` directory. A helper Python script is provided to read these partitioned files. After running the demo for a minute, you can view the results with:

```bash
python read_results.py
```

## Components

The pipeline consists of two main components: the data producer and the Flink processing job.

### 1. Go-based Kafka Producer

The producer is a Go application located in the `producer/` directory. Its sole responsibility is to generate synthetic advertising data and publish it to Kafka.

*   It produces messages to two topics: `impressions` and `clicks`.
*   It generates approximately 5 impressions per second.
*   For each impression, there is a 25% probability that a corresponding `click` event is generated.
*   Clicks are intentionally delayed by a random duration (up to 10 seconds) to simulate real-world user behavior and test the stream processor's ability to handle out-of-order events.

### 2. Flink Processing

The core stream processing logic is defined in `flink/ctr.py`. The pipeline can be broken down into four main stages:

#### 2.1. Two Sources

The pipeline begins by ingesting data from two distinct Kafka topics.
*   **Impressions Source:** Reads from the `impressions` topic.
*   **Clicks Source:** Reads from the `clicks` topic.

#### 2.2. Interval Join

Once the two streams are established, they are joined together using an **Interval Join**. This is a time-aware join that correlates events from both streams based on a time window.

The join condition is twofold:
1.  The `impression ID` must match (`impressions.impr_id == clicks.impr_id`).
2.  The click event must occur within **15 seconds** *after* the impression event.

This ensures that a click is only attributed to an impression if it happens shortly after the impression occurred, filtering out unrelated events.

#### 2.3. Windowed Aggregation

After a successful join, the resulting stream of correlated impression-click pairs is aggregated using a **30-second Tumble Window**.

A tumble (or tumbling) window slices the stream into fixed-size, non-overlapping time windows. For every 30-second window, the job groups the data by `campaign_id` and performs two key aggregations:
*   Counts the number of distinct impressions.
*   Counts the number of distinct clicks.

Finally, it calculates the CTR for each campaign within that window (`clicks / impressions`).

#### 2.4. Sink

The final aggregated results are written to the local filesystem using Flink's **File Sink**.

*   **Connector:** The sink is configured with the `'filesystem'` connector, outputting partitioned CSV files to the `/output/ctr_results` directory.
*   **Partitioning:** The results are partitioned by `campaign_id`, meaning a separate subdirectory is created for each campaign.
*   **Checkpointing for the File Sink:** The File Sink is a transactional sink that relies on Flink's checkpointing mechanism to commit data. In-progress files are written to a temporary directory. Only when a checkpoint is successfully completed does the sink move these files to the final output directory, making them visible. This provides exactly-once processing guarantees, ensuring that data is neither lost nor duplicated in the output, even in the event of a failure. For this reason, checkpointing is explicitly enabled in the job with a 30-second interval: `execution.checkpointing.interval", "30s"`.

## My Learnings

This project was a fantastic learning experience, and I'm walking away with several key insights into the tools and technologies I used.

### 1. My First Go Project
*   **Learning the Language:** I chose Go for the data producer specifically to learn it. Using goroutines to simulate click delays was a perfect, hands-on introduction to its powerful concurrency model.
*   **Clean JSON Handling:** I really liked how Go handles JSON serialization with struct tags. Defining the JSON key directly in the struct (e.g., `CampaignID string \`json:"campaign_id"\``) felt clean and intuitive. As a heavy user of Python's dataclasses, this feature felt both familiar and powerful.

### 2. Crucial Bash Learnings
*   **`trap` for Cleanup:** I learned to use the `trap` command for robust script cleanup, which functions much like Go's `defer`. It ensures my Docker containers always shut down cleanly.
*   **`sh -n` for Dry Runs:** Using `sh -n` to perform syntax checks on my script without executing it was a simple but effective way to catch errors early.

### 3. Flink Insights and Gotchas
*   **Timestamp Precision is Critical:** I learned you must be precise with timestamp data types. I had to explicitly cast numeric epoch values to `TIMESTAMP_LTZ` for Flink's watermarking and time-based joins to function correctly.
*   **Data Columns vs. Logical Aliases:** I discovered a key PyFlink API distinction: use `table_obj.column_name` for clarity when accessing physical data columns. However, for logical constructs created in the plan (like a window defined with `.alias("w")`), you **must** use `E.col("w")` to reference them. This highlights the difference between the data schema and the logical plan.
*   **Append vs. Update Streams:** I gained a clearer understanding of how windowed aggregations are crucial for creating simple, append-only output streams that are compatible with most sinks, as opposed to continuous aggregations which can produce retractions.
*   **Fail Fast, Don't Ignore Errors:** My takeaway is to never configure jobs to ignore parse errors. It's better to let the job fail fast, which immediately signals an upstream data quality issue that needs to be fixed.
*   **Architecture: Flink -> Kafka -> Sink:** I realized a more robust architectural pattern is often to sink Flink results back to a Kafka topic. From there, Kafka Connect can reliably handle delivery to a final destination (like MongoDB), offering more flexibility than native Flink sinks.

### 4. AI-Assisted Development
*   **Git Ingest for Full Context:** The ability to provide my entire codebase as context to the LLM was a massive help. It enabled a much deeper understanding of the project, which made generating documentation and debugging far more efficient.
*   **LLM Choice Matters:** As of August 2025, I found that for PyFlink-specific syntax, Gemini 2.5 Pro was more effective than GPT-5 Thinking, Claude Sonnet 4, or Grok Code Fast 1. This really drove home how an LLM's specific training data impacts its performance on specialized libraries.