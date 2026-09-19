# Real-Time Fraud Detection System with Apache Flink

[![Java](https://img.shields.io/badge/Java-11%20%7C%2017-ED8B00?logo=openjdk&logoColor=white)](https://www.oracle.com/java/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.20.0-E6526F?logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Build Tool](https://img.shields.io/badge/Build-Maven-C71A36?logo=apachemaven&logoColor=white)](https://maven.apache.org/)

A distributed, high-throughput stream processing pipeline built on **Apache Flink** to detect fraudulent financial transactions in real time. It blends stateful stream processing with **Flink CEP (Complex Event Processing)** to identify complex anomaly patterns and evaluates detection accuracy against tagged ground truth data.

---

## 1. Demo / Screenshot / GIF

![Flink Web UI Dashboard](results/flink_dashboard.png)

> *Note: Please place your screenshot of the running Flink Web UI (`http://localhost:8081`) showing the Job execution DAG graph and TaskManagers at `results/flink_dashboard.png`.*

![Evaluation Script Output](results/evaluation_output.png)

> *Note: Please place your screenshot of running `python scripts/evaluate.py` displaying the confusion matrix and Precision/Recall/F1 metrics at `results/evaluation_output.png`.*

---

## 2. Features

* **Multi-Rule Stateful Detection Engine**:
  * **R1 (Small-then-Large Transaction Probe)**: Identifies card verification probes (< $1.00) followed by a substantial withdrawal (> $500.00) on the same account using Flink `ValueState` and timers.
  * **R2 (High-Frequency Transaction Burst)**: Flags rapid-fire automated card testing attacks (> 5 transactions within a 10-second sliding window) using Flink `ListState`.
  * **R3 (Deviation from Historical Moving Average)**: Detects abnormal volume spikes where a transaction exceeds 5× the average of the last 10 transactions.
  * **R4 (Multi-Step CEP Sequence Pattern)**: Uses Apache Flink CEP to detect strict multi-stage behavioral sequences: `Small (< $1) -> Small (< $1) -> Large (> $500)` within 2 minutes.
* **Fault-Tolerant & Low-Latency Architecture**:
  * Periodic checkpointing (30s interval) for state recovery and exactly-once processing semantics.
  * Watermark-driven event-time handling ensuring ordered stream execution under concurrent workloads.
* **Embedded Ground-Truth Generation**:
  * Continuous synthetic data stream generator (`LabeledTransactionSource`) injecting realistic normal activity alongside tagged fraud attack scenarios.
* **Automated Precision/Recall Evaluation Framework**:
  * Dedicated offline evaluation script (`scripts/evaluate.py`) with 1-to-1 temporal nearest-neighbor bipartite matching between alerts and ground-truth records.

---

## 3. Tech Stack

* **Core Stream Engine**: [Apache Flink 1.20.0](https://flink.apache.org/) (`flink-streaming-java`, `flink-clients`, `flink-cep`, `flink-walkthrough-common`)
* **Programming Languages**: Java 11 / 17, Python 3.8+
* **Data Processing & Metrics**: [Pandas](https://pandas.pydata.org/) (for offline evaluation)
* **Build & Dependency Management**: Apache Maven 3.6+, Maven Wrapper (`mvnw`)
* **Containerization / Orchestration**: Docker & Docker Compose (JobManager & TaskManagers cluster)
* **Logging**: SLF4J Simple Logger

---

## 4. Installation

### Prerequisites

* **Java JDK 11** or **17** (LTS) installed and configured (`JAVA_HOME`).
* **Python 3.8+** with `pip`.
* *(Optional)* **Docker Desktop** (for running the distributed Flink cluster).

### Environment Configuration (Windows PowerShell Helper)

If you have multiple Java versions installed on your machine, you can create a local helper script named `set-env.ps1` in the project root (this file is included in `.gitignore` to prevent committing machine-specific paths):

```powershell
# set-env.ps1 (adjust path to match your local JDK 11 or 17 directory)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.x-hotspot"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
Write-Host "JAVA_HOME configured for this PowerShell session." -ForegroundColor Green
```

Run it once in your active terminal before building or running:

```powershell
.\set-env.ps1
```

### Steps

1. **Clone the repository:**

   ```bash
   git clone https://github.com/Tienndat2306/Fraud_Detetion_Flink.git
   cd Fraud_Detetion_Flink
   ```
2. **Install Python evaluation dependencies:**

   ```bash
   pip install -r requirements.txt
   ```
3. **Build the Fat/Uber JAR package:**

   * On Windows (using the included Maven Wrapper):
     ```cmd
     .\mvnw.cmd clean package -DskipTests
     ```
   * On Linux / macOS:
     ```bash
     ./mvnw clean package -DskipTests
     ```

   *Output JAR:* `target/frauddetection-0.1.jar`

---

## 5. Usage

### Method 1: Run in IntelliJ IDEA / VS Code (Recommended for Development)

1. Open the project root in **IntelliJ IDEA**.
2. Set Project SDK to **Java 11** or **Java 17**.
3. Open `src/main/java/spendreport/FraudDetectionJob.java`.
4. Right-click `main()` -> **Modify Run Configuration...** -> check **"Add dependencies with 'provided' scope to classpath"**.
5. Click **Run**. Real-time transaction and alert logs will output to the console, and CSV records will automatically be saved to `output/`.

### Method 2: Run Locally via Maven Wrapper (CLI)

Run the job directly on an embedded Flink MiniCluster:

```powershell
# Windows PowerShell
.\set-env.ps1   # (Optional) Run your local environment script if created
.\mvnw.cmd exec:java "-Dexec.mainClass=spendreport.FraudDetectionJob" "-Dexec.classpathScope=test"
```

### Method 3: Deploy to Distributed Cluster via Docker Compose

1. Start the Flink cluster:
   ```bash
   docker compose up -d
   ```
2. Open Flink Web Dashboard at [http://localhost:8081](http://localhost:8081).
3. Submit the job via Web UI (**Submit New Job** -> upload `target/frauddetection-0.1.jar`) or via Docker CLI:
   ```bash
   docker cp target/frauddetection-0.1.jar fraud_detetion_flink-jobmanager-1:/opt/flink/
   docker exec -it fraud_detetion_flink-jobmanager-1 flink run /opt/flink/frauddetection-0.1.jar
   ```
4. Stop the cluster when finished:
   ```bash
   docker compose down
   ```

### Method 4: Evaluate Detection Performance

Once the job generates `output/ground-truth.csv` and `output/alerts.csv`, run:

```bash
python scripts/evaluate.py
```

---

## 6. Project Structure

```text
Fraud_Detetion_Flink/
├── .github/                               # CI/CD workflows (.github/modernize/ git-ignored)
├── .mvn/wrapper/                          # Maven Wrapper files
├── docker-compose.yml                     # Docker Compose for Flink JobManager + TaskManagers
├── mvnw.cmd                               # Maven Wrapper executable (Windows)
├── pom.xml                                # Project Object Model (dependencies & plugins)
├── requirements.txt                       # Python dependencies (pandas)
├── scripts/
│   └── evaluate.py                        # Precision, Recall & F1-score evaluation script
├── output/                                # Generated at runtime (git-ignored)
│   ├── ground-truth.csv                   # Ground-truth tagged transactions
│   └── alerts.csv                         # Detected fraud alerts
├── results/                               # Benchmark run outputs & screenshots (*.csv git-ignored)
│   ├── flink_dashboard.png                # <!-- TODO: Add Flink Web UI screenshot -->
│   ├── evaluation_output.png              # <!-- TODO: Add evaluation output screenshot -->
│   ├── alerts_run1.csv                    # Baseline evaluation alerts
│   └── ground-truth_run1.csv              # Baseline evaluation ground truth
└── src/main/java/spendreport/
    ├── FraudDetectionJob.java             # Flink Main Pipeline (Stream wiring, CEP, Checkpointing)
    ├── FraudDetector.java                 # Stateful KeyedProcessFunction (Rules R1, R2, R3)
    ├── LabeledTransactionSource.java      # Synthetic transaction stream source with fraud labels
    ├── GroundTruthLogger.java             # Logger writing transactions to ground-truth.csv
    ├── AlertSink.java                     # Sink writing triggered alerts to alerts.csv
    └── AlertSinkFunction.java             # Console Alert Sink
```

---

## 7. Results & Evaluation

Evaluation results from the automated benchmark comparing `alerts.csv` against `ground-truth.csv` (sample size: 2,000 transactions):

### Overall System Metrics

| Metric                              | Value                | Description                                            |
| :---------------------------------- | :------------------- | :----------------------------------------------------- |
| **Ground Truth Transactions** | `2,000`            | 936 Fraud (46.8%), 1,064 Normal (53.2%)                |
| **Total Alerts Generated**    | `993`              | Across rules R1, R2, R3, R4                            |
| **True Positives (TP)**       | `679`              | Actual frauds successfully flagged                     |
| **False Positives (FP)**      | `314`              | Normal transactions incorrectly flagged                |
| **False Negatives (FN)**      | `257`              | Fraudulent transactions missed                         |
| **True Negatives (TN)**       | `750`              | Legitimate transactions correctly ignored              |
| **Precision**                 | **`68.38%`** | Proportion of triggered alerts that were genuine fraud |
| **Recall**                    | **`72.54%`** | Proportion of total frauds detected                    |
| **F1-Score**                  | **`70.40%`** | Harmonic mean of Precision and Recall                  |

### Breakdown by Detection Rule

| Rule ID      | Detection Pattern        | TP | FP | FN |  TN  |     Precision     | Recall | F1-Score |
| :----------- | :----------------------- | :-: | :-: | :-: | :---: | :---------------: | :----: | :------: |
| **R1** | Small-then-Large Probe   | 204 |  0  | 732 | 1,064 | **100.00%** | 21.79% |  35.79%  |
| **R2** | High-Frequency Burst     | 240 | 200 | 696 |  864  | **54.55%** | 25.64% |  34.88%  |
| **R3** | Moving Average Deviation | 187 | 74 | 749 |  990  | **71.65%** | 19.98% |  31.24%  |
| **R4** | CEP Multi-Step Sequence  | 88 |  0  | 848 | 1,064 | **100.00%** | 9.40% |  17.19%  |

---

## 8. Dataset

* **Source**: Synthetic labeled financial transaction stream generated in real-time by [`LabeledTransactionSource.java`](src/main/java/spendreport/LabeledTransactionSource.java).
* **Sample Count**: 2,000 transactions per benchmark cycle.
* **Distribution of Injected Patterns**:
  * `NORMAL`: 886 transactions
  * `FRAUD_HIGHFREQ`: 400 transactions
  * `FRAUD_SMALL` / `FRAUD_LARGE`: 116 transactions each
  * `FRAUD_R4_STEP1` / `FRAUD_R4_STEP2` / `FRAUD_R4_LARGE`: 88 transactions each
  * `NOISE_SMALL` / `NOISE_LARGE`: 89 transactions each
  * `FRAUD_DEVIATION`: 40 transactions

---

## 9. License

This project is licensed under the [MIT License](LICENSE).

---

## 10. Contact / Author

* **Author**: Nguyen Tien Dat (Tienndat2306)
* **Email**: [dattrithuc123@gmail.com](mailto:dattrithuc123@gmail.com)
* **GitHub**: [Tienndat2306](https://github.com/Tienndat2306)
* **LinkedIn**: [<!-- TODO: your-linkedin-handle -->](https://linkedin.com/in/<!-- TODO: your-linkedin-handle -->)

---

## Checklist of Items to Fill Manually:

- [ ] Add screenshots to `results/flink_dashboard.png` and `results/evaluation_output.png`.
- [ ] (Optional) Add your LinkedIn profile URL in the **Contact / Author** section.
