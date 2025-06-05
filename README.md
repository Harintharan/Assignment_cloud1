# DBLP Multi-Stage MapReduce Pipeline

## Project Overview

This project implements a multi-stage Hadoop MapReduce pipeline that processes the **DBLP XML dataset** to extract insights about publications, authors, and collaboration trends in computer science.

---

##  Input Dataset

- **Source:** [DBLP XML Dataset](https://www.kaggle.com/datasets/dheerajmpai/dblp2023)
- **Format:** XML ( `dblp.xml`)
- **HDFS Upload:**  
  ```bash
  hdfs dfs -mkdir -p /input
  hdfs dfs -put dblp.xml /input/dblp.xml

## Technologies Used
Java 8+

Hadoop 3.3.1

Custom XML InputFormat (DBLPXmlInputFormat)

Maven

# GitHub Repository

 Clone the project:
```bash
git clone https://github.com/Harintharan/Assignment_cloud1.git
cd Assignment_cloud1
```
## How to Build

mvn clean package

# creates the executable JAR in:
target/DblpJob-1.0-SNAPSHOT.jar

```bash
hadoop jar target/DblpJob-1.0-SNAPSHOT.jar org.dblp.driver.MapReduceJobRunner 
/input/dblp.xml 
/output/bucketing 
/output/authorship_score 
/output/max_median_avg 
/output/sort_stage1 
/output/sort_stage2
```


# Note: All /output/... directories must not already exist in HDFS. If needed, clean up:
```bash
hdfs dfs -rm -r /output/bucketing /output/authorship_score /output/max_median_avg /output/sort_stage1 /output/sort_stage2
```

# View Sample Output:
```bash
hdfs dfs -cat /output/bucketing/part-r-00000 | head
hdfs dfs -cat /output/authorship_score/part-r-00000 | head
hdfs dfs -cat /output/max_median_avg/part-r-00000 | head
hdfs dfs -cat /output/sort_stage2/part-r-00000 | head

