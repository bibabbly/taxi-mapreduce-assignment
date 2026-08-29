\# Distributed Taxi Trip Analytics — Hadoop MapReduce



\*\*Course:\*\* Big Data Essentials — MSc Big Data Analytics, AUCA

\*\*Student:\*\* Theoneste Bizimungu

\*\*Instructor:\*\* Dr. Kundan Kumar

\*\*Status:\*\* Work in progress



\---



\## Environment



| Component | Version |

|---|---|

| OS | Windows 11 (64-bit) |

| Hadoop | 3.3.6 (pseudo-distributed) |

| Java | JDK 1.8.0\_202 |

| Python | 3.x |

| Spark | 3.5.9 (for performance comparison) |



\*\*Paths\*\*



\- `HADOOP\_HOME` = `C:\\hadoop`

\- NameNode data — `C:\\hadoop\\data\\namenode`

\- DataNode data — `C:\\hadoop\\data\\datanode`

\- Windows native binaries — `winutils.exe`, `hadoop.dll` in `C:\\hadoop\\bin`



\*\*Services\*\*



\- HDFS NameNode UI — http://localhost:9870

\- YARN ResourceManager UI — http://localhost:8088

\- HDFS RPC — `hdfs://localhost:9000`



\---



\## Repository structure









\---



\## Dataset



NYC Taxi \& Limousine Commission (TLC) Trip Record Data

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page



Source files are Parquet and are converted to CSV before HDFS upload, since

Hadoop Streaming processes input line by line. Parquet is columnar and

compressed, which suits large-scale storage; CSV is row-oriented and therefore

convenient for demonstrating line-based Streaming.



\---



\## HDFS layout





\---



\## Running a job





\*\*Note on Windows syntax:\*\* the `-files` argument must be quoted. Without quotes

`cmd.exe` treats the comma as an argument separator and the job fails with

`Found 1 unexpected arguments on the command line`.



\---



\## Environment notes



Two Windows-specific issues were encountered and resolved during setup:



1\. \*\*NameNode permission failure\*\* — formatting HDFS from an Administrator prompt

&#x20;  made `data\\namenode\\current\\VERSION` unreadable to the normal user account.

&#x20;  Resolved with `icacls` and by running all Hadoop commands as the same user.



2\. \*\*YARN local directory permissions\*\* — the NodeManager requires `rwxr-xr-x` on

&#x20;  its local directories and validates this through the Windows security API.

&#x20;  Configured explicit paths via `yarn.nodemanager.local-dirs` and applied

&#x20;  permissions with `winutils.exe chmod`.



Both are documented in full in the final report.

