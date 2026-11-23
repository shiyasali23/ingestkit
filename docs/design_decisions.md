# Design decisions

## Batches split by workers and by size
Slides are hundreds of MB to several GB, while VCF and clinical files are a few KB. So batches are balanced by bytes, not by file count. The planner makes at least one batch per worker, so a small run still uses every core. If the data is bigger than `batch_size_gb` per batch, it makes more. Files go in largest first, each into the lightest batch, so one worker does not end up with three big slides while another only gets small VCFs. `batch_size_gb` is a target, not a hard limit.

## SHA-256 computed while copying
The checksum is calculated from the same bytes that are written. Then the staged copy is read back and compared. Raw files are named by their checksum, so a rerun or a resumed batch can never create a duplicate, and identical files are stored only once.

## Staging first, then parse, then raw
A file only reaches `raw/` after its copy has been verified and it has parsed cleanly. A file that fails goes to `quarantine/` with its reason, so `raw/` and `processed/` only hold data that passed every check.

## State in a database, saved per batch
Each batch result is saved in one transaction: the file statuses and the audit events. A crash loses at most the batches that were running, and `resume` picks up anything not marked done. The files table is also the lineage: for every stored file it holds the source path, the checksum, the raw and processed paths, the run and the code version. `ingestkit trace` reads it from there.

## Process pool with spawn
Workers are processes, not threads, because parsing is CPU work and Python threads share one interpreter lock. The pool uses `spawn`, because pyarrow and boto3 start threads, and forking a process that has threads can deadlock. There is one worker per core, up to `max_workers`.

## One machine first, Kubernetes when one machine is not enough
Moving data is usually limited by network and disk, not CPU. One server with a process pool can handle a lab's daily volume. Kubernetes helps when one machine's network is maxed out or the run has to survive a node failure. The worker code is the same in both cases. Only the runner changes.

## De-identification in the parse step
In production this belongs at the hospital, before the data leaves the site. Here it runs in the parse step: `raw/` keeps the exact copy for audits, and `processed/` holds the de-identified tables the ML team uses. Pseudonyms use HMAC with a secret key, so nobody can rebuild them by hashing a list of known hospital IDs.

## Known limits and next steps
- The genomics reader keeps the passing variants in memory. That is fine for gene panels, but whole genome VCFs would need to be written in chunks.
- There is one destination bucket with versioning. Production would put `raw/` in its own bucket with S3 Object Lock and a retention period.
- Clinical tables are written one per file. With many thousands of cases, a partitioned dataset or a table format such as Iceberg would work better.
- Monitoring is the JSON logs plus the state database (`ingestkit status`). A Prometheus endpoint and alerts would be the next step.
- The cloud pieces (Terraform, Kubernetes, the Airflow deployment) are checked but not deployed.
