# ToG: Backup and Replay in Real-time

[![Build Status](https://travis-ci.org/overvenus/tidbongoogle.svg?branch=master)](https://travis-ci.org/overvenus/tidbongoogle)

We use Raft Learner to **Backup** TiDB cluster to Google Drive, and **Replay**
it to Google Spreadsheet in real-time.

## Cluster Overview

![Overview](./images/overview.svg)

* Learner TiKV receives Raft log from its leader.

## Architecture

![Architecture](./images/architecture.svg)
