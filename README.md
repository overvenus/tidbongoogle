# ToG: Backup and Replay TiDB cluster in Real-time

[![Build Status](https://travis-ci.org/overvenus/tidbongoogle.svg?branch=master)](https://travis-ci.org/overvenus/tidbongoogle)

We use Raft Learner to **Backup** TiDB cluster to Google Drive, and **Replay**
it to Google Spreadsheet in real-time.

## Quickstarts

### Step 1, Turn on Google Drive/Spreadsheet API

Following the link: https://developers.google.com/drive/api/v3/quickstart/go
Click the bule button "ENABLE THE DRIVE API". Save your credentials.json

### Step2, Start up a TiDB cluster

For now we only support one TiKV node).

```sh
# Check out learner TiKV
$ git clone https://github.com/overvenus/tikv.git
$ git checkout rngine
$ cargo build --bin tikv-server
$ mv bin/tikv-server bin/tikv-server-rngine # Rename it to tikv-server-rngine
# Check out PD
$ git clone https://github.com/pingcap/pd.git
$ git checkout ov/add-learner-dont-promote-learner
$ make
# Download latest TiDB/TiKV and save it to bin/tikv-server
# For now, we should have 5 executable files under bin/:
#   1. tikv-server-rngine
#   2. pd-server
#   3. tidb-server
#   4. tikv-server
# And a tool for PD
#   5. pd-ctl
# Start PD and TiKV
$ ./bin/pd-server &; ./bin/tikv-server &
# Build and run ToG
$ make && bin/gogine # gogine will requests the Drive and the Sheet token
# Run learner TiKV
$ ./bin/tikv-server-rngine &
# Add region 2 to learner TiKV
$ ./bin/pd-ctl -u ${PD_CLIENT_URL}
>> op add add-learner 2 4
# Run TiDB
$ ./bin/tidb
# Create your favorite table ...
```

Check out your Drive folder.

## Cluster Overview

![Overview](./images/overview.svg)

* Learner TiKV receives Raft log from its leader.

## Architecture

![Architecture](./images/architecture.svg)
