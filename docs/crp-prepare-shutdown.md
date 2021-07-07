# crp-prepare-shutdown

## About

This tool will *prepare* MySQL for a graceful shutdown. **NOTE: IT WILL NOT ACTUALLY SHUTDOWN MYSQL.**

Ungraceful shutdowns of MySQL (just running `systemctl stop mysql`) do not:

- Guarantee replication was cleanly stopped
- Flush dirty buffer pool pages
- Wait for long transactions to complete
- Fully purge the undo logs
- Perform a full change buffer merge

Why is that bad?

Well, most of the time stopping MySQL is fine. However, under certain circumstances MySQL can startup in a bad state:

- Replication has erred (trying to apply an event but a record doesn't exist or a duplicate record was found)
- The shutdown command is just hanging (there is some looping message in the error log about waiting for a transaction or the change buffer)
- Maybe MySQL won't even startup (for some reason it's trying to perform crash recovery and failing)
- Maybe MySQL is starting up but it takes a long time (this can be anxiety inducing)

What are the "certain circumstances"?

It's not always easy to clearly identify but can depend upon the MySQL flushing configurations, load, concurrency, and underlying disk hardware.

When preparing MySQL for a minor/major upgrade, it is industry best practice to flush dirty pages and do a slow shutdown (`innodb_fast_shutdown=0`). This tool will follow those best practices every time.

Lastly, the tool can be ran against a standalone host or single-threaded/multi-threaded replicas. It will gracefully abort if the target host has any replicas.

## Overview

`crp-prepare-shutdown` will prepare MySQL for a graceful shutdown (it will not actually stop MySQL):

- Identify if the host is a replica (and stop replication).
- Check for any long running transactions (and gracefully abort if so).
- Set the following MySQL variables:
	- `innodb_max_dirty_pages_pct = 0` (and wait until dirty pages are "low enough")
	- `innodb_fast_shutdown = 0`
	- `innodb_buffer_pool_dump_at_shutdown = ON`
	- `innodb_buffer_pool_dump_pct = 75`
	- (The tool will recommend enabling `innodb_buffer_pool_load_at_startup` if it's not already.)

Once all steps are completed, it will notify you that MySQL is prepared for shutdown.

## Considerations

- It is strongly recommended not to use this tool on a MySQL host that is actively receiving write traffic. As the dirty pages will attempt to be completely flushed, it can increase disk operations greatly.

## Limitations

- By design, the tool will abort if the MySQL host has any replicas. For safety and best practice, any replicas should be relocated/failed over prior to shutdown.
- Currently, this tool does not support multi-channel replicas and will gracefully abort.

## Requrements

- A MySQL user with [SYSTEM_VARIABLES_ADMIN](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_system-variables-admin)

## Usage

Here is an example of the tool completing its happy path on a replica.

```
# ./crp-prepare-shutdown.py --verbose
2020-09-12 18:28:41 >>> [ START ] Preparing MySQL for shutdown.
2020-09-12 18:28:41 >>> This is a replica.
2020-09-12 18:28:41 >>> Stopping replication.
2020-09-12 18:28:41 >>> Stopping IO thread.
2020-09-12 18:28:41 >>> Giving the SQL thread 10 seconds to catch up.
2020-09-12 18:28:51 >>> Stopping SQL thread.
2020-09-12 18:28:51 >>> Checking for long running transactions.
2020-09-12 18:28:51 >>> There are no transactions running > 60 seconds.
2020-09-12 18:28:51 >>> innodb_max_dirty_pages_pct was 90.0.
2020-09-12 18:28:51 >>> Setting innodb_max_dirty_pages_pct to 0.
2020-09-12 18:28:51 >>> Checking dirty pages. The starting count is 6.
2020-09-12 18:28:51 >>> Dirty pages is 6, waiting (up to 1 minute) for it to get lower.
2020-09-12 18:28:52 >>> Dirty pages is 1, waiting (up to 1 minute) for it to get lower.
2020-09-12 18:28:53 >>> Dirty pages is 0.
2020-09-12 18:28:53 >>> Setting innodb_fast_shutdown to 0.
2020-09-12 18:28:53 >>> Setting innodb_buffer_pool_dump_at_shutdown to ON.
2020-09-12 18:28:53 >>> Setting innodb_buffer_pool_dump_pct to 75.
2020-09-12 18:28:53 >>> [ COMPLETED ] MySQL is prepared for shutdown!
```

In this example, the tool is gracefully aborting due to a transaction found running > 60 seconds. The transaction should be committed, rolled back, or killed. Otherwise, pass in the option `--no-transaction-check` to bypass it (it will just be rolled back when MySQL shuts down).

```
# ./crp-prepare-shutdown.py --verbose
2020-09-12 18:36:09 >>> [ START ] Preparing MySQL for shutdown.
2020-09-12 18:36:09 >>> This is a replica.
2020-09-12 18:36:09 >>> Stopping replication.
2020-09-12 18:36:09 >>> Stopping IO thread.
2020-09-12 18:36:09 >>> Giving the SQL thread 10 seconds to catch up.
2020-09-12 18:36:19 >>> Stopping SQL thread.
2020-09-12 18:36:19 >>> Checking for long running transactions.
+--------+---------------------+----------------------+----------------+----------+-----------+---------+------+---------+
| trx_id |     trx_started     | trx_duration_seconds | processlist_id |   user   |    host   | command | time | info_25 |
+--------+---------------------+----------------------+----------------+----------+-----------+---------+------+---------+
|  3932  | 2020-09-12 13:34:50 |         169          |       14       | msandbox | localhost |  Sleep  |  15  |   None  |
+--------+---------------------+----------------------+----------------+----------+-----------+---------+------+---------+
2020-09-12 18:36:19 >>> [ WARNING ] Restarting replication. There was either a problem or you aborted.
2020-09-12 18:36:19 >>> [ CRITICAL ] Transaction(s) found running > 60 seconds. COMMIT, ROLLBACK, or kill them. Otherwise, use the less safe --no-transaction-check.
```
## Options

###### --no-transaction-check/-t

**Type:** None

**Description:** Do not check for transactions running > 60 seconds. By default, the tool will gracefully abort if this condition is met. Long running transactions must be committed, rolled back, or killed. Otherwise, use this option to bypass the transaction check altogether (not recommended).

###### --verbose/-v

**Type:** None

**Description:** Print additional information while the tool is running.

## Connection Options

By default, the tool will always try to read `~/.my.cnf` to set connection options. Options in `~/.my.cnf` will be overwritten by any command line options. However, if a configuration file is specified in `--defaults-file` then only connection options within will be used (`~/.my.cnf` and command line options will be ignored).

###### --ask-pass

**Type:** string

**Description:** Get a prompt to enter the MySQL password to connect with. Also see the less safe --password.

###### --defaults-file

**Type:** string (path)

**Description:** The absolute path to a MySQL configuration file to connect to MySQL with.

###### --host/-H

**Type:** string

**Default:** `127.0.0.1`

**Description:** The MySQL host to connect to.

###### --password/-p

**Type:** string

**Description:** The MySQL password to connect with. It is recommended to use a `.my.cnf` file or --ask-pass instead.

###### --port/-P

**Type:** int

**Default:** `3306`

**Description:** The MySQL port to connect on.

###### --socket/-S

**Type:** str

**Description:** The MySQL socket to connect with.

###### --user/-u

**Type:** string

**Description:** The MySQL user to connect with. Required privileges are [SYSTEM_VARIABLES_ADMIN](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_system-variables-admin).
