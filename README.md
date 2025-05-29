# 🗃️ KVStore – A Simple Filesystem-based Key-Value Store

KVStore is a lightweight key-value storage engine written in Python. It uses the local filesystem to manage namespaces, tables, and key-value pairs. It supports time-to-live (TTL), write-ahead logging (WAL), data compaction, and an interactive CLI shell.

---

## 🚀 Features

- 🔹 Namespace and table abstraction
- 🔹 In-memory storage with WAL (Write-Ahead Logging)
- 🔹 Key TTL support for expirable entries
- 🔹 Disk flush and compaction
- 🔹 Tombstone deletion
- 🔹 Interactive command-line shell
- 🔹 File-based storage using JSON

---

## 📁 Project Structure

```
kvstore/
│
├── engine.py               # Core KVEngine class
├── cli.py                  # Interactive shell interface
├── kvstore/                # Storage root directory (created at runtime)
│   ├── namespace/
│   │   └── table/
│   │       └── *.json      # Flushed and compacted data
│   └── wal/                # Write-ahead log files
```

---

## 💻 Interactive Shell

Run the CLI using:

```bash
python cli.py
```

Then use commands like:

```shell
kvstore> create-namespace userspace
kvstore> use-namespace userspace
kvstore> create-table logins
kvstore> set logins:user1:admin123 3600
kvstore> get logins:user1
kvstore> delete logins:user1
kvstore> flush logins
kvstore> compact logins
```

---

## 🧪 Supported Commands

| Command                  | Format/Example                            | Description                              |
|--------------------------|--------------------------------------------|------------------------------------------|
| `list-namespaces`        |                                            | List all namespaces                      |
| `create-namespace`       | `create-namespace myns`                   | Create a new namespace                   |
| `use-namespace`          | `use-namespace myns`                      | Switch current working namespace         |
| `list-tables`            |                                            | List tables in current namespace         |
| `create-table`           | `create-table mytable`                    | Create a new table                       |
| `set`                    | `set mytable:key:value [ttl]`             | Set a key with optional TTL              |
| `get`                    | `get mytable:key`                         | Retrieve a key's value                   |
| `delete`                 | `delete mytable:key`                      | Tombstone a key                          |
| `flush`                  | `flush mytable`                           | Write in-memory data to disk             |
| `compact`                | `compact mytable`                         | Merge and clean old flushed files        |
| `exit`                   |                                            | Exit the shell                           |

---

## 🛠 Internals

- **WAL**: Write-ahead logging ensures durability before data is stored in memory.
- **Memstore**: In-memory dictionary with versioned values per key.
- **TTL**: Values can have expiration timestamps.
- **Flush**: Dumps memstore to disk in JSON files.
- **Compaction**: Merges flushed files and removes expired/deleted entries.

---

## 🔧 Requirements

- Python 3.6+
- No external dependencies

---

## 📜 License

This project is open-source and available under the [MIT License](LICENSE).

---

## 👨‍💻 Author

Created by [Your Name] – feel free to contribute or suggest improvements!
