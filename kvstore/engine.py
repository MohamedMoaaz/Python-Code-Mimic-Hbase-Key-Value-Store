import os
from pathlib import Path
import json
import time
import uuid

class KVEngine:
    MAX_MEM_SIZE = 1024 * 1024

    def __init__(self):
        self.current_ns = None
        self.current_namespace = None
        self.kv_root = Path(os.getcwd()) / "kvstore"
        self.wal_dir = self.kv_root / "wal"
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        self.memstore = {}

    def _write_to_wal(self, operation: str, table: str, key: str, value: str = None, ttl: float = 0):
        """Write operation to WAL file"""
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")

        # Create WAL entry
        wal_entry = {
            "timestamp": time.time(),
            "operation": operation,
            "namespace": self.current_namespace,
            "table": table,
            "key": key,
            "value": value,
            "ttl": ttl
        }

        # Write to WAL file
        wal_file = self.wal_dir / f"{self.current_namespace}_{table}_{uuid.uuid4()}.wal"
        with open(wal_file, "w") as f:
            json.dump(wal_entry, f, indent=4)

    def namespace_exists(self, ns: str) -> bool:
        return (self.kv_root / ns).is_dir()

    def list_namespaces(self):
        return [ns.name for ns in self.kv_root.iterdir() if ns.is_dir()]

    def create_namespace(self, ns: str) -> str:
        ns_path = self.kv_root / ns
        if ns_path.exists():
            return f"[ERROR] Namespace '{ns}' already exists."
        ns_path.mkdir()
        return f"[OK] Namespace '{ns}' created successfully."

    def use_namespace(self, ns: str) -> str:
        if not self.namespace_exists(ns):
            return f"[ERROR] Namespace '{ns}' does not exist."
        self.current_namespace = ns
        return f"[OK] Using namespace: {ns}"

    def table_exists(self, ns: str, table: str) -> bool:
        return (self.kv_root / ns / table).is_dir()

    def create_table(self, ns: str, table: str) -> str:
        ns_path = self.kv_root / ns
        if not ns_path.exists():
            return f"[ERROR] Namespace '{ns}' does not exist."

        table_path = ns_path / table
        if table_path.exists():
            return f"[ERROR] Table '{table}' already exists in namespace '{ns}'."

        table_path.mkdir()
        return f"[OK] Table '{table}' created in namespace '{ns}'."

    def list_tables(self, ns: str):
        ns_path = self.kv_root / ns
        if not ns_path.exists():
            return f"[ERROR] Namespace '{ns}' does not exist."
        
        return [tbl.name for tbl in ns_path.iterdir() if tbl.is_dir()]

    def set_key(self, table: str, key: str, value: str, ttl: float = 0):
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")
        if not self.table_exists(self.current_namespace, table):
            raise FileNotFoundError(f"Table '{table}' does not exist in namespace '{self.current_namespace}'.")

        # Write to WAL first
        self._write_to_wal("SET", table, key, value, ttl)

        table_id = f"{self.current_namespace}:{table}"
        self.memstore.setdefault(table_id, {})
        self.memstore[table_id].setdefault(key, [])

        self.memstore[table_id][key].append({
            "value": value,
            "timestamp": time.time(),
            "ttl": ttl
        })

    def get_key(self, table: str, key: str):
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")

        table_id = f"{self.current_namespace}:{table}"
        table_path = self.kv_root / self.current_namespace / table

        # First check memstore
        if table_id in self.memstore and key in self.memstore[table_id]:
            versions = self.memstore[table_id][key]
            for entry in reversed(versions):  # Newest first
                if entry["ttl"] == 0 or time.time() <= entry["timestamp"] + entry["ttl"]:
                    if entry["value"] == "_DEL":
                        return None
                    return entry["value"]

        # If not in memstore, search in table files
        if table_path.exists():
            all_versions = []
            
            # Search in all JSON files in the table directory
            for file_path in table_path.glob("*.json"):
                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                        if key in data:
                            all_versions.extend(data[key])
                except Exception as e:
                    print(f"[WARN] Error reading {file_path}: {e}")
                    continue

            # Sort all versions by timestamp
            all_versions.sort(key=lambda x: x["timestamp"])

            # Check versions from newest to oldest
            for entry in reversed(all_versions):
                if entry["ttl"] == 0 or time.time() <= entry["timestamp"] + entry["ttl"]:
                    if entry["value"] == "_DEL":
                        return None
                    return entry["value"]

        return None  # Key not found or all versions expired
    
    def delete_key(self, table: str, key: str):
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")
        if not self.table_exists(self.current_namespace, table):
            raise FileNotFoundError(f"Table '{table}' does not exist in namespace '{self.current_namespace}'.")

        # Write to WAL first
        self._write_to_wal("DELETE", table, key)

        table_id = f"{self.current_namespace}:{table}"
        if table_id not in self.memstore:
            self.memstore[table_id] = {}
        
        # Mark the key as deleted by adding a _DEL version
        self.memstore[table_id].setdefault(key, []).append({
            "value": "_DEL",
            "timestamp": time.time(),
            "ttl": 0
        })
        return f"[OK] Marked key '{key}' as deleted in table '{table}'."
    
    def flush_table(self, table: str):
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")
        if not self.table_exists(self.current_namespace, table):
            raise FileNotFoundError(f"Table '{table}' does not exist in namespace '{self.current_namespace}'.")

        table_id = f"{self.current_namespace}:{table}"
        if table_id not in self.memstore:
            return "[WARN] Nothing to flush."

        # Write to WAL first
        self._write_to_wal("FLUSH", table, "FLUSH")

        table_path = self.kv_root / self.current_namespace / table / f"{int(time.time())}_flush.json"
        table_path.parent.mkdir(parents=True, exist_ok=True)

        with open(table_path, "w") as f:
            json.dump(self.memstore[table_id], f, indent=4)

        del self.memstore[table_id]  # clear flushed data
        return f"[OK] Flushed {table_id} to {table_path.name}"
    
    def compact_table(self, table: str):
        if self.current_namespace is None:
            raise ValueError("No namespace selected. Use 'use-namespace' first.")
        if not self.table_exists(self.current_namespace, table):
            raise FileNotFoundError(f"Table '{table}' does not exist in namespace '{self.current_namespace}'.")

        table_path = self.kv_root / self.current_namespace / table
        if not table_path.exists():
            return "[WARN] No files to compact."

        # Read all flush files
        merged_data = {}
        for file_path in table_path.glob("*_flush.json"):
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                    for key, versions in data.items():
                        if key not in merged_data:
                            merged_data[key] = []
                        merged_data[key].extend(versions)
            except Exception as e:
                print(f"[WARN] Error reading {file_path}: {e}")
                continue

        # Clean up data - keep only latest non-deleted versions
        cleaned_data = {}
        for key, versions in merged_data.items():
            # Sort versions by timestamp
            versions.sort(key=lambda x: x["timestamp"])
            
            # Keep only the latest non-deleted version
            latest_valid = None
            for version in versions:
                if version["value"] != "_DEL" and (version["ttl"] == 0 or time.time() <= version["timestamp"] + version["ttl"]):
                    latest_valid = version
            
            if latest_valid:
                cleaned_data[key] = [latest_valid]

        # Write compacted data to new file
        if cleaned_data:
            new_file = table_path / f"{int(time.time())}_compacted.json"
            with open(new_file, "w") as f:
                json.dump(cleaned_data, f, indent=4)

            # Remove old files
            for file_path in table_path.glob("*_flush.json"):
                try:
                    file_path.unlink()
                except Exception as e:
                    print(f"[WARN] Error removing {file_path}: {e}")

            return f"[OK] Table '{table}' compacted successfully. New file: {new_file.name}"
        else:
            return "[WARN] No valid data to compact."
    
if __name__ == "__main__":
    engine = KVEngine()

    print(engine.create_namespace("test_ns"))
    print(engine.use_namespace("test_ns"))
    print(engine.create_table("test_ns", "test_table"))
    print(engine.set_key("test_table", "key1", "value1"))
    print(engine.get_key("test_table", "key1"))
    print(engine.set_key("test_table", "key1", "value2", ttl=5))
    time.sleep(6)
    print(engine.get_key("test_table", "key1"))
    print(engine.delete_key("test_table", "key1"))
    print(engine.get_key("test_table", "key1"))
    print(engine.flush_table("test_table"))
    print(engine.list_tables("test_ns"))
