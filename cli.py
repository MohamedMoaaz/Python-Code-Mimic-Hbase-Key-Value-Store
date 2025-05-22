import argparse
import time
from kvstore.engine import KVEngine

def parse_path_key_value(string):
    parts = string.split(":")
    if len(parts) < 4:
        raise argparse.ArgumentTypeError("Expected format: namespace:table:key:value")
    return parts[0], parts[1], parts[2], ":".join(parts[3:])

def parse_path_key(string):

    parts = string.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("Expected format: namespace:table:key")
    return parts[0], parts[1], parts[2]

def parse_path(string):

    parts = string.split(":")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("Expected format: namespace:table")
    return parts[0], parts[1]

def interactive_shell():

    db = KVEngine()
    print("Entering interactive shell. Type 'exit' to quit.")
    
    while True:
        try:
            command = input("kvstore> ")
            if command.lower() == 'exit':
                print("Exiting shell...")
                break
            elif command.startswith("list-namespaces"):
                print(db.list_namespaces())
            elif command.startswith("create-namespace"):
                
                ns = command.split(" ")[1]
                if db.namespace_exists(ns):
                    print(f"[ERROR] Namespace {ns} already exists.")
                else:
                    db.create_namespace(ns)
                    print(f"[OK] Created namespace: {ns}")
            elif command.startswith("use-namespace"):
                
                ns = command.split(" ")[1]
                if db.namespace_exists(ns):
                    db.use_namespace(ns)
                    print(f"[OK] Using namespace: {ns}")
                else:
                    print(f"[ERROR] Namespace {ns} does not exist.")
            elif command.startswith("list-tables"):
                if not db.namespace_exists(db.current_namespace):
                    print(f"[ERROR] Namespace {ns} does not exist.")
                else:
                    print(db.list_tables(db.current_namespace))
            elif command.startswith("create-table"):
                parts = command.split(" ")
                tbl = parts[1]
                if not db.namespace_exists(db.current_namespace):
                    print(f"[ERROR] Namespace {db.current_namespace} does not exist.")
                elif db.table_exists(db.current_namespace, tbl):
                    print(f"[ERROR] Table {tbl} already exists in namespace {db.current_namespace}.")
                else:
                    db.create_table(db.current_namespace, tbl)
                    print(f"[OK] Created table: {db.current_namespace}:{tbl}")
            elif command.startswith("set"):
                parts = command.split()

                if len(parts) < 2:
                    print("[ERROR] Usage: set table:key:value [ttl]")
                    continue
                kv_parts = parts[1].split(":")
                if len(kv_parts) < 3:
                    print("[ERROR] Format must be: table:key:value [ttl]")
                    continue

                table = kv_parts[0]
                key = kv_parts[1]
                value = ":".join(kv_parts[2:])  
                ttl = int(parts[2]) if len(parts) > 2 else time.time()

                try:
                    db.set_key(table, key, value, ttl)
                    print(f"[OK] Set {key} in {db.current_namespace}:{table}")
                except Exception as e:
                    print(f"[ERROR] {e}")
            elif command.startswith("get"):
                tbl, k = command.split(" ")[1].split(":")
                val = db.get_key(tbl, k)
                if val is None:
                    print(f"[MISS] {k} not found in {db.current_namespace}:{tbl}")
                else:
                    print(f"[HIT] {db.current_namespace}:{tbl}:{k} = {val}")
            elif command.startswith("delete"):
                tbl, k = command.split(" ")[1].split(":")
                db.delete_key(tbl, k)
                print(f"[OK] Deleted {k} from {db.current_namespace}:{tbl} (tombstone added)")
            elif command.startswith("flush"):
                tbl = command.split(" ")[1]
                db.flush_table(tbl)
                print(f"[OK] Flushed {db.current_namespace}:{tbl} to disk.")
            elif command.startswith("compact"):
                tbl = command.split(" ")[1]
                db.compact_table(tbl)
                print(f"[OK] Compacted {db.current_namespace}:{tbl} into one file.")
            else:
                print("Unknown command. Try again.")
        except Exception as e:
            print(f"[ERROR] {e}")




if __name__ == "__main__":
    interactive_shell()