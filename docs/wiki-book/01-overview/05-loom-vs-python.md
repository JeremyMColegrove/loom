# Loom vs Python

Python is excellent and very flexible. Loom is intentionally narrower.

The difference is not "good vs bad." It is "general-purpose vs purpose-built."

## Critical syntax difference

In Python, `"..."` is a normal string.

In Loom, `"..."` is a path literal with file-oriented semantics.
- In call arguments, Loom reads that file and passes file contents.
- To pass literal text, Loom uses escaped strings: `\"..."`.

Example:

```loom
@secret("API_KEY_FILE")   // read file API_KEY_FILE; contents become lookup key
@secret(\"API_KEY\")      // literal key name API_KEY
```

## Example: Parse CSV and split outputs

Loom:

```loom
@read(\"./inbox/orders.csv") >> @csv.parse as data >> [
    filter(row >> row.amount > 1000) >> "./out/high-value.csv",
    filter(row >> row.amount <= 1000) >> "./out/normal.csv"
]
```

Python (rough equivalent):

```python
import csv
from pathlib import Path

in_path = Path(\"./inbox/orders.csv")
high_path = Path(\"./out/high-value.csv")
normal_path = Path(\"./out/normal.csv")

with in_path.open(newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

high = [r for r in rows if float(r.get(\"amount", 0)) > 1000]
normal = [r for r in rows if float(r.get(\"amount", 0)) <= 1000]

headers = rows[0].keys() if rows else ["amount"]

for path, dataset in [(high_path, high), (normal_path, normal)]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(\"w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(dataset)
```

Both work. Loom is shorter because parsing + filtering + routing is the center of the language.

## Example: Failure handling

Loom has built-in flow recovery:

```loom
@read(\"./inbox/orders.csv") >> @csv.parse >> "./out/clean.csv" on_fail as err >> [
    \"failed: " + err >> "./logs/errors.log",
    "./inbox/orders.csv" -> "./quarantine/"
]
```

Python can do the same, but you write explicit `try/except`, error formatting, and move logic by hand.

## Decision guide

Use Loom when:
- The job is mostly file/data flow.
- You want readable pipeline scripts.
- You want security controls around file actions.

Use Python when:
- You need broad libraries or complex non-pipeline logic.
- You are building larger general applications.
