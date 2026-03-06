# Who This Book Is For

This book is for people doing real file automation work.

## Primary audience

- Developers who build ingestion and ETL-like scripts.
- Platform or DevOps engineers maintaining file workflows.
- Teams migrating from ad-hoc scripts to a clearer pipeline model.

## Good fit scenarios

Loom is a good fit when:
- The main work is reading, parsing, filtering, and writing files.
- You want predictable syntax and explicit flow.
- You need policy-based controls around read/write/import/watch actions.

## When Loom may not be the best fit

Loom is not trying to replace full application frameworks. If your task is mostly UI, HTTP services, or complex in-memory algorithms, a general-purpose language may still be a better center.

Many teams use Loom for pipeline orchestration and Python/Rust/Go for specialized heavy logic.
