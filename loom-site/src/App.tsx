const githubUrl = 'https://github.com/JeremyMColegrove/loom'

const loomWatchScript = `# process_orders.loom
@watch("./incoming")
  >> @read
  >> @csv
  >> @filter(row => row["status"] == "paid")
  >> @map(row => {
       id: row["order_id"],
       amount: to_number(row["total"]),
       currency: row["currency"],
       received_at: now(),
       source: "web"
     })
  >> @atomic("./out/paid_orders.json")

on_fail(err) {
  err
    >> @format("[order-pipeline] {message}")
    >> "./logs/order-errors.log"
}`

const pythonWatcherEquivalent = `# process_orders.py
from __future__ import annotations

import csv
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
from watchdog.observers import Observer

INCOMING = Path("./incoming")
OUTPUT = Path("./out/paid_orders.json")
LOG_FILE = Path("./logs/order-errors.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [order-pipeline] %(levelname)s %(message)s",
)

class OrderWatcher(FileSystemEventHandler):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_run = 0.0

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory:
            self._maybe_process(Path(event.src_path))

    def on_modified(self, event: FileModifiedEvent) -> None:
        if not event.is_directory:
            self._maybe_process(Path(event.src_path))

    def _maybe_process(self, file_path: Path) -> None:
        if file_path.suffix.lower() != ".csv":
            return

        with self._lock:
            now = time.monotonic()
            # Debounce bursts from editor/write events.
            if now - self._last_run < 0.25:
                return
            self._last_run = now

        try:
            paid_rows: list[dict[str, Any]] = []
            with file_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("status") != "paid":
                        continue

                    paid_rows.append(
                        {
                            "id": row.get("order_id"),
                            "amount": float(row.get("total", 0.0)),
                            "currency": row.get("currency", "USD"),
                            "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "source": "web",
                        }
                    )

            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            temp_output = OUTPUT.with_suffix(".json.tmp")
            temp_output.write_text(json.dumps(paid_rows, indent=2), encoding="utf-8")
            temp_output.replace(OUTPUT)

            logging.info("Processed %s paid orders from %s", len(paid_rows), file_path.name)
        except Exception:
            logging.exception("Failed to process %s", file_path)


def main() -> None:
    INCOMING.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    watcher = OrderWatcher()
    observer = Observer()
    observer.schedule(watcher, str(INCOMING), recursive=False)
    observer.start()

    logging.info("Watching %s for CSV changes", INCOMING)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down watcher")
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()`

const eventLog = [
  '[watch] incoming/orders-1820.csv',
  '[csv] rows: 240',
  '[filter] paid: 198',
  '[atomic] out/paid_orders.json',
  '[ok] pipeline completed in 38ms',
]

const features = [
  {
    title: 'Direct Pipelines',
    description: 'Transform data in the order you read it. No framework ceremony, no hidden lifecycle.',
  },
  {
    title: 'File-Native Runtime',
    description: 'Watch directories, stream through typed transforms, and commit output atomically.',
  },
  {
    title: 'Built For Shipping',
    description: 'Rust runtime performance with script-level readability and LSP support for daily usage.',
  },
]

export function App() {
  return (
    <main className="site-shell">
      <div className="aurora" aria-hidden="true" />
      <div className="grid-glow" aria-hidden="true" />

      <div className="layout">
        <header className="topbar">
          <div className="brand">loom</div>
          <a className="button button-ghost" href={githubUrl} target="_blank" rel="noreferrer">
            GitHub
          </a>
        </header>

        <section className="hero">
          <div className="hero-copy">
            <p className="eyebrow">Dev-first workflow language</p>
            <h1>Automate file pipelines with less code, more control.</h1>
            <p className="subtitle">
              Loom gives you shell-friendly scripts for watching directories, transforming records, and shipping reliable
              outputs without stitching together half a dozen Python modules.
            </p>
            <div className="hero-actions">
              <a className="button button-solid" href="#comparison">
                See Loom vs Python
              </a>
              <a className="button button-ghost" href="#start">
                Quick Start
              </a>
            </div>
            <div className="hero-meta">
              <span>Readable scripts</span>
              <span>Atomic writes</span>
              <span>Rust runtime</span>
            </div>
          </div>

          <aside className="hero-panel" aria-label="Pipeline terminal preview">
            <div className="terminal-head">
              <span className="dot red" />
              <span className="dot amber" />
              <span className="dot green" />
              <strong>pipeline.loom</strong>
            </div>
            <pre className="hero-code"><code>{loomWatchScript}</code></pre>
            <ul className="event-feed">
              {eventLog.map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          </aside>
        </section>

        <section className="feature-grid" aria-label="Loom capabilities">
          {features.map((item) => (
            <article key={item.title} className="feature-card">
              <h2>{item.title}</h2>
              <p>{item.description}</p>
            </article>
          ))}
        </section>

        <section id="comparison" className="compare">
          <div className="section-head">
            <p className="eyebrow">Real watcher comparison</p>
            <h2>Same job. Less moving parts in Loom.</h2>
            <p>
              Both examples watch a directory and process paid orders. The Python version is valid and realistic, but
              the operational overhead is significantly higher.
            </p>
          </div>

          <div className="compare-grid">
            <article className="code-card">
              <div className="card-head">
                <h3>Loom</h3>
                <span>single pipeline script</span>
              </div>
              <pre><code>{loomWatchScript}</code></pre>
            </article>

            <article className="code-card">
              <div className="card-head">
                <h3>Python (watchdog)</h3>
                <span>full directory watcher implementation</span>
              </div>
              <pre><code>{pythonWatcherEquivalent}</code></pre>
            </article>
          </div>
        </section>

        <section id="start" className="start">
          <div className="section-head">
            <p className="eyebrow">Quick start</p>
            <h2>Build and run in under two minutes.</h2>
          </div>
          <pre className="command-block"><code>{`# from repo root
cargo build --release

# run a script
./target/release/loom my_script.loom

# launch language server mode
./target/release/loom --lsp`}</code></pre>
        </section>

        <footer className="footer">
          <p>Built for people who want scripting speed with production discipline.</p>
          <a href={githubUrl} target="_blank" rel="noreferrer">
            {githubUrl}
          </a>
        </footer>
      </div>
    </main>
  )
}
