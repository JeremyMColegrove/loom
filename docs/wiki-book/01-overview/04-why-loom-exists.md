# Why Loom Exists

You made Loom to solve a practical gap: there are many scripting languages, but there is no simple, focused language that feels natural for complex file handling tasks.

Typical pain points in file scripts:
- Too much boilerplate around file IO and parsing.
- Hard-to-read control flow spread across functions and callbacks.
- Error handling added late and inconsistently.
- Security policy often bolted on instead of built in.

Loom addresses this by making file pipelines first-class.

## Design goals behind Loom

- Lightweight: small syntax, fast to learn.
- Readable: left-to-right data flow.
- Safe by default: explicit policy and trust modes.
- Practical: built-ins for common pipeline tasks.

## The key tradeoff

Loom intentionally narrows scope.

It is stronger for file pipeline clarity, and weaker for broad “do everything” scripting. This tradeoff is by design.
