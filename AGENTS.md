# HappySync engineering notes

Read PLAN.md for the product specification. Real consumer NAT/public DHT/VPS acceptance is deferred by the user; local automated validation is required now. Never claim external validation from simulation.

Use subagents for independent components and coordinate shared interfaces. Prefer codebase-memory-mcp graph tools for code discovery when available; fall back to filesystem tools when unavailable. No graph connector is currently exposed.

Keep secrets out of diagnostics and status. Preserve concurrent content. Tests must verify directory content hashes and persisted recovery, not only logs. Default ignores include .git/ and node_modules/ with configurable gitignore-style rules.
