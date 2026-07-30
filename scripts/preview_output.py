"""Separate preview script — avoids heredoc/indentation issues in the workflow YAML."""
import json
from pathlib import Path

p = Path("./scripts/grayscale_btc.json")

if not p.exists():
    print("grayscale_btc.json not found")
    raise SystemExit(0)

print("exists:", p.exists())
print("path:", p.resolve())

with p.open("r", encoding="utf-8") as f:
    data = json.load(f)

print("success:", data.get("success"))
print("method_used:", data.get("method_used"))
print("status_code:", data.get("status_code"))
print("title:", data.get("title"))
print("html_length:", data.get("html_length"))
print("markdown_length:", data.get("markdown_length"))
print("error_message:", data.get("error_message"))
print("attempts_summary:", json.dumps(data.get("attempts_summary"), ensure_ascii=False, indent=2))
