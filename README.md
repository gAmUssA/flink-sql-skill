# 🌊 Flink SQL Skill for Claude

[![Skill Validation](https://github.com/gAmUssA/flink-sql-skill/actions/workflows/validate.yml/badge.svg)](https://github.com/gAmUssA/flink-sql-skill/actions/workflows/validate.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A Claude skill for Apache Flink SQL, Table API, and UDF development — covering both OSS Flink and Confluent Cloud.

## 🎯 What This Skill Enables

When Claude has this skill loaded, it can help you:

- **Write Flink SQL queries** — windows, joins, aggregations, MATCH_RECOGNIZE
- **Build Table API pipelines** — Java and Python
- **Create UDFs** — Scalar functions, Table functions, Process Table Functions (PTFs)
- **Deploy to Confluent Cloud** — CLI commands, compute pools, artifacts
- **Troubleshoot errors** — Common mistakes and solutions
- **Design streaming architectures** — Event-time, watermarks, state management

## 📚 Skill Contents

```
flink-sql-skill/
├── SKILL.md                          # Core patterns, quick reference (~330 lines)
└── references/
    ├── udf-guide.md                  # UDF development templates (~400 lines)
    ├── sql-patterns.md               # Advanced SQL patterns (~500 lines)
    ├── confluent-cloud.md            # Confluent-specific features (~480 lines)
    ├── ptf-guide.md                  # Process Table Functions (~725 lines)
    ├── kafka-patterns.md             # Kafka connector patterns (~450 lines)
    └── troubleshooting.md            # Common errors & fixes (~600 lines)
```

**Total:** ~3,500 lines of curated Flink knowledge

## 🚀 How to Use

### Option 1: Claude.ai Projects

1. Create a new Project in Claude.ai
2. Go to **Project Knowledge**
3. Upload `SKILL.md` and the `references/` folder contents
4. Start chatting about Flink!

### Option 2: Claude Code CLI

```bash
# Clone this repo
git clone https://github.com/gAmUssA/flink-sql-skill.git

# Reference in Claude Code
claude --skill ./flink-sql-skill/
```

### Option 3: Raw GitHub URL

Reference the skill directly:
```
https://raw.githubusercontent.com/gAmUssA/flink-sql-skill/main/SKILL.md
```

## 💡 Example Queries

Once the skill is loaded, try asking Claude:

```
"Help me write a Flink SQL query with tumbling windows to count orders per hour"

"Create a UDF that masks email addresses for GDPR compliance"

"What's the difference between interval joins and temporal joins?"

"Show me how to deploy a UDF to Confluent Cloud"

"Debug this error: Cannot generate watermark for rowtime column"
```

## 🔧 Key Patterns Covered

### SQL Patterns
- Window aggregations (TUMBLE, HOP, SESSION, CUMULATE)
- All join types (interval, temporal, lookup, window)
- Deduplication and Top-N queries
- MATCH_RECOGNIZE for pattern detection
- CDC handling with Debezium

### UDF Development
- Java ScalarFunction and TableFunction templates
- Python @udf decorator patterns
- Maven POM with shade plugin
- Confluent Cloud deployment workflow

### Process Table Functions (PTFs)
- 9 real-world examples from production use cases
- State management (ValueState, MapView, ListView)
- Timer patterns for session detection
- Changelog handling (consuming and producing CDC)
- Multiple input streams (co-partition patterns)

### Kafka Integration
- Kafka source/sink table definitions
- Avro + Schema Registry configuration
- Upsert Kafka for stateful outputs
- Flink vs Kafka Streams comparison

## 🙏 Credits & Sources

This skill incorporates patterns from:

- [MartijnVisser/flink-ptf-examples](https://github.com/MartijnVisser/flink-ptf-examples) — PTF examples
- [confluentinc/flink-table-api-java-examples](https://github.com/confluentinc/flink-table-api-java-examples) — Official Confluent examples
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-master/)
- [Confluent Cloud Flink Documentation](https://docs.confluent.io/cloud/current/flink/)

## 📖 Related Resources

- [Kafka in Action](https://www.manning.com/books/kafka-in-action) (co-authored by Viktor Gamov)
- [FLIP-440: ProcessTableFunction](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=298781093)
- [Querying Streams YouTube Series](https://www.youtube.com/@ViktorGamov)

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run validation: `python scripts/validate.py`
5. Submit a pull request

## 📄 License

Apache License 2.0 — See [LICENSE](LICENSE) for details.

---

**Maintainer:** [Viktor Gamov](https://github.com/gAmUssA) — Principal Developer Advocate at Confluent

*"Tables as movies, not photographs"* 🎬
