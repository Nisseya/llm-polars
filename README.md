# 🧠 Polars Code Generation Dataset Builder

This repository automates the creation of a **synthetic dataset** that maps *natural-language questions* to **executable Polars LazyFrame code**.  
It is designed for training and benchmarking **LLMs specialized in data manipulation, analytics, and ETL automation**.

---

## 🎯 Project Purpose

Modern data engineers and analysts often describe analytical goals in plain language ("calculate the average per product"), while the machine needs executable code.  
This project builds a bridge between the two: **it creates a large corpus of realistic questions and their corresponding Polars code answers**.

The resulting dataset can be used to:
- Fine-tune open models for **data-to-code generation** (Polars, Pandas, SQL).
- Train **RAG pipelines** for analytics assistants.
- Evaluate model understanding of **data schema semantics** and **analytical intent**.
- Generate few-shot examples for prompt engineering or model alignment.

---

## 🧩 Methodology

The pipeline follows a modular and reproducible workflow — each step corresponds to a script in this repository.

### 1️⃣ Dataset Acquisition (Realistic Data Context)
- Reads a `data/list.json` containing Kaggle dataset references.
- Downloads and extracts datasets to `data/sets/`.
- Each dataset represents a real-world structure: sales, healthcare, demographics, etc.

### 2️⃣ Schema Extraction
- Each CSV is analyzed using **Polars** with encoding and separator auto-detection.
- For each column, we store:
  - Name
  - Data type
  - Three distinct non-null sample values
- This schema (in `data/schemas.json`) is what GPT sees to generate contextual questions.

### 3️⃣ Natural-Language Question Generation
- For every dataset schema, GPT-5-mini generates 50 natural-language questions.
- Styles vary: from beginner to analyst to business stakeholder.
- Each question touches on different analytical skills (aggregation, joins, cleaning, ratios...).
- Results are stored in `data/questions/`.

### 4️⃣ Code Generation (Polars Lazy)
- The generated questions are then fed again to GPT, this time with a **system prompt** enforcing strict Polars Lazy syntax:
  - No imports
  - No print
  - One `.collect()` at the end
  - Idiomatic use of `.with_columns()`, `.group_by()`, `.join_asof()` etc.
- Output code is stored in `data/code/`.

### 5️⃣ Pairing and Structuring
- Each question-code pair is merged into fine-tuning-ready JSONL format.
- Metadata like filename and schema are preserved.
- Two main outputs:
  - `data/pairs.jsonl` (for chat-based fine-tuning)
  - `data/file_code_pair.jsonl` (for retrieval-based use)

---

## 📦 Folder Structure

```
data/
 ├─ sets/              # Extracted Kaggle CSVs
 ├─ schemas.json       # Auto-inferred schemas (used as model context)
 ├─ question_jobs/     # GPT batch requests for question generation
 ├─ code_jobs/         # GPT batch requests for code generation
 ├─ questions/         # Model outputs (questions)
 ├─ code/              # Model outputs (Polars code)
 ├─ pairs.jsonl        # Final question→code pairs
 └─ file_code_pair.jsonl # Version with file mapping
logs/
 ├─ question_batchs.csv
 ├─ code_batchs.csv
 └─ errors_*.json
```

---

## ⚙️ Step-by-Step Workflow

| Step | Script | Description |
|------|---------|-------------|
| 1 | `download_datasets.py` | Download Kaggle datasets listed in `data/list.json` |
| 2 | `get_schemas.py` | Infer schemas (column name, dtype, samples) |
| 3 | `generate_jsonl_questions.py` | Build GPT prompts for realistic user questions |
| 4 | `create_question_batchs.py` | Upload question batches to OpenAI |
| 5 | `download_questions.py` | Download GPT-generated questions |
| 6 | `generate_jsonl_code.py` | Build GPT prompts for Polars code |
| 7 | `create_code_batchs.py` | Upload code batches to OpenAI |
| 8 | `download_code.py` | Download code completions |
| 9 | `create_pairs.py` | Merge question/code into fine-tuning JSONL |
| 10 | `create_schema_code_pairs.py` | Variant pairing preserving filenames |

---

## 🧠 Example Output

**User Question:**
> Could you show the top 5 regions by total revenue, excluding rows with missing sales?

**Schema:**
```json
[
 {"name": "Region", "dtype": "Utf8", "non_null_sample": ["North","West","South"]},
 {"name": "Sales", "dtype": "Float64", "non_null_sample": [230.5, 189.2, 401.0]}
]
```

**Generated Polars Code:**
```python
df.drop_nulls("Sales")  .group_by("Region")  .agg(pl.col("Sales").sum().alias("total_sales"))  .sort("total_sales", descending=True)  .head(5)  .collect()
```

---

## 🧪 Training Usage

You can directly fine-tune a chat model or use the data for evaluation:

```bash
openai api fine_tunes.create -t data/pairs.jsonl -m gpt-4o-mini
```

Or for retrieval-augmented inference:

```python
from datasets import load_dataset
ds = load_dataset("path/to/pairs.jsonl")
```

---

## 🔍 Insights & Research Potential

This project allows the study of:
- How models interpret schemas in NL-to-code generation.
- Error propagation between natural question → code synthesis.
- Synthetic-to-real domain transfer for data engineering LLMs.
- The evolution of code quality across prompt styles or subjects.

By automating every step, it provides **a reproducible framework** for building and scaling data-centric training datasets.

---

## 🧠 Future Work

- Add **execution verification** (run generated Polars code to validate syntax).  
- Expand to **SQL and Pandas** code variants.  
- Add **synthetic dataset augmentation** for schema diversity.  
- Integrate **self-evaluation and scoring** of code correctness.

---

## 📜 License

MIT © 2025 Yassine Hadi  
Use freely for research, dataset generation, and model training.
