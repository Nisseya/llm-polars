import os 
import json
from pathlib import Path
import re

def main():
    jsonl_question_paths = get_question_paths()
    
    filenames = get_mapped_filenames()
    
    with open("data/Schemas.json","r", encoding="utf-8") as f:
        schemas = json.load(f)
    
    for i,jsonl_path in enumerate(jsonl_question_paths):
        with open(Path("data/questions",jsonl_path), "r") as f:
            questions = [json.loads(line) for line in f if line.strip()]
            
        schema = get_schema(questions[0], filenames, schemas)
        if schema==None:
            continue
        
        with open(f"data/code_jobs/batch_{i}.jsonl","w",encoding="utf-8") as f:
            for j,question in enumerate(questions):
                prompt = get_system_prompt(schema)
                entry = {
                            "custom_id": f"schema_{i}_{j}",
                            "method": "POST",
                            "url": "/v1/chat/completions",
                            "body": {
                                "model": "gpt-5-mini",
                                "messages": [
                                    {"role": "system", "content": prompt},
                                    {"role": "user", "content": str(question["response"]["body"]["choices"][0]["message"]["content"])},
                                ]
                            },
                        }

                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                
def get_mapped_filenames():
    filenames = [filename for filename in os.listdir("data/sets") if filename.endswith(".csv")]
    map_filename_safe = {re.sub(r'[^a-zA-Z0-9_-]', '_', filename): filename for filename in filenames}
    return map_filename_safe

def get_system_prompt(schema) -> str:
    return (
        "You are an assistant that outputs ONLY executable Polars Lazy code for data analysis.\n"
        "\n"
        "CONTEXT & CONTRACT:\n"
        "- The input is a natural-language analytics request (business-style) and optionally a dataset schema.\n"
        "- A LazyFrame named df ALREADY EXISTS. Do not import anything, do not create df; just transform df.\n"
        "- Use Polars 1.33 stable. Use the LAZY engine only. No Pandas, no NumPy, no Python loops.\n"
        "- Output ONLY the minimal necessary code. NO comments, NO explanations, NO backticks.\n"
        "- Call .collect() ONE time at the very end. Before that, keep everything lazy.\n"
        "- Prefer method-chaining and pure expressions. Keep column names stable and descriptive.\n"
        "\n"
        "DATA HANDLING RULES:\n"
        "- Types: cast explicitly when needed (e.g., pl.col('Date').str.strptime(pl.Date, strict=False)).\n"
        "- Missing/zero-safe math: guard divisions with pl.when(condition).then(...).otherwise(None) and fill_null if required.\n"
        "- Dates: sort by date before window ops. For daily series per entity, group by entity, then apply .rolling_* or .group_by_rolling as appropriate.\n"
        "- Rolling windows: compute by entity (e.g., .group_by('Prefecture').agg(pl.col('x').rolling_mean(window_size=7))).\n"
        "- Joins: choose the safest join for retention:\n"
        "  • When enriching a daily fact table with static dimensions (e.g., population), use left join on the fact table key(s).\n"
        "  • When the dimension is slowly changing, join on keys + effective version/date using join_asof (backward) or an appropriate interval logic.\n"
        "- Missing dates in a panel/time-series: build a complete date grid per entity and left-join to ensure gaps appear; then fill_null(0) only for count-like measures if the intent is 'no report means zero'.\n"
        "- Column hygiene: trim/standardize identifiers as needed, but do not fabricate data. If a standardization mapping is required, use a small inline when/then map or string ops.\n"
        "\n"
        "EXPECTED OUTPUT SHAPE:\n"
        "- If the user asks to 'produce a merged dataset' or 'calculate derived columns', return a transformed lazy pipeline ending with .collect().\n"
        "- If multiple steps are needed (join, derive, filter, roll), keep them in a single chain. Avoid temporary materialization.\n"
        "- Do not print, do not return Python variables; just end with .collect().\n"
        "\n"
        "PATTERNS TO PREFER:\n"
        "- with_columns for derived metrics (e.g., rates per 100k, occupancy %).\n"
        "- when/then/otherwise for safe divisions (avoid divide-by-zero).\n"
        "- join / join_asof with explicit how= and on= (and by= for asof if per-entity).\n"
        "- group_by([...]).agg([...]) for aggregations; ensure sorting for time-aware ops.\n"
        "- complete calendar scaffold when needed using a generated date range per group and a left join.\n"
        "\n"
        "OUTPUT FORMAT:\n"
        "- Return ONLY Polars Lazy code operating on df and ending with .collect(). No comments, no markdown, no prose.\n"
        "- If the task requires additional inputs (e.g., a dimension LazyFrame dim), assume it already exists as dim.\n"
        "- Never use scan_* or imports; df and any referenced LazyFrames already exist.\n"
        
        "HERE IS THE SCHEMA OF THE LAZYFRAME:\n"
        f"{schema}"
    )
    
def get_schema(question, filenames, schemas):
    print(isinstance(question,dict))
    question_filename_raw = question["custom_id"]
    question_filename = "_".join(question_filename_raw.split("_")[3:])
    filename = filenames[question_filename]
    schema = next((s["schema"] for s in schemas if s["file"] == filename), None)
    return schema

def get_question_paths():
    return os.listdir("data/questions")

if __name__=="__main__":
    main()