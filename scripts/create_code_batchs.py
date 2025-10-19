from openai import OpenAI
import os
from dotenv import load_dotenv
from pathlib import Path
import csv
from tqdm import tqdm 
from lib.error_logs import log_err

def create_csv():
    path = Path("logs/code_batchs.csv")
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["source_file", "file_id", "batch_id", "status", "endpoint", "completion_window"])

def already_recorded(filename: str) -> bool:
    path = Path("logs/code_batchs.csv")
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("source_file") == filename:
                return True
    return False

def record_batch(batchname,batch,upload):
    path = Path("logs/code_batchs.csv")
    with path.open("a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    batchname,
                    upload.id,
                    batch.id,
                    "/v1/chat/completions",
                    "24h"
                ])

def main():
    create_csv()
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)
    
    batchs = os.listdir("data/code_jobs")
    batchs = sorted([p for p in Path("data/code_jobs").iterdir()])
    
    for batch in tqdm(batchs[20:]):
        if already_recorded(batch.name):
            print("already_recorded")
            continue
        
        try:
            upload = client.files.create(
                    file=open(batch, "rb"),
                    purpose="batch"
                )
            
            created_batch = client.batches.create(
                    input_file_id=upload.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",           
                    metadata={"source_file": batch.name}
                )
            
            record_batch(batch.name,created_batch, upload)
        except Exception as e:
            print(e)
            log_err("errors_batch_code.json", str(batch.name))
    
if __name__=="__main__":
    main()