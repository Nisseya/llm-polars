from openai import OpenAI
from dotenv import load_dotenv
import os 
import polars as pl
from typing import List

def main():
    batchs = get_jobs()
    client = get_openai_client()
    
    for i,batch in enumerate(batchs):
        job = client.batches.retrieve(batch_id=batch)
        if getattr(job,"status")!="completed":
            continue #should be logging but idc now
        download_code(client,getattr(job,"output_file_id"),i)
        

def download_code(client, file_id,i):
    content = client.files.content(file_id).content
    with open(f"data/code/code{i}.jsonl", "wb") as f:
        f.write(content)
    
def get_openai_client()->OpenAI:
    load_dotenv()
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_jobs()->List[str]:
    return pl.read_csv("logs/code_batchs.csv").select("batch_id").to_series().to_list()
    
    
if __name__=="__main__":
    main()