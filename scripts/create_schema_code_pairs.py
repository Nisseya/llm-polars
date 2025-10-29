import os
from pathlib import Path
import json
import re 
from tqdm import tqdm

def main():
    question_files = sorted(os.listdir("data/questions"))[20:]
    code_files = sorted(os.listdir("data/code"))
    
    filenames = get_mapped_filenames()
    
    with open("data/schemas.json","r", encoding="utf-8") as f:
        schemas = json.load(f)
        
    with open("data/file_code_pair.jsonl","w",encoding="utf-8") as final_file:
        
        for question_file, code_file in tqdm(zip(question_files, code_files)):
            
            with open(Path("data/questions",question_file),"r") as f:
                questions = [json.loads(line) for line in f if line.strip()]
                
            with open(Path("data/code",code_file)) as f:
                codes = [json.loads(line) for line in f if line.strip()]
                
            filename = get_filename(questions[0], filenames, schemas)
            
            
            for question, code in zip(questions, codes):
                code_content = str(code["response"]["body"]["choices"][0]["message"]["content"])
                entry ={
                        "filename": filename,
                        "code": code_content
                }
                final_file.write(json.dumps(entry, ensure_ascii=False) + "\n")
                

            
def get_filename(question, filenames, schemas):
    question_filename_raw = question["custom_id"]
    question_filename = "_".join(question_filename_raw.split("_")[3:])
    filename = filenames[question_filename]
    return filename

def get_question_paths():
    return os.listdir("data/questions")

def get_mapped_filenames():
    filenames = [filename for filename in os.listdir("data/sets") if filename.endswith(".csv")]
    map_filename_safe = {re.sub(r'[^a-zA-Z0-9_-]', '_', filename): filename for filename in filenames}
    return map_filename_safe
    

if __name__=="__main__":
    main()