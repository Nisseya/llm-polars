import os 
import polars as pl 
import chardet
from lib.error_logs import log_err
import json
from tqdm import tqdm

def main():
    schemas = []
    filenames = [f for f in os.listdir("data/sets") if os.path.isfile(os.path.join("data/sets", f)) and f.endswith(".csv")]
    
    for filename in tqdm(filenames):
        with open(os.path.join("data/sets",filename),"rb") as f:
            content = f.read()
        try:
            source_encoding = detect_encoding(content)
            if source_encoding != "utf-8":
                content = convert_bytes_to_utf8(content, source_encoding)
            
            sep = detect_sep(str(content[:10000]))
            
            df = pl.read_csv(
                content, 
                separator= sep,
                null_values=["NULL", "None", "N/A"],
                infer_schema_length=10000,
                truncate_ragged_lines=True,
                ignore_errors=True
                )
        except:
            log_err("df.json",filename)
            continue
        
        schema = extract_schema(df)
        schemas.append(
            {
            "file":filename, 
            "schema": schema
            }
            )
        
    with open("data/schemas.json",'w',encoding="utf-8") as f:
        json.dump(schemas,f,ensure_ascii=False,indent=4)


def detect_encoding(content: bytes) -> str:
    result = chardet.detect(content)
    if result['encoding'] == "MacRoman":
        return "ISO-8859-1"
    return result['encoding'] 

def detect_sep(text: str) -> str:
    lines = text.splitlines()[:4]
    possible_seps = [',', ';', '\t', '|']

    for sep in possible_seps:
        counts = [line.count(sep) for line in lines]
        if len(set(counts)) == 1 and counts[0] > 0:
            return sep

    return ','


def convert_bytes_to_utf8(input_bytes, source_encoding):
    decoded_string = input_bytes.decode(source_encoding)
    utf8_bytes = decoded_string.encode('utf-8')
    return utf8_bytes

def extract_schema(df: pl.DataFrame)->list[dict]:
    return [
        {
            "name": col,
            "dtype": str(df[col].dtype),
            "non_null_sample": df[col].drop_nulls().unique().head(3).to_list(),
        }
        for col in df.columns
    ]


if __name__=="__main__":
    main()