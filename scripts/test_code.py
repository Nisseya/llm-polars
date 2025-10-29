import os 
import json 
import polars as pl 
from lib.error_logs import log_err
import chardet

def main():
    pairs = extract_pairs()
    
    for pair in pairs[:10]:
        filename, script = extract_filename_script(pair)
        df = get_df_from_path(filename)
        try:
            execute_pl_script(df,script)
            print(script)
            print("\n"*2,"IT IS WORKING","\n"*2)
        except Exception as e:
            print(script)
            print(e)
            log_err("bad_scripts.json",script)
            
def execute_pl_script(df, script):
    globals = {"__builtins__": {}, "pl": pl}
    locals = {"df": df}
    try:
        result = eval(script, globals, locals)
        return result
    except:
        raise
    
def extract_pairs():
    with open("data/file_code_pair.jsonl","r", encoding="utf-8") as f:
        pairs = [json.loads(line) for line in f.read().splitlines()]
    return pairs 

def extract_filename_script(pair):
    return (
        pair["filename"],
        pair["code"]
    )

def get_df_from_path(filename):
    with open(os.path.join("data/sets",filename),"rb") as f:
        content = f.read()
        source_encoding = detect_encoding(content)
        if source_encoding != "utf-8":
            content = convert_bytes_to_utf8(content, source_encoding)
        
        sep = detect_sep(str(content[:10000]))
        
        df = pl.scan_csv(
            content, 
            separator= sep,
            null_values=["NULL", "None", "N/A"],
            infer_schema_length=10000,
            truncate_ragged_lines=True,
            ignore_errors=True,
            )
        return df

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

if __name__=="__main__":
    main()