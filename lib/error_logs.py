import json, os

def log_err(logfile, content):
    """logs a file that had a problem into log file"""
    full_path = os.path.join("logs",logfile)
    with open(full_path,'r', encoding='utf-8') as f:
        errors = json.load(f)
    errors.append(content)
    
    with open(full_path,'w',encoding='utf-8') as f:
        json.dump(errors, f, ensure_ascii=False, indent=4)