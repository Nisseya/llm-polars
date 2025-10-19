from scripts.download_datasets import main as download
from scripts.get_schemas import main as enhance
from scripts.list_datasets import main as list

import os, json

if __name__=="__main__":
    
    dirs = ['data/examples',"data/sets","data/sets_tmp","logs"]
    files = ['logs/errors.json', "logs/df.json", "data/list.json","data/schemas.json"]
    
    for dir in dirs:
        os.makedirs(dir) 
    
    for file in files:
        with open(file,"w",encoding='utf-8') as f:
            json.dump([],f,ensure_ascii=False)
    
    ## Get schemas of all datasets (~~400 datasets)
    list()
    download()
    enhance()
    
    ## Now generate pairs-> generate code and validate it