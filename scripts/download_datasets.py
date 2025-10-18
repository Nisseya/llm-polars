import kaggle 
import json, os, zipfile
import shutil


def main():
    with open("data/list.json", "r", encoding='utf-8') as f:
        datasets = json.load(f)
    
    errs = []
    
    for dataset in datasets:
        try:
            name = dataset.get("ref")
            if already_downloaded(name.split("/")[1]):
                continue
            download(name)
        except:
            errs.append(dataset)
            
    with open("logs/errors.json","w", encoding='utf-8') as f:
        json.dump(errs,f,ensure_ascii=False,indent=4)
    
    extract_all()

def already_downloaded(name:str)-> bool:
    filenames = [f for f in os.listdir("data/sets_tmp")]
    for filename in filenames:
        if name in filename:
            return True
    return False


def download(name):
    kaggle.api.dataset_download_files(name, path="data/sets_tmp")

def extract_all():
    for file in os.listdir("data/sets_tmp"):
        if file.endswith(".zip"):
            with zipfile.ZipFile(f"data/sets_tmp/{file}", "r") as zip_ref:
                zip_ref.extractall("data/sets")
    shutil.rmtree("data/sets_tmp")
    
if __name__=="__main__":
    main()