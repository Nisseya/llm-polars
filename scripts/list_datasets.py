import kaggle
import json
from pydantic import BaseModel
from typing import List

KEYWORDS = [
    "retail", "food", "production", "marketing", "finance", "banking", "sales",
    "ecommerce", "energy", "transport", "education", "health", "hospital",
    "environment", "pollution", "climate", "agriculture", "economy", "housing",
    "real estate", "demographics", "tourism", "technology", "social media",
    "sports", "weather", "stock", "insurance", "customer", "hr", "manufacturing",
    "logistics", "traffic", "public sector", "government", "crime", "startup",
    "budget", "revenue", "expenses"
]
MAX_TOTALBYTES = 1024*1024

def main():
    total_datasets = []
    for keyword in KEYWORDS:
        datasets = search_datasets(keywords=keyword)
        total_datasets.extend(datasets)
    serializable = [d.model_dump() for d in total_datasets]
    with open("data/list.json", "w", encoding='utf-8') as f:
        json.dump(serializable, f, ensure_ascii=False, indent=4)
        

class DatasetData(BaseModel):
    id: int
    ref: str
    url: str
    subtitle: str
    totalbytes: int


def search_datasets(keywords)-> List[DatasetData]:
    results = kaggle.api.dataset_list("votes",search=keywords)
    datasets = get_dataset_list(results)
    return datasets

def get_dataset_list(results) -> List[DatasetData]:
    datasets = []
    for row in results:
        ndataset = DatasetData(
            id = getattr(row,"id"),
            ref = getattr(row,"ref"),
            url = getattr(row,"url"),
            subtitle = getattr(row,'subtitle'),
            totalbytes = getattr(row,"total_bytes"),
        )
        if ndataset.totalbytes< MAX_TOTALBYTES:
            datasets.append(ndataset)
    return datasets
        


def fetch_dataset():
    with open("data/list.json", "r", encoding='utf-8') as f:
        datasets = json.loads(f.read())

if __name__=="__main__":
    main()

