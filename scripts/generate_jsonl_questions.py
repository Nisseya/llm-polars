styles = [
    "Be concise and direct, like a user familiar with data manipulation",
    "Be concise and direct, like a user familiar with data manipulation",
    "Be concise and direct, like a user familiar with data manipulation",
    "Speak naturally and somewhat vaguely, like someone exploring their dataset",
    "Use a professional tone, as if you were a business analyst expressing a need",
    "Sound like a data student asking a comprehension question",
    "Sound like a data student asking a comprehension question",
    "Be slightly technical but keep a conversational tone"
    "Be slightly technical but keep a conversational tone"
]

additional_infos = [
    "",
    "",
    "",
    "do one mistake on a column name",
    "do one mistake on a column name",
    "do one mistake on a column name",
    "don't use exact column names but more conceptual"
]

subjects = [
    "Data aggregation and grouping (e.g., computing averages or totals per category)",
    "Joins between datasets (e.g., merging two tables by a common key)",
    "Filters and logical conditions (e.g., keeping only rows matching a criterion)",
    "Creation of new columns (e.g., combining or transforming existing variables)",
    "Data cleaning and preparation (e.g., missing values, duplicates, bounds, formats)",
    "Sorting and ranking data (e.g., top N, ascending/descending order)",
    "Analytical windows and ranking (e.g., ranks, rolling differences)",
    "Data reshaping (e.g., pivot, melt, column reorganization)",
    "Temporal or periodic analysis (e.g., sales per month, 7-day moving average)",
    "Ratio or proportion calculations (e.g., share of a country in total sales)"
]

def get_prompt(style: str, subject: str, additional_info:str):
    return (
        "You are tasked with writing questions to enrich a training dataset for a data engineering model.\n"
        "The questions should sound like those a real user might ask about their dataset in a conversation.\n\n"
        "You are given a dataset schema describing, for each column:\n"
        "- The column name\n"
        "- Its datatype\n"
        "- Three distinct sample values\n\n"
        f"Write one realistic question in the given style: {style}\n"
        f"The question should require understanding or applying this concept: {subject}\n\n"
        "Important:\n"
        "- The user does not ask for an exact technical query but rather expresses a business or analytical need.\n"
        "- The phrasing should feel natural, sometimes vague or contextual (like a real-world request).\n"
        "- You can include slight complexity, such as multiple conditions, column comparisons, or grouping.\n"
        "- Questions aren't necessarily on all columns, just pick some randomly\n"
        "Obviously, generate only the message without adding noise or unnecessary things\n"
        f"Here are some additional instructions: {additional_info}"
    )
    
import random
import json
from dotenv import load_dotenv
import re 
from tqdm import tqdm

load_dotenv()

def main():
    schemas = get_schemas()
    for i,schema in tqdm(enumerate(schemas)):
        with open(f"data/question_jobs/batch_{i}.jsonl","w", encoding='utf-8') as f:
            filename, columns = schema["file"], schema["schema"] 
            safe_filename = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)
            for j in range(50):
                subject,style,additional_info = pick_subject_and_style()
                prompt = get_prompt(subject,style,additional_info)
                entry = {
                            "custom_id": f"schema_{i}_{j}_{safe_filename}",
                            "method": "POST",
                            "url": "/v1/chat/completions",
                            "body": {
                                "model": "gpt-5-mini",
                                "messages": [
                                    {"role": "system", "content": prompt},
                                    {"role": "user", "content": str(columns)},
                                ]
                            },
                        }

                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def pick_subject_and_style():
    return random.choice(subjects), random.choice(styles), random.choice(additional_infos)

def generate_questions(schema,client):
    subject,style,additional_info = pick_subject_and_style()
    prompt = get_prompt(style, subject,additional_info)


    completion = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": schema}
        ]
    )
    
    message = completion.choices[0].message.content.strip()
    
    return {"message":message, 
            "metadata":{
                "style": style,
                "subject": subject,
                "additional_info": additional_info
                }
            }

def get_schemas():
    with open("data/schemas.json","r",encoding="utf-8") as f:
        schemas =  json.load(f)
    return schemas



if __name__=="__main__":
    main()


