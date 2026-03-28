import numbers
import pandas as pd
import re
import phonenumbers
import json
import ast

df = pd.read_csv("C:\\Users\\LegionGaming\\Downloads\\Telegram Desktop\\super_dirty_students.csv")
df = df.dropna(subset=['name'])
df = df.fillna({"gender": "Unknown","score": 0,"phone": "Yoq","email": "Unknown","attendance": "Unknown","gpa": 0,"remarks": "Unknown"})

def clean_gender(x):
    if pd.isna(x) or x in [None, '', 'nan']:
        return "Unknown"
    x = str(x).strip().lower()
    if x in ['m', 'male']:
        return "Male"
    elif x in ['f', 'female']:
        return "Female"
    else:
        return x.capitalize() 
def clean_gender(x):
    if pd.isna(x) or x in [None, '']:
        return x 
    x = str(x).strip().lower()
    
    if x in ['m', 'male', 'malee', 'mal', 'ml']:
        return "Male"
    
    if x in ['f', 'female', 'fmale', 'femlae', 'femal', 'fem']:
        return "Female"
    
    return x.capitalize()

df['gender'] = df['gender'].apply(clean_gender)

numeric_cols = ["age", "score", "gpa", "attendance", "money_spent"]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")  

df["age"] = df["age"].fillna(df["age"].median())
df["score"] = df["score"].fillna(df["score"].mean())
df["gpa"] = df["gpa"].fillna(df["gpa"].mean())
df["attendance"] = df["attendance"].fillna(0)
df["money_spent"] = df["money_spent"].fillna(0)

date_cols = ["date_of_join", "event_time"]

for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors="coerce")

df["email"] = df["email"].astype(str).str.lower().str.strip()
email_pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
df["email"] = df["email"].astype(str).str.lower().str.strip()

df["email"] = df["email"].str.replace("?", "", regex=False)

df["email"] = df["email"].apply(
    lambda x: x.split("@")[0] + "@" + "".join(x.split("@")[1:]) if x.count("@") > 1 else x
)

email_pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'



def is_valid_email(email):
    if pd.isna(email) or email in [None, '']:
        return False
    email = str(email).strip()
    if '@' not in email:
        return False
    pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
    return bool(re.match(pattern, email.lower()))

df = df[df['email'].apply(is_valid_email)].reset_index(drop=True)

if 'gpa' in df.columns:
    df['gpa'] = pd.to_numeric(df['gpa'], errors='coerce').round(2)

if 'score' in df.columns:
    df['score'] = pd.to_numeric(df['score'], errors='coerce').round()

df['phone'] = df['phone'].str.replace('[^a-zA-Z0-9]', '', regex=True)
df['phone'] = df['phone'].apply(lambda x:str(x))
df['phone'] = df['phone'].apply(lambda x: x[0:3] + '-' + x[3:6] + '-' + x[6:10])
df['phone'] = df['phone'].str.replace('nan--','')
df['phone'] = df['phone'].str.replace('Na--','')

def parse_json(x):
    try:
        if pd.isna(x):
            return {}
        if isinstance(x, str):
            x = x.replace("'", '"')
            return json.loads(x)
        return x
    except:
        return {}

df["profile_json"] = df["profile_json"].apply(parse_json)
json_df = pd.json_normalize(df["profile_json"])
df = pd.concat([df, json_df], axis=1)

tech_cols = [c for c in df.columns if c.startswith("skills.tech.")]

if tech_cols:
    df["tech_skills"] = df[tech_cols].apply(
        lambda r: ", ".join(
            [f"{c.split('.')[-1]}={int(v)}" for c, v in r.items() if pd.notna(v)]
        ),
        axis=1
    )

    df = df.drop(columns=tech_cols)
def format_list(x):
    if isinstance(x, list):
        return ", ".join(map(str, x))
    return x

for col in df.columns:
    df[col] = df[col].apply(format_list)
if "skills.soft" in df.columns:
    df["skills_soft"] = df["skills.soft"]

if "skills.hard" in df.columns:
    df["skills_hard"] = df["skills.hard"]

if "devices.type" in df.columns:
    df["device_type"] = df["devices.type"]

if "devices.brand" in df.columns:
    df["device_brand"] = df["devices.brand"]

def clean_devices(x):
    if pd.isna(x) or x == "":
        return {}
    try:
        res = ast.literal_eval(x)
        if isinstance(res, tuple):
            return {f"item_{i}": val for i, val in enumerate(res)}
        return res if isinstance(res, dict) else {}
    except:
        return {}

df["devices"] = df["devices"].apply(clean_devices)

devices_df = pd.json_normalize(df["devices"]).add_prefix("device_")
devices_df.index = df.index 


df = pd.concat([df.drop(columns=["devices"]), devices_df], axis=1)



print(df.head())



def parse_addresses(df, address_col):
    def extract_details(address):
        if pd.isna(address) or address == "":
            return pd.Series([None, None, None])

        postal_match = re.search(r'(\d{5,6})$', address)
        postal = postal_match.group(1) if postal_match else None

        district_match = re.search(r'([^,]+district)', address, re.IGNORECASE)
        district = district_match.group(1).strip() if district_match else None

        city_match = re.search(r',\s*([^,]+),\s*[A-Z]{2},', address)
        city = city_match.group(1).strip() if city_match else None

        return pd.Series([city, district, postal])

    df[['addr_city', 'addr_district', 'addr_postal']] = df[address_col].apply(extract_details)

    return df
df = parse_addresses(df, "address_raw")

def standardize_columns(df):
    if 'course' in df.columns:
        df['course'] = df['course'].astype(str).str.strip().str.lower()
        
        def categorize_course(course):
            if 'data science' in course or 'data_sciense' in course:
                return 'Data Science'
            elif 'python' in course:
                return 'Python'
            elif course in ['none', 'nan', '']:
                return 'Other'
            else:
                return 'Other'
        
        df['course'] = df['course'].apply(categorize_course)

    if 'status' in df.columns:
        df['status'] = df['status'].astype(str).str.strip().str.lower()
       
    return df

df = standardize_columns(df)

def fix_data_types_and_dates(df):
    date_columns = ['date_of_join', 'event_time']
    
    for col in date_columns:
        if col in df.columns:
           
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
        
    int_cols = ['student_id', 'age', 'family.siblings', 'device_year']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    float_cols = ['score', 'gpa', 'money_spent', 'family.income.father', 'family.income.mother']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace('nan', None)

    return df

if 'address_raw' in df.columns:
    df = df.drop(columns=['address_raw'])

df = fix_data_types_and_dates(df)

df = df.drop(columns=['profile_json'])

df = df.fillna({"addr_city": "Unknown","addr_district": "Unknown","addr_postal": "Unknown","event_time": "Unknown","hobbies": "Unknown","skills.soft": "Unknown","tech_skills": "Unknown","skills_soft": "Unknown","device_item_0.type": "Unknown","device_item_0.brand": "Unknown","device_item_0.year": "Unknown","device_item_1.brand": "Unknown","device_item_1.type": "Unknown","device_item_1.year": "Unknown"})


replace_cols = ["tech_skills"]

for col in replace_cols:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: "Unknown" if pd.isna(x) or x in [None, '', 'nan'] else x)

df.to_csv("C:\\Users\\LegionGaming\\Downloads\\Telegram Desktop\\super_dirty_students_cleaned.csv", index=False)
print(df.to_string())
