from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import requests
import json
import os
from airflow.models import Variable

# 1. ตั้งค่าพื้นฐานให้ตัว DAG (ท่อส่งข้อมูล)
default_args = {
    'owner': 'de_user',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1), # วันที่เริ่มต้นให้ท่อมีผลทำงาน (ใช้ปีปัจจุบัน)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,                        # ถ้าพัง ให้บอทพยายามลองรันใหม่เอง 1 ครั้ง
    'retry_delay': timedelta(minutes=5), # ระยะเวลาที่จะรอ ก่อนจะกดรันใหม่อีกรอบ
}

# 2. ฟังก์ชันสำหรับ Task ที่ 1: ดึงข้อมูล (Extract)
def extract_bitcoin():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd,thb"


    response = requests.get(url)
    data = response.json()
    
    print(data)  # แสดงข้อมูลดิบที่ดึงมาได้
    # 3. แปะป้ายเวลา (Timestamp) กำกับไว้ว่าเราไปดึงมาตอนกี่โมง
    data['bitcoin']['fetched_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 4. เซฟข้อมูลดิบก้อนนี้ลงคอมเป็นไฟล์ JSON
    file_path = '/opt/airflow/raw_bitcoin.json'
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)
        
    print("[Extract] ดึงข้อมูลและเซฟไฟล์ลง Container สำเร็จ!")


# 3. ฟังก์ชันสำหรับ Task ที่ 2: พ่นขึ้น Cloud (Load)
def load_to_bigquery():
    # ยื่นกุญแจให้ระบบ (ระบุ path กุญแจที่เรายัดเข้ากล่อง Docker ไว้)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/gcp-key.json"
    
    # อ่านไฟล์ JSON จากสเต็ปแรก
    file_path = '/opt/airflow/raw_bitcoin.json'
    with open(file_path, 'r') as f:
        raw_data = json.load(f)
        
    row_to_insert = [
        {
            "price_usd": raw_data['bitcoin']['usd'],
            "price_thb": raw_data['bitcoin']['thb'],
            "fetched_at": raw_data['bitcoin']['fetched_at']
        }
    ]
    
    # 🌟 สั่งให้ดึงค่ามาจากหน้าเว็บ Airflow 
    PROJECT_ID = Variable.get("gcp_project_id")
    # PROJECT_ID = "banded-setting-501714-g0" 
    
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.crypto_dataset.bitcoin_prices"
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("price_usd", "FLOAT"),
            bigquery.SchemaField("price_thb", "FLOAT"),
            bigquery.SchemaField("fetched_at", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )
    
    load_job = client.load_table_from_json(row_to_insert, table_id, job_config=job_config)
    load_job.result()
    print("✅ [Load] พ่นข้อมูลขึ้น Google BigQuery สำเร็จ!")

def transform_bitcoin():
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/gcp-key.json"
    PROJECT_ID = Variable.get("gcp_project_id")
    client = bigquery.Client(project=PROJECT_ID)
    
    # 🌟 เขียนคำสั่ง SQL เพื่อยุบรวมข้อมูลรายชั่วโมง 
    sql_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.crypto_dataset.hourly_bitcoin_summary` AS
    SELECT 
      TIMESTAMP_TRUNC(CAST(fetched_at AS TIMESTAMP), HOUR) AS fetch_hour,
      ROUND(AVG(price_usd), 2) AS avg_price_usd,
      MAX(price_usd) AS max_price_usd,
      MIN(price_usd) AS min_price_usd,
      COUNT(*) AS total_records
    FROM `{PROJECT_ID}.crypto_dataset.bitcoin_prices`
    GROUP BY fetch_hour
    ORDER BY fetch_hour DESC;
    """
    
    # สั่งให้ BigQuery ประมวลผล SQL
    query_job = client.query(sql_query)
    query_job.result() # รอจนกว่าจะประมวลผลเสร็จ
    print("✅ [Transform] ประมวลผล SQL และอัปเดตตารางสรุปข้อมูลรายชั่วโมงสำเร็จ!")
    
# 4. ประกาศตัวแผนผังงาน (DAG)
with DAG(
    'bitcoin_crypto_pipeline',          # ชื่อตารางงานที่จะไปโผล่บนหน้าเว็บ
    default_args=default_args,
    description='ท่อส่งข้อมูลราคา Bitcoin เข้า BigQuery อัตโนมัติ',
    schedule_interval='*/5 * * * *',    # ⏱️ สั่งให้ตื่นขึ้นมารันเองอัตโนมัติ "ทุกๆ 5 นาที" (Cron Expression)
    catchup=False                       # ไม่ย้อนรันงานเก่าในอดีต เอาเฉพาะปัจจุบันพอ
) as dag:

    # กำหนดสถานะงานย่อย (Tasks)
    task_extract = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=extract_bitcoin
    )

    task_load = PythonOperator(
        task_id='load_to_bigquery_cloud',
        python_callable=load_to_bigquery
    )
    task_transform = PythonOperator(
        task_id='transform_bitcoin_data',
        python_callable=transform_bitcoin
    )

    
    task_extract >> task_load >> task_transform
    