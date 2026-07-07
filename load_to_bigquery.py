from google.cloud import bigquery
import json
import os

# 1. ยื่นกุญแจ JSON ให้ระบบรับทราบเพื่อขอผ่านทาง
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

# 2. เปิดดูข้อมูลดิบ Bitcoin ที่เราดูดมาในสเต็ปแรก
with open("raw_bitcoin.json", "r") as file:
    raw_data = json.load(file)
print(raw_data)  # แสดงข้อมูลดิบที่ดึงมาได้
# จัดโครงสร้างข้อมูลเตรียมหยอดลงตาราง (ให้ตรงกับช่องที่เราต้องการเก็บ)
row_to_insert = [
    {
        "price_usd": raw_data['bitcoin']['usd'],
        "price_thb": raw_data['bitcoin']['thb'],
        "fetched_at": raw_data['bitcoin']['fetched_at']
    }
]
print(row_to_insert)  # แสดงข้อมูลที่จัดโครงสร้างแล้ว
# 3. ใส่ Project ID ของคุณตรงนี้ (เอามาจากหน้าเว็บ Google Cloud ตัวที่เพิ่งสร้าง)
PROJECT_ID = "banded-setting-501714-g0" 

# เปิดท่อเชื่อมต่อไปยัง BigQuery
client = bigquery.Client(project=PROJECT_ID)

# ชี้เป้าปลายทาง: ชื่อโปรเจค.ชื่อเดตาเซต.ชื่อตาราง
table_id = f"{PROJECT_ID}.crypto_dataset.bitcoin_prices"

print("🚚 กำลังขนข้อมูลราคา Bitcoin วิ่งขึ้น Google BigQuery...")

# 4. ยิงข้อมูลขึ้นคลาวด์ตรงๆ (ระบบจะสร้างตารางให้เราอัตโนมัติถ้ายังไม่มี)
# อ้างอิงโครงสร้างตาราง (Schema) ว่าต้องการให้ช่องไหนเก็บข้อมูลประเภทอะไร
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("price_usd", "FLOAT"),
        bigquery.SchemaField("price_thb", "FLOAT"),
        bigquery.SchemaField("fetched_at", "STRING"),
    ],
    write_disposition="WRITE_APPEND", # ถ้าดึงข้อมูลมาใหม่ ให้เอาไปต่อท้ายตารางเดิมเรื่อยๆ
)

try:
    # สั่งโหลดข้อมูลเข้าไป
    load_job = client.load_table_from_json(
        row_to_insert, table_id, job_config=job_config
    )
    load_job.result() # รอจนกว่าระบบคลาวด์จะเซฟเสร็จ
    
    print("📥 สำเร็จแล้ว! ข้อมูลถูกบันทึกบน Cloud Data Warehouse จริงเรียบร้อยแล้ว!")

except Exception as e:
    print(f"❌ อัปโหลดไม่สำเร็จเนื่องจาก: {e}")