# 🚀 Crypto Data Ingestion Pipeline (Bitcoin to Google BigQuery)

โปรเจคสร้าง Pipeline ขนาดเล็ก (EL Pipeline) เพื่อดึงข้อมูลราคา Bitcoin แบบ Real-time จากภายนอกเข้ามาเก็บไว้บน Cloud Data Warehouse (Google BigQuery) 

## 🏗️ System Architecture

Pipeline นี้ทำงานในรูปแบบ **Extract & Load (EL)** โดยแยกส่วนการประมวลผลและการเก็บข้อมูลออกจากกันอย่างชัดเจน:
1. **Extract**: ใช้ Python ดึงข้อมูลราคา Bitcoin (USD, THB) และ Timestamp ล่าสุดจาก CoinGecko API
2. **Load**: ใช้ Google Cloud SDK ในการตรวจสอบสิทธิ์และพ่นข้อมูลเข้าสู่ Google BigQuery Sandbox ในรูปแบบ Streaming Ingestion (Append ข้อมูลต่อท้ายตารางเดิม)

---

## 🛠️ Tech Stack & Tools
* **Language:** Python
* **Cloud Platform:** Google Cloud Platform (GCP)
* **Data Warehouse:** Google BigQuery (Sandbox mode)
* **Libraries:** `google-cloud-bigquery`, `requests` / `json`

---

## 🔒 Security & Best Practices
* **Principle of Least Privilege:** บัญชีบริการ (Service Account) ที่ใช้ในการเชื่อมต่อคลาวด์ ได้รับการจำกัดสิทธิ์เฉพาะส่วนที่จำเป็นในการทำงาน (`BigQuery Data Editor` และ `BigQuery Job User`)
* **Secret Management:** ไฟล์กุญแจดิจิทัล `gcp-key.json` ถูกปกป้องและคัดแยกออกจากระบบควบคุมเวอร์ชัน (Version Control) อย่างเด็ดขาดผ่านไฟล์ `.gitignore` เพื่อป้องกันปัญหารหัสลับหลุดสู่สาธารณะ

---

## 🚀 How to Setup & Run

### 1. Prerequisites
* ติดตั้ง Python เรียบร้อยแล้ว
* มีโปรเจคบน Google Cloud และเปิดใช้งาน BigQuery Sandbox พร้อมสร้าง Dataset ชื่อ `crypto_dataset`

### 2. Installation
ติดตั้ง Library ที่จำเป็นสำหรับการเชื่อมต่อคลาวด์:
\`\`\`bash
pip install google-cloud-bigquery
\`\`\`

### 3. Google Cloud Key Setup
ดาวน์โหลดไฟล์กุญแจ Service Account (JSON) จาก GCP Console มาวางไว้ที่ root ของโปรเจคนี้ และเปลี่ยนชื่อไฟล์เป็น `gcp-key.json`

### 4. Running the Pipeline
สั่งรันสคริปต์เพื่อดึงและโหลดข้อมูลขึ้นคลาวด์:
\`\`\`bash
python load_to_bigquery.py
\`\`\`

---

## 📈 Future Improvements (Roadmap)
* **Orchestration**: นำ Apache Airflow หรือ Prefect เข้ามาครอบเพื่อตั้งเวลา (Scheduling) ให้ท่อส่งข้อมูลทำงานอัตโนมัติในทุกๆ ชั่วโมงโดยไม่ต้องกดรันมือ
* **Data Transformation**: นำเครื่องมือ dbt (Data Build Tool) เข้ามาเชื่อมกับ BigQuery เพื่อทำกระบวนการคลีนและจัดโมเดลข้อมูล (T - Transform) ให้พร้อมสำหรับการนำไปทำแดชบอร์ดต่อ