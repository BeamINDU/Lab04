import psycopg2
import json
import os
from typing import Dict, List, Any, Optional
import boto3
import logging
from contextlib import contextmanager
from tenant_manager import get_tenant_config, get_tenant_database_config

logger = logging.getLogger(__name__)

class PostgreSQLAgent:
    def __init__(self, tenant_id: Optional[str] = None):
        # Current tenant
        self.current_tenant_id = tenant_id
        self._connection_pools = {}  # Store connections per tenant
        
        # Claude for SQL generation
        self.bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-1')
        )
        
        # Enhanced database schema information with new tables
        self.schema_info = {
            # Core tables
            "employees": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อพนักงาน"},
                    {"name": "department", "type": "VARCHAR", "description": "แผนก (Information Technology, Sales & Marketing, Human Resources, Management)"},
                    {"name": "position", "type": "VARCHAR", "description": "ตำแหน่งงาน"},
                    {"name": "salary", "type": "DECIMAL", "description": "เงินเดือน"},
                    {"name": "hire_date", "type": "DATE", "description": "วันที่เข้าทำงาน"},
                    {"name": "email", "type": "VARCHAR", "description": "อีเมล"}
                ],
                "description": "ข้อมูลพนักงานบริษัท"
            },
            "projects": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อโปรเจค"},
                    {"name": "client", "type": "VARCHAR", "description": "ลูกค้า"},
                    {"name": "budget", "type": "DECIMAL", "description": "งบประมาณ"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (active, completed, cancelled)"},
                    {"name": "start_date", "type": "DATE", "description": "วันเริ่มโปรเจค"},
                    {"name": "end_date", "type": "DATE", "description": "วันสิ้นสุดโปรเจค"},
                    {"name": "tech_stack", "type": "VARCHAR", "description": "เทคโนโลยีที่ใช้"}
                ],
                "description": "ข้อมูลโปรเจคของบริษัท"
            },
            "employee_projects": {
                "columns": [
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "project_id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "role", "type": "VARCHAR", "description": "บทบาทในโปรเจค"},
                    {"name": "allocation", "type": "DECIMAL", "description": "สัดส่วนงาน (0-1)"}
                ],
                "description": "ความสัมพันธ์ระหว่างพนักงานและโปรเจค"
            },
            
            # Enhanced tables - Department Management
            "departments": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสแผนก"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อแผนก"},
                    {"name": "description", "type": "TEXT", "description": "รายละเอียดแผนก"},
                    {"name": "manager_id", "type": "INTEGER", "description": "รหัสหัวหน้าแผนก"},
                    {"name": "budget", "type": "DECIMAL", "description": "งบประมาณแผนก"},
                    {"name": "location", "type": "VARCHAR", "description": "ที่ตั้งแผนก"},
                    {"name": "established_date", "type": "DATE", "description": "วันที่ก่อตั้งแผนก"},
                    {"name": "is_active", "type": "BOOLEAN", "description": "สถานะการใช้งาน"}
                ],
                "description": "ข้อมูลแผนกต่างๆ ในบริษัท"
            },
            
            # Client Management
            "clients": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสลูกค้า"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อลูกค้า"},
                    {"name": "industry", "type": "VARCHAR", "description": "อุตสาหกรรม"},
                    {"name": "contact_person", "type": "VARCHAR", "description": "ผู้ติดต่อ"},
                    {"name": "email", "type": "VARCHAR", "description": "อีเมลลูกค้า"},
                    {"name": "phone", "type": "VARCHAR", "description": "เบอร์โทรศัพท์"},
                    {"name": "address", "type": "TEXT", "description": "ที่อยู่"},
                    {"name": "website", "type": "VARCHAR", "description": "เว็บไซต์"},
                    {"name": "contract_value", "type": "DECIMAL", "description": "มูลค่าสัญญา"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะลูกค้า (active, inactive, prospect, churned)"},
                    {"name": "first_project_date", "type": "DATE", "description": "วันที่เริ่มโปรเจคแรก"},
                    {"name": "country", "type": "VARCHAR", "description": "ประเทศ (สำหรับลูกค้าต่างชาติ)"},
                    {"name": "currency", "type": "VARCHAR", "description": "สกุลเงิน (สำหรับลูกค้าต่างชาติ)"}
                ],
                "description": "ข้อมูลลูกค้าของบริษัท"
            },
            
            # Skills Management
            "skills": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสทักษะ"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อทักษะ"},
                    {"name": "category", "type": "VARCHAR", "description": "หมวดหมู่ (technical, soft, language, certification)"},
                    {"name": "description", "type": "TEXT", "description": "รายละเอียดทักษะ"},
                    {"name": "is_active", "type": "BOOLEAN", "description": "สถานะการใช้งาน"},
                    {"name": "global_demand_level", "type": "VARCHAR", "description": "ระดับความต้องการในตลาดโลก (สำหรับ International Office)"}
                ],
                "description": "ทักษะและความสามารถต่างๆ"
            },
            "employee_skills": {
                "columns": [
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "skill_id", "type": "INTEGER", "description": "รหัสทักษะ"},
                    {"name": "proficiency_level", "type": "INTEGER", "description": "ระดับความชำนาญ (1-5)"},
                    {"name": "acquired_date", "type": "DATE", "description": "วันที่ได้รับทักษะ"},
                    {"name": "certified", "type": "BOOLEAN", "description": "มีใบรับรองหรือไม่"},
                    {"name": "certification_date", "type": "DATE", "description": "วันที่ได้รับใบรับรอง"}
                ],
                "description": "ทักษะของพนักงานแต่ละคน"
            },
            
            # Time and Expense Tracking
            "timesheets": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสไทม์ชีต"},
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "project_id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "work_date", "type": "DATE", "description": "วันที่ทำงาน"},
                    {"name": "hours_worked", "type": "DECIMAL", "description": "จำนวนชั่วโมงที่ทำงาน"},
                    {"name": "task_description", "type": "TEXT", "description": "รายละเอียดงานที่ทำ"},
                    {"name": "billable", "type": "BOOLEAN", "description": "เป็นชั่วโมงที่เรียกเก็บเงินได้หรือไม่"},
                    {"name": "hourly_rate", "type": "DECIMAL", "description": "อัตราค่าจ้างต่อชั่วโมง"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (draft, submitted, approved, rejected)"},
                    {"name": "hourly_rate_usd", "type": "DECIMAL", "description": "อัตราค่าจ้างใน USD (สำหรับ International)"},
                    {"name": "currency", "type": "VARCHAR", "description": "สกุลเงิน"},
                    {"name": "client_timezone", "type": "VARCHAR", "description": "เขตเวลาของลูกค้า"}
                ],
                "description": "การบันทึกเวลาทำงานของพนักงาน"
            },
            "expenses": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสค่าใช้จ่าย"},
                    {"name": "project_id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "category", "type": "VARCHAR", "description": "หมวดหมู่ (travel, equipment, software, training)"},
                    {"name": "description", "type": "TEXT", "description": "รายละเอียดค่าใช้จ่าย"},
                    {"name": "amount", "type": "DECIMAL", "description": "จำนวนเงิน"},
                    {"name": "expense_date", "type": "DATE", "description": "วันที่เกิดค่าใช้จ่าย"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (pending, approved, rejected, reimbursed)"},
                    {"name": "currency", "type": "VARCHAR", "description": "สกุลเงิน"},
                    {"name": "amount_usd", "type": "DECIMAL", "description": "จำนวนเงินใน USD"},
                    {"name": "country", "type": "VARCHAR", "description": "ประเทศที่เกิดค่าใช้จ่าย"}
                ],
                "description": "ค่าใช้จ่ายในโปรเจค"
            },
            
            # Meeting Management
            "meetings": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสการประชุม"},
                    {"name": "title", "type": "VARCHAR", "description": "หัวข้อการประชุม"},
                    {"name": "description", "type": "TEXT", "description": "รายละเอียดการประชุม"},
                    {"name": "meeting_date", "type": "TIMESTAMP", "description": "วันเวลาประชุม"},
                    {"name": "duration_minutes", "type": "INTEGER", "description": "ระยะเวลาประชุม (นาที)"},
                    {"name": "location", "type": "VARCHAR", "description": "สถานที่ประชุม"},
                    {"name": "meeting_type", "type": "VARCHAR", "description": "ประเภทการประชุม (internal, client, vendor, training)"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (scheduled, completed, cancelled)"},
                    {"name": "organizer_id", "type": "INTEGER", "description": "ผู้จัดการประชุม"},
                    {"name": "timezone", "type": "VARCHAR", "description": "เขตเวลา"},
                    {"name": "meeting_platform", "type": "VARCHAR", "description": "แพลตฟอร์ม (zoom, teams, google_meet)"},
                    {"name": "language", "type": "VARCHAR", "description": "ภาษาที่ใช้ในการประชุม"}
                ],
                "description": "การประชุมต่างๆ"
            },
            "meeting_attendees": {
                "columns": [
                    {"name": "meeting_id", "type": "INTEGER", "description": "รหัสการประชุม"},
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "attendance_status", "type": "VARCHAR", "description": "สถานะการเข้าร่วม (invited, confirmed, attended, absent)"},
                    {"name": "role", "type": "VARCHAR", "description": "บทบาท (organizer, presenter, participant)"},
                    {"name": "timezone", "type": "VARCHAR", "description": "เขตเวลาของผู้เข้าร่วม"}
                ],
                "description": "ผู้เข้าร่วมประชุม"
            },
            
            # Asset Management
            "equipment": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสอุปกรณ์"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อเรียกอุปกรณ์"},
                    {"name": "category", "type": "VARCHAR", "description": "หมวดหมู่ (laptop, desktop, monitor, software_license)"},
                    {"name": "brand", "type": "VARCHAR", "description": "ยี่ห้อ"},
                    {"name": "model", "type": "VARCHAR", "description": "รุ่น"},
                    {"name": "serial_number", "type": "VARCHAR", "description": "หมายเลขเครื่อง"},
                    {"name": "purchase_date", "type": "DATE", "description": "วันที่ซื้อ"},
                    {"name": "purchase_price", "type": "DECIMAL", "description": "ราคาซื้อ"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (available, assigned, maintenance, retired)"},
                    {"name": "assigned_to", "type": "INTEGER", "description": "มอบหมายให้พนักงาน"},
                    {"name": "location", "type": "VARCHAR", "description": "ที่ตั้ง"},
                    {"name": "country", "type": "VARCHAR", "description": "ประเทศ (สำหรับ International)"},
                    {"name": "shipping_status", "type": "VARCHAR", "description": "สถานะการจัดส่ง"}
                ],
                "description": "อุปกรณ์และทรัพย์สินของบริษัท"
            },
            
            # Training Management
            "training": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสการอบรม"},
                    {"name": "title", "type": "VARCHAR", "description": "หัวข้อการอบรม"},
                    {"name": "description", "type": "TEXT", "description": "รายละเอียดการอบรม"},
                    {"name": "provider", "type": "VARCHAR", "description": "ผู้จัดอบรม"},
                    {"name": "category", "type": "VARCHAR", "description": "หมวดหมู่ (technical, soft_skills, certification)"},
                    {"name": "start_date", "type": "DATE", "description": "วันเริ่มอบรม"},
                    {"name": "end_date", "type": "DATE", "description": "วันสิ้นสุดอบรม"},
                    {"name": "duration_hours", "type": "INTEGER", "description": "ระยะเวลาอบรม (ชั่วโมง)"},
                    {"name": "cost", "type": "DECIMAL", "description": "ค่าใช้จ่าย"},
                    {"name": "max_participants", "type": "INTEGER", "description": "จำนวนผู้เข้าร่วมสูงสุด"},
                    {"name": "location", "type": "VARCHAR", "description": "สถานที่อบรม"},
                    {"name": "training_type", "type": "VARCHAR", "description": "รูปแบบ (classroom, online, workshop)"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (planned, ongoing, completed, cancelled)"},
                    {"name": "language", "type": "VARCHAR", "description": "ภาษาที่ใช้อบรม"},
                    {"name": "international_certification", "type": "BOOLEAN", "description": "ใบรับรองระหว่างประเทศ"}
                ],
                "description": "หลักสูตรอบรมต่างๆ"
            },
            "training_attendees": {
                "columns": [
                    {"name": "training_id", "type": "INTEGER", "description": "รหัสการอบรม"},
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "registration_date", "type": "DATE", "description": "วันที่ลงทะเบียน"},
                    {"name": "attendance_status", "type": "VARCHAR", "description": "สถานะการเข้าร่วม (registered, attended, completed)"},
                    {"name": "completion_date", "type": "DATE", "description": "วันที่ผ่านการอบรม"},
                    {"name": "score", "type": "DECIMAL", "description": "คะแนนที่ได้"},
                    {"name": "certificate_issued", "type": "BOOLEAN", "description": "ออกใบรับรองแล้วหรือไม่"},
                    {"name": "feedback", "type": "TEXT", "description": "ความคิดเห็น"}
                ],
                "description": "ผู้เข้าร่วมอบรม"
            },
            
            # International Office specific tables
            "international_contracts": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสสัญญา"},
                    {"name": "contract_number", "type": "VARCHAR", "description": "หมายเลขสัญญา"},
                    {"name": "client_id", "type": "INTEGER", "description": "รหัสลูกค้า"},
                    {"name": "project_id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "contract_type", "type": "VARCHAR", "description": "ประเภทสัญญา (fixed_price, time_and_materials)"},
                    {"name": "total_value", "type": "DECIMAL", "description": "มูลค่ารวม"},
                    {"name": "currency", "type": "VARCHAR", "description": "สกุลเงิน"},
                    {"name": "total_value_usd", "type": "DECIMAL", "description": "มูลค่าใน USD"},
                    {"name": "payment_terms", "type": "VARCHAR", "description": "เงื่อนไขการชำระเงิน"},
                    {"name": "start_date", "type": "DATE", "description": "วันเริ่มสัญญา"},
                    {"name": "end_date", "type": "DATE", "description": "วันสิ้นสุดสัญญา"},
                    {"name": "governing_law", "type": "VARCHAR", "description": "กฎหมายที่ใช้บังคับ"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะสัญญา (draft, active, completed, terminated)"}
                ],
                "description": "สัญญาระหว่างประเทศ (สำหรับ International Office)"
            },
            "international_payments": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสการชำระเงิน"},
                    {"name": "contract_id", "type": "INTEGER", "description": "รหัสสัญญา"},
                    {"name": "invoice_number", "type": "VARCHAR", "description": "หมายเลขใบแจ้งหนี้"},
                    {"name": "amount", "type": "DECIMAL", "description": "จำนวนเงิน"},
                    {"name": "currency", "type": "VARCHAR", "description": "สกุลเงิน"},
                    {"name": "amount_usd", "type": "DECIMAL", "description": "จำนวนเงินใน USD"},
                    {"name": "exchange_rate", "type": "DECIMAL", "description": "อัตราแลกเปลี่ยน"},
                    {"name": "payment_method", "type": "VARCHAR", "description": "วิธีการชำระเงิน (wire_transfer, swift, paypal)"},
                    {"name": "payment_date", "type": "DATE", "description": "วันที่ชำระเงิน"},
                    {"name": "due_date", "type": "DATE", "description": "วันครบกำหนด"},
                    {"name": "received_date", "type": "DATE", "description": "วันที่รับเงิน"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (pending, received, overdue, cancelled)"}
                ],
                "description": "การชำระเงินระหว่างประเทศ (สำหรับ International Office)"
            },
            
            # Views
            "employee_performance": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อพนักงาน"},
                    {"name": "department", "type": "VARCHAR", "description": "แผนก"},
                    {"name": "projects_count", "type": "INTEGER", "description": "จำนวนโปรเจคที่เข้าร่วม"},
                    {"name": "total_hours_this_month", "type": "DECIMAL", "description": "ชั่วโมงทำงานเดือนนี้"},
                    {"name": "skills_count", "type": "INTEGER", "description": "จำนวนทักษะ"},
                    {"name": "equipment_assigned", "type": "INTEGER", "description": "อุปกรณ์ที่ได้รับมอบหมาย"}
                ],
                "description": "ประสิทธิภาพการทำงานของพนักงาน (View)"
            },
            "project_financial_summary": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อโปรเจค"},
                    {"name": "client", "type": "VARCHAR", "description": "ลูกค้า"},
                    {"name": "budget", "type": "DECIMAL", "description": "งบประมาณ"},
                    {"name": "labor_cost", "type": "DECIMAL", "description": "ค่าแรงงาน"},
                    {"name": "expenses_total", "type": "DECIMAL", "description": "ค่าใช้จ่ายรวม"},
                    {"name": "total_cost", "type": "DECIMAL", "description": "ต้นทุนรวม"},
                    {"name": "remaining_budget", "type": "DECIMAL", "description": "งบประมาณคงเหลือ"},
                    {"name": "team_size", "type": "INTEGER", "description": "ขนาดทีม"}
                ],
                "description": "สรุปการเงินโปรเจค (View)"
            },
            "department_statistics": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสแผนก"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อแผนก"},
                    {"name": "budget", "type": "DECIMAL", "description": "งบประมาณแผนก"},
                    {"name": "employee_count", "type": "INTEGER", "description": "จำนวนพนักงาน"},
                    {"name": "avg_salary", "type": "DECIMAL", "description": "เงินเดือนเฉลี่ย"},
                    {"name": "total_salary", "type": "DECIMAL", "description": "เงินเดือนรวม"}
                ],
                "description": "สถิติแผนก (View)"
            }
        }

    def set_tenant(self, tenant_id: str):
        """Set current tenant for operations"""
        self.current_tenant_id = tenant_id
        logger.info(f"PostgreSQL Agent switched to tenant: {tenant_id}")

    def get_current_tenant_id(self) -> str:
        """Get current tenant ID"""
        if not self.current_tenant_id:
            raise ValueError("No tenant ID set. Call set_tenant() first.")
        return self.current_tenant_id

    def get_tenant_db_config(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get database configuration for specified tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            db_config = get_tenant_database_config(tenant_id)
            logger.debug(f"Retrieved DB config for tenant: {tenant_id}")
            return db_config
        except Exception as e:
            logger.error(f"Failed to get DB config for tenant {tenant_id}: {e}")
            raise

    @contextmanager
    def get_connection(self, tenant_id: Optional[str] = None):
        """Get database connection for specified tenant with context manager"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        conn = None
        try:
            db_config = self.get_tenant_db_config(tenant_id)
            conn = psycopg2.connect(**db_config)
            logger.debug(f"Database connection established for tenant: {tenant_id}")
            yield conn
        except Exception as e:
            logger.error(f"Database connection error for tenant {tenant_id}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
                logger.debug(f"Database connection closed for tenant: {tenant_id}")

    def generate_sql(self, question: str, tenant_id: Optional[str] = None) -> str:
        """สร้าง SQL จาก natural language question - Enhanced for new schema"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        # Get tenant info for context
        tenant_config = get_tenant_config(tenant_id)
        
        # สร้าง enhanced schema description
        schema_desc = ""
        for table_name, table_info in self.schema_info.items():
            schema_desc += f"\nTable: {table_name}\n"
            schema_desc += f"Description: {table_info['description']}\n"
            schema_desc += "Columns:\n"
            for col in table_info['columns']:
                schema_desc += f"  - {col['name']} ({col['type']}): {col['description']}\n"
            schema_desc += "\n"
        
        # Add tenant context to prompt
        tenant_context = f"""
บริบทของบริษัท: {tenant_config.name}
Database: {tenant_config.database_config['database']}

ข้อมูลเพิ่มเติม:
- Company A (Bangkok HQ): ข้อมูลสำนักงานใหญ่ กรุงเทพฯ
- Company B (Chiang Mai): ข้อมูลสาขาเชียงใหม่ เน้นท่องเที่ยวและธุรกิจท้องถิ่น
- Company C (International): ข้อมูลสำนักงานต่างประเทศ มีข้อมูล multi-currency และ timezone
"""
        
        # Enhanced prompt with examples
        prompt = f"""คุณเป็น SQL expert ที่เชี่ยวชาญในการแปลงคำถามภาษาไทยและอังกฤษเป็น SQL query

{tenant_context}

Database Schema:
{schema_desc}

คำถาม: {question}

กรุณาสร้าง SQL query ที่ถูกต้อง โดย:
1. ใช้เฉพาะ tables และ columns ที่มีใน schema
2. ตอบให้ตรงกับคำถาม
3. ใช้ JOIN เมื่อจำเป็น (เช่น LEFT JOIN สำหรับข้อมูลที่อาจไม่มี)
4. ใช้ aggregate functions เมื่อต้องการสรุป (COUNT, SUM, AVG)
5. ใช้ WHERE clause เมื่อต้องการกรองข้อมูล
6. สำหรับ International Office: ใช้ _usd columns สำหรับการคำนวณ
7. ส่งคืนเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย

ตัวอย่าง:
- "พนักงานกี่คน?" → SELECT COUNT(*) FROM employees;
- "เงินเดือนเฉลี่ยของแผนก IT?" → SELECT AVG(salary) FROM employees WHERE department = 'Information Technology';
- "โปรเจคที่ยังไม่เสร็จ?" → SELECT * FROM projects WHERE status = 'active';
- "พนักงานที่มีทักษะ React?" → SELECT e.name FROM employees e JOIN employee_skills es ON e.id = es.employee_id JOIN skills s ON es.skill_id = s.id WHERE s.name = 'React';

SQL Query:"""

        try:
            # Get tenant-specific API keys if available
            tenant_config = get_tenant_config(tenant_id)
            api_keys = tenant_config.api_keys
            
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 500,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            sql_query = response_body['content'][0]['text'].strip()
            
            # Clean up SQL query (remove markdown formatting if any)
            if sql_query.startswith('```'):
                sql_query = sql_query.split('\n')[1:-1]
                sql_query = '\n'.join(sql_query)
            
            logger.debug(f"Generated SQL for tenant {tenant_id}: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL for tenant {tenant_id}: {e}")
            return None

    def execute_query(self, sql: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute SQL query และส่งคืนผลลัพธ์"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Get results
                if sql.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    data = []
                    for row in results:
                        data.append(dict(zip(columns, row)))
                    
                    logger.info(f"Query executed successfully for tenant {tenant_id}, returned {len(data)} rows")
                    
                    return {
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "row_count": len(data),
                        "tenant_id": tenant_id
                    }
                else:
                    # For non-SELECT queries
                    conn.commit()
                    logger.info(f"Non-SELECT query executed for tenant {tenant_id}")
                    
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "affected_rows": cursor.rowcount,
                        "tenant_id": tenant_id
                    }
                    
        except Exception as e:
            logger.error(f"Query execution error for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": None,
                "tenant_id": tenant_id
            }

    def format_results(self, results: Dict[str, Any], question: str, tenant_id: Optional[str] = None) -> str:
        """แปลงผลลัพธ์ SQL เป็นคำตอบภาษาไทยที่เข้าใจง่าย"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาดในการค้นหาข้อมูล: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถามของคุณ"
        
        # Get tenant info for context
        tenant_config = get_tenant_config(tenant_id)
        
        # สร้าง prompt สำหรับ Claude ในการแปลงผลลัพธ์
        prompt = f"""บริษัท: {tenant_config.name}
คำถาม: {question}

ผลลัพธ์จากฐานข้อมูล:
{json.dumps(data, ensure_ascii=False, indent=2, default=str)}

กรุณาสรุปผลลัพธ์เป็นคำตอบภาษาไทยที่เข้าใจง่าย โดย:
1. ตอบตรงประเด็นคำถาม
2. แสดงตัวเลขและข้อมูลที่สำคัญ
3. ใช้ภาษาที่เป็นธรรมชาติ
4. หากมีข้อมูลมาก ให้สรุปเป็นสาระสำคัญ
5. ระบุว่าข้อมูลมาจากบริษัท {tenant_config.name}

คำตอบ:"""

        try:
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            answer = response_body['content'][0]['text'].strip()
            
            logger.debug(f"Formatted results for tenant {tenant_id}")
            return answer
            
        except Exception as e:
            logger.error(f"Error formatting results for tenant {tenant_id}: {e}")
            # Fallback to simple formatting
            return self._simple_format(data, question, tenant_config.name)

    def _simple_format(self, data: List[Dict], question: str, tenant_name: str) -> str:
        """Simple formatting fallback"""
        if len(data) == 1 and len(data[0]) == 1:
            # Single value result
            value = list(data[0].values())[0]
            return f"ผลลัพธ์จาก {tenant_name}: {value}"
        
        # Multiple results
        result_text = f"ข้อมูลจาก {tenant_name} - พบข้อมูลทั้งหมด {len(data)} รายการ:\n\n"
        for i, row in enumerate(data[:5], 1):  # Show first 5 results
            result_text += f"{i}. "
            result_text += ", ".join([f"{k}: {v}" for k, v in row.items()])
            result_text += "\n"
        
        if len(data) > 5:
            result_text += f"\n... และอีก {len(data) - 5} รายการ"
        
        return result_text

    def query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Main method สำหรับรับคำถามและส่งคืนคำตอบ"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            # Generate SQL
            sql = self.generate_sql(question, tenant_id)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query จากคำถามนี้ได้",
                    "sql": None,
                    "data": None,
                    "tenant_id": tenant_id
                }
            
            logger.info(f"Generated SQL for tenant {tenant_id}: {sql}")
            
            # Execute query
            results = self.execute_query(sql, tenant_id)
            
            # Format answer
            if results["success"]:
                answer = self.format_results(results, question, tenant_id)
                return {
                    "success": True,
                    "answer": answer,
                    "sql": sql,
                    "data": results["data"],
                    "tenant_id": tenant_id
                }
            else:
                return {
                    "success": False,
                    "answer": f"เกิดข้อผิดพลาดในการดึงข้อมูล: {results.get('error')}",
                    "sql": sql,
                    "data": None,
                    "tenant_id": tenant_id
                }
                
        except Exception as e:
            logger.error(f"Error in query for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาด: {str(e)}",
                "sql": None,
                "data": None,
                "tenant_id": tenant_id
            }

    def test_connection(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Test database connection for a tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test, current_database() as db_name")
                result = cursor.fetchone()
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "database": result[1] if result else "unknown",
                    "message": "Connection successful"
                }
        except Exception as e:
            logger.error(f"Connection test failed for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e),
                "message": "Connection failed"
            }

    def get_tenant_stats(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get database statistics for a tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            stats = {}
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                
                # Get table counts
                for table_name in self.schema_info.keys():
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    stats[f"{table_name}_count"] = count
                
                # Get database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
                """)
                db_size = cursor.fetchone()[0]
                stats['database_size'] = db_size
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "stats": stats
                }
                
        except Exception as e:
            logger.error(f"Failed to get stats for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e)
            }


# Multi-tenant convenience functions
def create_postgres_agent(tenant_id: str) -> PostgreSQLAgent:
    """Create PostgreSQL agent for specific tenant"""
    agent = PostgreSQLAgent(tenant_id)
    return agent

def query_postgres_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick query function for specific tenant"""
    agent = create_postgres_agent(tenant_id)
    return agent.query(question, tenant_id)

def test_tenant_database(tenant_id: str) -> Dict[str, Any]:
    """Test database connection for specific tenant"""
    agent = create_postgres_agent(tenant_id)
    return agent.test_connection(tenant_id)


# Test usage
if __name__ == "__main__":
    # Test multi-tenant functionality
    test_tenants = ['company-a', 'company-b', 'company-c']
    test_questions = [
        "มีพนักงานกี่คน?",
        "เงินเดือนเฉลี่ยของพนักงานเท่าไหร่?",
        "โปรเจคที่กำลังดำเนินการอยู่มีอะไรบ้าง?"
    ]
    
    for tenant_id in test_tenants:
        print(f"\n{'='*60}")
        print(f"🏢 Testing Tenant: {tenant_id}")
        print(f"{'='*60}")
        
        # Test connection
        try:
            connection_result = test_tenant_database(tenant_id)
            print(f"🔗 Connection: {connection_result}")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            continue
        
        # Test queries
        agent = create_postgres_agent(tenant_id)
        for question in test_questions:
            print(f"\n❓ คำถาม: {question}")
            try:
                result = agent.query(question, tenant_id)
                print(f"✅ คำตอบ: {result['answer']}")
                if result.get('sql'):
                    print(f"🔍 SQL: {result['sql']}")
            except Exception as e:
                print(f"❌ Error: {e}")
        
        # Get stats
        try:
            stats = agent.get_tenant_stats(tenant_id)
            if stats['success']:
                print(f"\n📊 Stats: {stats['stats']}")
        except Exception as e:
            print(f"❌ Stats error: {e}")
        
        print("-" * 60)