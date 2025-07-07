import psycopg2
import json
import os
from typing import Dict, List, Any
import boto3

class PostgreSQLAgent:
    def __init__(self):
        # Database connection
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'siamtech'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password')
        }
        
        # Claude for SQL generation
        self.bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-1')
        )
        
        # Database schema information
        self.schema_info = {
            "employees": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อพนักงาน"},
                    {"name": "department", "type": "VARCHAR", "description": "แผนก (IT, Sales, HR, Management)"},
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
            }
        }

    def get_connection(self):
        """สร้าง database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"Database connection error: {e}")
            return None

    def generate_sql(self, question: str) -> str:
        """สร้าง SQL จาก natural language question"""
        
        # สร้าง schema description
        schema_desc = ""
        for table_name, table_info in self.schema_info.items():
            schema_desc += f"\nTable: {table_name}\n"
            schema_desc += f"Description: {table_info['description']}\n"
            schema_desc += "Columns:\n"
            for col in table_info['columns']:
                schema_desc += f"  - {col['name']} ({col['type']}): {col['description']}\n"
        
        prompt = f"""คุณเป็น SQL expert ที่เชี่ยวชาญในการแปลงคำถามภาษาไทยเป็น SQL query

Database Schema:
{schema_desc}

คำถาม: {question}

กรุณาสร้าง SQL query ที่ถูกต้อง โดย:
1. ใช้เฉพาะ tables และ columns ที่มีใน schema
2. ตอบให้ตรงกับคำถาม
3. ใช้ JOIN เมื่อจำเป็น
4. ส่งคืนเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย

SQL Query:"""

        try:
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
            
            return sql_query
            
        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query และส่งคืนผลลัพธ์"""
        conn = self.get_connection()
        if not conn:
            return {
                "success": False,
                "error": "Cannot connect to database",
                "data": None
            }
        
        try:
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
                
                return {
                    "success": True,
                    "data": data,
                    "columns": columns,
                    "row_count": len(data)
                }
            else:
                # For non-SELECT queries
                conn.commit()
                return {
                    "success": True,
                    "message": "Query executed successfully",
                    "affected_rows": cursor.rowcount
                }
                
        except Exception as e:
            conn.rollback()
            return {
                "success": False,
                "error": str(e),
                "data": None
            }
        finally:
            cursor.close()
            conn.close()

    def format_results(self, results: Dict[str, Any], question: str) -> str:
        """แปลงผลลัพธ์ SQL เป็นคำตอบภาษาไทยที่เข้าใจง่าย"""
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาดในการค้นหาข้อมูล: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถามของคุณ"
        
        # สร้าง prompt สำหรับ Claude ในการแปลงผลลัพธ์
        prompt = f"""คำถาม: {question}

ผลลัพธ์จากฐานข้อมูล:
{json.dumps(data, ensure_ascii=False, indent=2, default=str)}

กรุณาสรุปผลลัพธ์เป็นคำตอบภาษาไทยที่เข้าใจง่าย โดย:
1. ตอบตรงประเด็นคำถาม
2. แสดงตัวเลขและข้อมูลที่สำคัญ
3. ใช้ภาษาที่เป็นธรรมชาติ
4. หากมีข้อมูลมาก ให้สรุปเป็นสาระสำคัญ

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
            
            return answer
            
        except Exception as e:
            print(f"Error formatting results: {e}")
            # Fallback to simple formatting
            return self._simple_format(data, question)

    def _simple_format(self, data: List[Dict], question: str) -> str:
        """Simple formatting fallback"""
        if len(data) == 1 and len(data[0]) == 1:
            # Single value result
            value = list(data[0].values())[0]
            return f"ผลลัพธ์: {value}"
        
        # Multiple results
        result_text = f"พบข้อมูลทั้งหมด {len(data)} รายการ:\n\n"
        for i, row in enumerate(data[:5], 1):  # Show first 5 results
            result_text += f"{i}. "
            result_text += ", ".join([f"{k}: {v}" for k, v in row.items()])
            result_text += "\n"
        
        if len(data) > 5:
            result_text += f"\n... และอีก {len(data) - 5} รายการ"
        
        return result_text

    def query(self, question: str) -> Dict[str, Any]:
        """Main method สำหรับรับคำถามและส่งคืนคำตอบ"""
        try:
            # Generate SQL
            sql = self.generate_sql(question)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query จากคำถามนี้ได้",
                    "sql": None,
                    "data": None
                }
            
            print(f"Generated SQL: {sql}")
            
            # Execute query
            results = self.execute_query(sql)
            
            # Format answer
            if results["success"]:
                answer = self.format_results(results, question)
                return {
                    "success": True,
                    "answer": answer,
                    "sql": sql,
                    "data": results["data"]
                }
            else:
                return {
                    "success": False,
                    "answer": f"เกิดข้อผิดพลาดในการดึงข้อมูล: {results.get('error')}",
                    "sql": sql,
                    "data": None
                }
                
        except Exception as e:
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาด: {str(e)}",
                "sql": None,
                "data": None
            }

# Test usage
if __name__ == "__main__":
    agent = PostgreSQLAgent()
    
    # Test questions
    test_questions = [
        "มีพนักงานกี่คน?",
        "พนักงานแผนก IT มีกี่คน?",
        "เงินเดือนเฉลี่ยของพนักงานเท่าไหร่?",
        "โปรเจคที่กำลังดำเนินการอยู่มีอะไรบ้าง?",
        "พนักงานคนไหนทำงานในโปรเจค CRM?"
    ]
    
    for question in test_questions:
        print(f"\nคำถาม: {question}")
        result = agent.query(question)
        print(f"คำตอบ: {result['answer']}")
        if result.get('sql'):
            print(f"SQL: {result['sql']}")
        print("-" * 50)