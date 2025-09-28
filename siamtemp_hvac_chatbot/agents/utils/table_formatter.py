# table_formatter.py - สร้างตารางจาก results
"""
Table Formatter for converting database results to formatted tables
"""

from typing import List, Dict, Any
import json
from datetime import datetime

class TableFormatter:
    """Format database results into tables"""
    
    def __init__(self):
        self.max_cell_width = 50  # ความกว้างสูงสุดของ cell
        self.table_styles = {
            'markdown': self._format_markdown_table,
            'ascii': self._format_ascii_table,
            'html': self._format_html_table,
            'simple': self._format_simple_table
        }
    
    def format_results_as_table(self, results: List[Dict], 
                               style: str = 'markdown',
                               title: str = None) -> str:
        """สร้างตารางจาก results - แสดงเป็นตารางทั้งหมด"""
        
        if not results:
            return "ไม่มีข้อมูล"
        
        # เตรียมข้อมูล
        headers = list(results[0].keys())
        processed_results = self._process_data(results)
        
        # บังคับใช้ markdown table สำหรับทุกกรณี
        table = self._format_markdown_table(headers, processed_results)
        
        # เพิ่ม title และสรุป
        response = ""
        if title:
            response += f"## {title}\n\n"
        
        response += f"**จำนวนรายการทั้งหมด: {len(results)} รายการ**\n\n"
        response += table
        
        return response
    
    def _process_data(self, results: List[Dict]) -> List[List[str]]:
        """ประมวลผลข้อมูลให้เหมาะสำหรับแสดงในตาราง"""
        processed = []
        
        for row in results:
            processed_row = []
            for key, value in row.items():
                # แปลงค่าต่างๆ ให้เป็น string ที่เหมาะสม
                if value is None:
                    processed_row.append("-")
                elif isinstance(value, (int, float)):
                    if isinstance(value, float) and value.is_integer():
                        processed_row.append(str(int(value)))
                    else:
                        processed_row.append(f"{value:,.0f}" if isinstance(value, float) else str(value))
                elif isinstance(value, str):
                    # ตัดความยาวหากเกิน max_cell_width
                    if len(value) > self.max_cell_width:
                        processed_row.append(value[:self.max_cell_width-3] + "...")
                    else:
                        processed_row.append(value)
                else:
                    processed_row.append(str(value))
            
            processed.append(processed_row)
        
        return processed
    
    def _format_markdown_table(self, headers: List[str], data: List[List[str]]) -> str:
        """สร้าง Markdown table"""
        
        # แปลง headers เป็นภาษาไทย
        thai_headers = self._translate_headers(headers)
        
        # สร้าง header row
        header_row = "| " + " | ".join(thai_headers) + " |"
        separator = "| " + " | ".join(["---"] * len(headers)) + " |"
        
        # สร้าง data rows
        data_rows = []
        for row in data:
            data_rows.append("| " + " | ".join(row) + " |")
        
        return "\n".join([header_row, separator] + data_rows)
    
    def _format_ascii_table(self, headers: List[str], data: List[List[str]]) -> str:
        """สร้าง ASCII table"""
        
        thai_headers = self._translate_headers(headers)
        
        # คำนวณความกว้างของแต่ละคอลัมน์
        col_widths = []
        for i in range(len(headers)):
            max_width = len(thai_headers[i])
            for row in data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(min(max_width, self.max_cell_width))
        
        # สร้าง border
        border = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        
        # สร้าง header
        header_row = "|"
        for i, header in enumerate(thai_headers):
            header_row += f" {header:<{col_widths[i]}} |"
        
        # สร้าง data rows
        table_rows = [border, header_row, border]
        
        for row in data:
            data_row = "|"
            for i in range(len(col_widths)):
                cell = row[i] if i < len(row) else ""
                if len(cell) > col_widths[i]:
                    cell = cell[:col_widths[i]-3] + "..."
                data_row += f" {cell:<{col_widths[i]}} |"
            table_rows.append(data_row)
        
        table_rows.append(border)
        
        return "\n".join(table_rows)
    
    def _format_html_table(self, headers: List[str], data: List[List[str]]) -> str:
        """สร้าง HTML table"""
        
        thai_headers = self._translate_headers(headers)
        
        html = ['<table border="1" style="border-collapse: collapse;">']
        
        # Header
        html.append("  <thead>")
        html.append("    <tr>")
        for header in thai_headers:
            html.append(f"      <th>{header}</th>")
        html.append("    </tr>")
        html.append("  </thead>")
        
        # Body
        html.append("  <tbody>")
        for row in data:
            html.append("    <tr>")
            for cell in row:
                html.append(f"      <td>{cell}</td>")
            html.append("    </tr>")
        html.append("  </tbody>")
        html.append("</table>")
        
        return "\n".join(html)
    
    def _format_simple_table(self, headers: List[str], data: List[List[str]]) -> str:
        """สร้างตารางแบบง่าย"""
        
        thai_headers = self._translate_headers(headers)
        result = []
        
        for i, row in enumerate(data, 1):
            result.append(f"รายการที่ {i}:")
            for j, header in enumerate(thai_headers):
                if j < len(row):
                    result.append(f"  {header}: {row[j]}")
            result.append("")  # เว้นบรรทัด
        
        return "\n".join(result)
    
    def _translate_headers(self, headers: List[str]) -> List[str]:
        """แปลง column names เป็นภาษาไทย"""
        
        translation_map = {
            # v_work_force
            'date': 'วันที่',
            'customer': 'ลูกค้า', 
            'project': 'โครงการ',
            'job_description_pm': 'ประเภทงาน PM',
            'detail': 'รายละเอียด',
            'service_group': 'ทีมงาน',
            
            # v_sales
            'customer_name': 'ชื่อลูกค้า',
            'year': 'ปี',
            'year_label': 'ปี',
            'total_revenue': 'รายได้รวม',
            'overhaul_num': 'งาน Overhaul',
            'replacement_num': 'งาน Replacement', 
            'service_num': 'งาน Service',
            'parts_num': 'งานอะไหล่',
            'product_num': 'งานผลิตภัณฑ์',
            'solution_num': 'งานโซลูชัน',
            
            # v_spare_part
            'product_code': 'รหัสสินค้า',
            'product_name': 'ชื่อสินค้า',
            'wh': 'คลัง',
            'balance_num': 'จำนวนคงเหลือ',
            'unit_price_num': 'ราคาต่อหน่วย',
            'total_num': 'มูลค่ารวม',
            
            # Common
            'count': 'จำนวน',
            'sum': 'รวม',
            'avg': 'เฉลี่ย',
            'max': 'สูงสุด',
            'min': 'ต่ำสุด'
        }
        
        return [translation_map.get(header, header) for header in headers]

# Integration function for orchestrator.py
def create_table_response(results: List[Dict], context_question: str = "", 
                         table_style: str = 'markdown') -> str:
    """สำหรับ integrate กับ orchestrator.py"""
    
    formatter = TableFormatter()
    
    # กำหนด title จากคำถาม
    if 'งาน' in context_question:
        title = 'ข้อมูลงานและการบริการ'
    elif 'อะไหล่' in context_question:
        title = 'ข้อมูลอะไหล่และคลังสินค้า'
    elif 'รายได้' in context_question or 'ยอดขาย' in context_question:
        title = 'ข้อมูลรายได้และยอดขาย'
    else:
        title = 'ผลลัพธ์การค้นหา'
    
    return formatter.format_results_as_table(
        results=results,
        style=table_style,
        title=title
    )

# สำหรับใช้ใน _generate_response() 
def format_results_as_table_response(results: List[Dict], question: str) -> str:
    """ฟังก์ชันหลักสำหรับสร้าง table response - แสดงเป็นตารางทั้งหมด"""
    
    if not results:
        return "ไม่พบข้อมูลที่ตรงกับคำถาม"
    
    # บังคับใช้ markdown table สำหรับทุกกรณี
    return create_table_response(results, question, 'markdown')

# Example usage:
if __name__ == "__main__":
    # ตัวอย่างข้อมูล
    sample_results = [
        {
            'date': '2025-07-04',
            'customer': 'สำนักเทคโนโลยีสารสนเทศ มหาวิทยาลัยขอนแก่น',
            'project': 'ม.ขอนแก่น', 
            'detail': 'งานบำรุงรักษารายปีครั้งที่ 2/3',
            'service_group': 'สุพรรณ จักรกริศน์ อวยชัย'
        },
        {
            'date': '2025-07-05',
            'customer': 'บริษัท อินสแตนท์ ดักส์ ซัพพลาย จำกัด',
            'project': 'PJ. บริษัท พรีเมียร์สตาร์ช จำกัด (มหาชน) มุกดาหาร',
            'detail': 'งานบำรุงรักษาในประกัน',
            'service_group': 'สุพรรณ จักรกริศน์ อวยชัย'
        }
    ]
    
    print(format_results_as_table_response(sample_results, "งานบำรุงรักษา"))