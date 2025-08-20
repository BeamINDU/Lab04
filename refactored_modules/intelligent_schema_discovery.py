# refactored_modules/intelligent_schema_discovery_fixed.py
# 🔧 แก้ไขปัญหาทั้งหมดที่เจอ

import time
import re
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class IntelligentSchemaDiscovery:
    """🧠 Fixed version - แก้ไขปัญหาทั้งหมด"""
    
    def __init__(self, database_handler):
        self.db_handler = database_handler
        
        # เก็บ cache ของข้อมูลที่เรียนรู้แล้ว
        self.learned_schemas = {
            'departments': {},
            'positions': {},
            'projects': {},
            'relationships': {}
        }
        
        self.cache_timestamps = {}
        self.cache_duration = 1800  # 30 นาที
        
        logger.info("🧠 Fixed Intelligent Schema Discovery system initialized")
    
    async def get_contextual_schema(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 Main function - Fixed version"""
        
        # ขั้นตอนที่ 1: วิเคราะห์คำถาม
        question_analysis = self._analyze_question_deeply(question)
        logger.info(f"🔍 Question analysis result: {question_analysis}")
        
        # ขั้นตอนที่ 2: เตรียมข้อมูลที่จำเป็น
        required_data = await self._gather_required_data(tenant_id, question_analysis)
        
        # ขั้นตอนที่ 3: สร้าง schema context
        contextual_schema = self._build_intelligent_context(question_analysis, required_data, tenant_id)
        
        return contextual_schema
    
    def _analyze_question_deeply(self, question: str) -> Dict[str, Any]:
        """🔍 Fixed question analysis"""
        
        question_lower = question.lower()
        
        analysis_result = {
            'question_type': 'unknown',
            'main_entities': [],
            'specific_keywords': [],
            'needs_relationships': False,
            'needs_counting': False,
            'needs_filtering': False,
            'confidence_level': 0.0
        }
        
        # วิเคราะห์ประเภทคำถาม
        if any(word in question_lower for word in ['กี่คน', 'จำนวน', 'นับ', 'how many', 'count']):
            analysis_result['question_type'] = 'counting'
            analysis_result['needs_counting'] = True
            analysis_result['confidence_level'] += 0.3
        
        elif any(word in question_lower for word in ['ใคร', 'รายชื่อ', 'แสดง', 'list', 'show', 'who']):
            analysis_result['question_type'] = 'listing'
            analysis_result['confidence_level'] += 0.3
        
        elif any(word in question_lower for word in ['รับผิดชอบ', 'ทำงาน', 'assigned', 'working', 'responsible']):
            analysis_result['question_type'] = 'relationship'
            analysis_result['needs_relationships'] = True
            analysis_result['confidence_level'] += 0.4
        
        # วิเคราะห์ entities หลัก
        if any(word in question_lower for word in ['แผนก', 'department', 'ฝ่าย']):
            analysis_result['main_entities'].append('departments')
            analysis_result['confidence_level'] += 0.2
            
            # หาคำสำคัญเฉพาะสำหรับแผนก
            if any(keyword in question_lower for keyword in ['it', 'ไอที', 'เทคโนโลยี', 'tempnology', 'information']):
                analysis_result['specific_keywords'].append('department_it')
        
        if any(word in question_lower for word in ['ตำแหน่ง', 'position', 'งาน', 'job']):
            analysis_result['main_entities'].append('positions')
            analysis_result['confidence_level'] += 0.2
            
            # หาคำสำคัญเฉพาะสำหรับตำแหน่ง
            position_keywords = ['developer', 'frontend', 'backend', 'designer', 'manager']
            for keyword in position_keywords:
                if keyword in question_lower:
                    analysis_result['specific_keywords'].append(f'position_{keyword}')
        
        if any(word in question_lower for word in ['โปรเจค', 'project', 'งาน', 'ระบบ']):
            analysis_result['main_entities'].append('projects')
            analysis_result['confidence_level'] += 0.2
        
        # วิเคราะห์ความต้องการ filtering
        if any(word in question_lower for word in ['ที่', 'ใน', 'ของ', 'where', 'in', 'with']):
            analysis_result['needs_filtering'] = True
        
        return analysis_result
    
    async def _gather_required_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """📊 Fixed data gathering with proper error handling"""
        
        required_data = {}
        
        # ดึงข้อมูลแผนก หากจำเป็น
        if 'departments' in analysis['main_entities']:
            try:
                required_data['departments'] = await self._get_department_data(tenant_id, analysis)
            except Exception as e:
                logger.error(f"Failed to get department data: {e}")
                required_data['departments'] = self._get_fallback_department_data()
        
        # ดึงข้อมูลตำแหน่ง หากจำเป็น
        if 'positions' in analysis['main_entities']:
            try:
                required_data['positions'] = await self._get_position_data(tenant_id, analysis)
            except Exception as e:
                logger.error(f"Failed to get position data: {e}")
                required_data['positions'] = self._get_fallback_position_data()
        
        # ดึงข้อมูลโปรเจค หากจำเป็น - 🆕 Fixed
        if 'projects' in analysis['main_entities']:
            try:
                required_data['projects'] = await self._get_project_data(tenant_id, analysis)
            except Exception as e:
                logger.error(f"Failed to get project data: {e}")
                required_data['projects'] = self._get_fallback_project_data()
        
        # ดึงข้อมูลความสัมพันธ์ หากจำเป็น
        if analysis['needs_relationships']:
            try:
                required_data['relationships'] = await self._get_relationship_patterns(tenant_id)
            except Exception as e:
                logger.error(f"Failed to get relationship data: {e}")
                required_data['relationships'] = {'has_relationships': False}
        
        return required_data
    
    async def _get_department_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """🏢 Fixed department data fetching"""
        
        cache_key = f"{tenant_id}_departments"
        if self._is_cache_valid(cache_key):
            logger.info(f"📊 Using cached department data for {tenant_id}")
            return self.learned_schemas['departments'][tenant_id]
        
        try:
            # 🔧 Fixed: ใช้ _get_database_connection แทน get_database_connection
            conn = self.db_handler._get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            department_data = {
                'all_departments': [],
                'relevant_departments': [],
                'department_stats': {}
            }
            
            # ดึงแผนกทั้งหมดพร้อมจำนวนพนักงาน
            cursor.execute("""
                SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
                FROM employees 
                GROUP BY department 
                ORDER BY employee_count DESC
            """)
            
            for row in cursor.fetchall():
                dept_name, count, avg_salary = row
                department_data['all_departments'].append(dept_name)
                department_data['department_stats'][dept_name] = {
                    'employee_count': count,
                    'avg_salary': float(avg_salary) if avg_salary else 0
                }
            
            # หากมี keyword เฉพาะ ให้หาแผนกที่เกี่ยวข้อง
            if analysis['specific_keywords']:
                for keyword in analysis['specific_keywords']:
                    if keyword.startswith('department_'):
                        dept_type = keyword.replace('department_', '')
                        relevant_depts = self._find_matching_departments(
                            department_data['all_departments'], dept_type
                        )
                        department_data['relevant_departments'].extend(relevant_depts)
            
            cursor.close()
            conn.close()
            
            # บันทึกลง cache
            self.learned_schemas['departments'][tenant_id] = department_data
            self.cache_timestamps[cache_key] = time.time()
            
            logger.info(f"✅ Loaded department data for {tenant_id}: {len(department_data['all_departments'])} departments")
            return department_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get department data for {tenant_id}: {e}")
            return self._get_fallback_department_data()
    
    async def _get_position_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """👔 Fixed position data fetching"""
        
        cache_key = f"{tenant_id}_positions"
        if self._is_cache_valid(cache_key):
            return self.learned_schemas['positions'][tenant_id]
        
        try:
            # 🔧 Fixed: ใช้ _get_database_connection
            conn = self.db_handler._get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            position_data = {
                'all_positions': [],
                'relevant_positions': [],
                'position_stats': {}
            }
            
            # ดึงตำแหน่งทั้งหมดพร้อมข้อมูลสถิติ
            cursor.execute("""
                SELECT position, COUNT(*) as position_count, 
                       AVG(salary) as avg_salary, department
                FROM employees 
                GROUP BY position, department 
                ORDER BY position_count DESC
            """)
            
            for row in cursor.fetchall():
                position, count, avg_salary, department = row
                if position not in position_data['all_positions']:
                    position_data['all_positions'].append(position)
                
                position_data['position_stats'][position] = {
                    'count': count,
                    'avg_salary': float(avg_salary) if avg_salary else 0,
                    'department': department
                }
            
            # หากมี keyword เฉพาะ ให้หาตำแหน่งที่เกี่ยวข้อง
            if analysis['specific_keywords']:
                for keyword in analysis['specific_keywords']:
                    if keyword.startswith('position_'):
                        pos_type = keyword.replace('position_', '')
                        relevant_positions = self._find_matching_positions(
                            position_data['all_positions'], pos_type
                        )
                        position_data['relevant_positions'].extend(relevant_positions)
            
            cursor.close()
            conn.close()
            
            # บันทึกลง cache
            self.learned_schemas['positions'][tenant_id] = position_data
            self.cache_timestamps[cache_key] = time.time()
            
            return position_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get position data for {tenant_id}: {e}")
            return self._get_fallback_position_data()
    
    async def _get_project_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """📋 🆕 Added missing project data method"""
        
        cache_key = f"{tenant_id}_projects"
        if self._is_cache_valid(cache_key):
            return self.learned_schemas['projects'][tenant_id]
        
        try:
            conn = self.db_handler._get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            project_data = {
                'all_projects': [],
                'project_stats': {}
            }
            
            # ดึงโปรเจคทั้งหมด
            cursor.execute("""
                SELECT name, client, budget, status
                FROM projects 
                ORDER BY budget DESC
                LIMIT 20
            """)
            
            for row in cursor.fetchall():
                project_name, client, budget, status = row
                project_data['all_projects'].append(project_name)
                project_data['project_stats'][project_name] = {
                    'client': client,
                    'budget': float(budget) if budget else 0,
                    'status': status
                }
            
            cursor.close()
            conn.close()
            
            # บันทึกลง cache
            self.learned_schemas['projects'][tenant_id] = project_data
            self.cache_timestamps[cache_key] = time.time()
            
            return project_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get project data for {tenant_id}: {e}")
            return self._get_fallback_project_data()
    
    async def _get_relationship_patterns(self, tenant_id: str) -> Dict[str, Any]:
        """🤝 Fixed relationship data fetching"""
        
        cache_key = f"{tenant_id}_relationships"
        if self._is_cache_valid(cache_key):
            return self.learned_schemas['relationships'][tenant_id]
        
        try:
            conn = self.db_handler._get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            relationship_data = {
                'employee_project_count': 0,
                'unique_roles': [],
                'allocation_patterns': {},
                'has_relationships': False
            }
            
            # ตรวจสอบว่ามีความสัมพันธ์หรือไม่
            cursor.execute("SELECT COUNT(*) FROM employee_projects")
            count = cursor.fetchone()[0]
            relationship_data['employee_project_count'] = count
            relationship_data['has_relationships'] = count > 0
            
            if count > 0:
                # ดึงบทบาทที่มีอยู่
                cursor.execute("SELECT DISTINCT role FROM employee_projects ORDER BY role")
                relationship_data['unique_roles'] = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            # บันทึกลง cache
            self.learned_schemas['relationships'][tenant_id] = relationship_data
            self.cache_timestamps[cache_key] = time.time()
            
            return relationship_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get relationship data for {tenant_id}: {e}")
            return {'has_relationships': False, 'employee_project_count': 0}
    
    def _find_matching_departments(self, all_departments: List[str], dept_type: str) -> List[str]:
        """🔍 หาแผนกที่ตรงกับ keyword"""
        
        matching_departments = []
        
        patterns = {
            'it': ['information', 'tempnology', 'it', 'ไอที', 'เทคโนโลยี'],
            'sales': ['sales', 'marketing', 'ขาย', 'การตลาด'],
            'management': ['management', 'จัดการ', 'บริหาร', 'ผู้บริหาร']
        }
        
        if dept_type in patterns:
            search_patterns = patterns[dept_type]
            
            for department in all_departments:
                dept_lower = department.lower()
                if any(pattern in dept_lower for pattern in search_patterns):
                    matching_departments.append(department)
        
        return matching_departments
    
    def _find_matching_positions(self, all_positions: List[str], pos_type: str) -> List[str]:
        """🔍 หาตำแหน่งที่ตรงกับ keyword"""
        
        matching_positions = []
        
        patterns = {
            'developer': ['developer', 'dev', 'programmer', 'โปรแกรม', 'พัฒนา'],
            'frontend': ['frontend', 'front-end', 'หน้าบ้าน'],
            'backend': ['backend', 'back-end', 'หลังบ้าน'],
            'designer': ['designer', 'design', 'ออกแบบ', 'ดีไซน์'],
            'manager': ['manager', 'ผจก', 'ผู้จัดการ', 'หัวหน้า']
        }
        
        if pos_type in patterns:
            search_patterns = patterns[pos_type]
            
            for position in all_positions:
                pos_lower = position.lower()
                if any(pattern in pos_lower for pattern in search_patterns):
                    matching_positions.append(position)
        
        return matching_positions
    
    def _build_intelligent_context(self, analysis: Dict[str, Any], 
                                 required_data: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """🎯 สร้าง context ที่ชาญฉลาด"""
        
        context = {
            'schema_type': 'intelligent_contextual',
            'question_analysis': analysis,
            'discovered_at': datetime.now().isoformat(),
            'tenant_id': tenant_id,
            'guidance': {},
            'specific_data': {}
        }
        
        # สร้างคำแนะนำเฉพาะ
        if analysis['question_type'] == 'counting':
            context['guidance']['query_type'] = 'COUNT query required'
            context['guidance']['sql_hints'] = [
                'ใช้ COUNT(*) สำหรับนับจำนวน',
                'ใช้ GROUP BY หากต้องการแบ่งกลุ่ม',
                'ไม่ต้อง JOIN หากข้อมูลอยู่ในตาราง employees เท่านั้น'
            ]
        
        elif analysis['question_type'] == 'relationship':
            context['guidance']['query_type'] = 'JOIN query required'
            context['guidance']['sql_hints'] = [
                'ต้องใช้ JOIN ระหว่าง employees, employee_projects, และ projects',
                'ใช้ e.id = ep.employee_id และ ep.project_id = p.id',
                'เลือกฟิลด์ที่เหมาะสม: e.name, p.name, ep.role'
            ]
        
        # รวมข้อมูลเฉพาะที่จำเป็น
        if 'departments' in required_data:
            dept_data = required_data['departments']
            if dept_data['relevant_departments']:
                context['specific_data']['departments'] = dept_data['relevant_departments']
            else:
                context['specific_data']['departments'] = dept_data['all_departments']
        
        if 'positions' in required_data:
            pos_data = required_data['positions']
            if pos_data['relevant_positions']:
                context['specific_data']['positions'] = pos_data['relevant_positions']
            else:
                context['specific_data']['positions'] = pos_data['all_positions'][:10]
        
        if 'relationships' in required_data:
            rel_data = required_data['relationships']
            context['specific_data']['has_employee_projects'] = rel_data['has_relationships']
            if rel_data['has_relationships']:
                context['specific_data']['available_roles'] = rel_data['unique_roles']
        
        return context
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """⏰ ตรวจสอบ cache validity"""
        if cache_key not in self.cache_timestamps:
            return False
        
        cache_age = time.time() - self.cache_timestamps[cache_key]
        return cache_age < self.cache_duration
    
    def _get_fallback_department_data(self) -> Dict[str, Any]:
        """🔄 Fallback department data"""
        logger.warning("⚠️ Using fallback department data")
        return {
            'all_departments': ['Information tempnology', 'Sales & Marketing', 'Management'],
            'relevant_departments': [],
            'department_stats': {}
        }
    
    def _get_fallback_position_data(self) -> Dict[str, Any]:
        """🔄 Fallback position data"""
        logger.warning("⚠️ Using fallback position data")
        return {
            'all_positions': ['Frontend Developer', 'Backend Developer', 'Designer', 'Manager'],
            'relevant_positions': [],
            'position_stats': {}
        }
    
    def _get_fallback_project_data(self) -> Dict[str, Any]:
        """🔄 🆕 Added fallback project data"""
        logger.warning("⚠️ Using fallback project data")
        return {
            'all_projects': ['CRM System', 'Mobile App', 'Website'],
            'project_stats': {}
        }


class IntelligentPromptBuilder:
    """🎯 Fixed Prompt Builder"""
    
    def __init__(self, tenant_configs):
        self.tenant_configs = tenant_configs
        logger.info("🎯 Fixed Intelligent Prompt Builder initialized")
    
    def build_contextual_prompt(self, question: str, tenant_id: str, 
                              intelligent_context: Dict[str, Any]) -> str:
        """🎯 Fixed contextual prompt building"""
        
        config = self.tenant_configs[tenant_id]
        analysis = intelligent_context.get('question_analysis', {})
        guidance = intelligent_context.get('guidance', {})
        specific_data = intelligent_context.get('specific_data', {})
        
        prompt_sections = []
        
        # ส่วนที่ 1: บริบทบริษัท
        prompt_sections.append(f"คุณคือ PostgreSQL Expert สำหรับ {config.name}")
        prompt_sections.append("")
        
        # ส่วนที่ 2: บริบทธุรกิจ
        business_context = self._get_business_context(tenant_id)
        prompt_sections.append(business_context)
        prompt_sections.append("")
        
        # ส่วนที่ 3: โครงสร้างฐานข้อมูล
        prompt_sections.append("📊 โครงสร้างฐานข้อมูล:")
        prompt_sections.append("• employees: id, name, department, position, salary, hire_date, email")
        prompt_sections.append("• projects: id, name, client, budget, status, start_date, end_date, temp_stack")
        prompt_sections.append("• employee_projects: employee_id, project_id, role, allocation")
        prompt_sections.append("")
        
        # ส่วนที่ 4: ข้อมูลเฉพาะที่เกี่ยวข้อง
        if specific_data:
            prompt_sections.append("🎯 ข้อมูลจริงที่เกี่ยวข้องกับคำถามนี้:")
            
            if 'departments' in specific_data and specific_data['departments']:
                dept_list = "', '".join(specific_data['departments'])
                prompt_sections.append(f"🏢 แผนกที่มีอยู่จริง: '{dept_list}'")
            
            if 'positions' in specific_data and specific_data['positions']:
                pos_list = "', '".join(specific_data['positions'][:8])
                prompt_sections.append(f"👔 ตำแหน่งที่เกี่ยวข้อง: '{pos_list}'")
            
            if 'has_employee_projects' in specific_data:
                if specific_data['has_employee_projects']:
                    prompt_sections.append("🤝 มีข้อมูลความสัมพันธ์พนักงาน-โปรเจค")
                    if 'available_roles' in specific_data:
                        roles = "', '".join(specific_data['available_roles'][:5])
                        prompt_sections.append(f"🎭 บทบาทที่มี: '{roles}'")
                else:
                    prompt_sections.append("⚠️ ไม่มีข้อมูลความสัมพันธ์พนักงาน-โปรเจค")
            
            prompt_sections.append("")
        
        # ส่วนที่ 5: คำแนะนำเฉพาะ
        if guidance:
            prompt_sections.append("🎯 คำแนะนำเฉพาะสำหรับคำถามนี้:")
            
            if 'query_type' in guidance:
                prompt_sections.append(f"• ประเภทคำถาม: {guidance['query_type']}")
            
            if 'sql_hints' in guidance:
                for hint in guidance['sql_hints']:
                    prompt_sections.append(f"• {hint}")
            
            prompt_sections.append("")
        
        # ส่วนที่ 6: กฎสำคัญ
        prompt_sections.append("🔧 กฎสำคัญ:")
        prompt_sections.append("1. ใช้เฉพาะชื่อแผนก/ตำแหน่งที่ระบุข้างบนเท่านั้น")
        prompt_sections.append("2. ใช้ ILIKE '%keyword%' สำหรับการค้นหา")
        prompt_sections.append("3. ใช้ LIMIT 20 เสมอ")
        prompt_sections.append("4. ตรวจสอบ syntax ให้ถูกต้อง")
        
        if analysis.get('question_type') == 'counting':
            prompt_sections.append("5. ใช้ COUNT(*) สำหรับการนับ และ GROUP BY สำหรับการแบ่งกลุ่ม")
        elif analysis.get('question_type') == 'relationship':
            prompt_sections.append("5. ใช้ JOIN เมื่อต้องการข้อมูลจากหลายตาราง")
        else:
            prompt_sections.append("5. หลีกเลี่ยง JOIN หากไม่จำเป็น")
        
        prompt_sections.append("")
        
        # ส่วนที่ 7: คำถามและคำสั่ง
        prompt_sections.append(f"❓ คำถาม: {question}")
        prompt_sections.append("")
        prompt_sections.append("สร้าง PostgreSQL query ที่ถูกต้องและแม่นยำ:")
        
        return "\n".join(prompt_sections)
    
    def _get_business_context(self, tenant_id: str) -> str:
        """🏢 Business context"""
        contexts = {
            'company-a': """🏢 บริบท: สำนักงานใหญ่ กรุงเทพมฯ - Enterprise Banking & E-commerce
💰 งบประมาณ: 800K-3M+ บาท | ลูกค้า: ธนาคาร, บริษัทใหญ่""",

            'company-b': """🏨 บริบท: สาขาภาคเหนือ เชียงใหม่ - Tourism & Hospitality
💰 งบประมาณ: 300K-800K บาท | ลูกค้า: โรงแรม, ท่องเที่ยว""",

            'company-c': """🌍 บริบท: International Office - Global Software Solutions
💰 งบประมาณ: 1M-4M+ USD | ลูกค้า: บริษัทข้ามชาติ"""
        }
        
        return contexts.get(tenant_id, contexts['company-a'])


class EnhancedSchemaIntegration:
    """🔗 Fixed Integration class"""
    
    def __init__(self, database_handler, tenant_configs):
        self.schema_discovery = IntelligentSchemaDiscovery(database_handler)
        self.prompt_builder = IntelligentPromptBuilder(tenant_configs)
        logger.info("🔗 Fixed Enhanced Schema Integration initialized")
    
    async def generate_intelligent_sql_prompt(self, question: str, tenant_id: str) -> str:
        """🎯 Fixed intelligent prompt generation"""
        
        try:
            # ขั้นตอนที่ 1: วิเคราะห์และหาข้อมูล
            intelligent_context = await self.schema_discovery.get_contextual_schema(question, tenant_id)
            
            # ขั้นตอนที่ 2: สร้าง prompt
            intelligent_prompt = self.prompt_builder.build_contextual_prompt(
                question, tenant_id, intelligent_context
            )
            
            logger.info(f"✅ Generated intelligent prompt for {tenant_id}: {len(intelligent_prompt)} chars")
            return intelligent_prompt
            
        except Exception as e:
            logger.error(f"❌ Failed to generate intelligent prompt: {e}")
            
            # fallback
            return self._create_fallback_prompt(question, tenant_id)
    
    def _create_fallback_prompt(self, question: str, tenant_id: str) -> str:
        """🔄 Fallback prompt"""
        
        return f"""คุณคือ PostgreSQL Expert

📊 โครงสร้างฐานข้อมูล:
• employees: id, name, department, position, salary, hire_date, email
• projects: id, name, client, budget, status, start_date, end_date, temp_stack
• employee_projects: employee_id, project_id, role, allocation

🔧 กฎสำคัญ:
1. ใช้ LIMIT 20
2. ใช้ ILIKE สำหรับการค้นหา
3. ตรวจสอบ syntax ให้ถูกต้อง
4. ใช้ชื่อแผนกจริง: 'Information tempnology' (ไม่ใช่ 'IT')

คำถาม: {question}

สร้าง PostgreSQL query:"""