import os
from dataclasses import dataclass
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

@dataclass
class TenantConfig:
    """Enhanced Tenant configuration with business intelligence"""
    tenant_id: str
    name: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    model_name: str
    language: str
    business_type: str
    key_metrics: List[str]

class TenantConfigManager:
    """🔧 Manages multi-tenant configurations"""
    
    def __init__(self):
        self.tenant_configs = self._load_enhanced_tenant_configs()
    
    def _load_enhanced_tenant_configs(self) -> Dict[str, TenantConfig]:
        """Load enhanced tenant configurations"""
        configs = {}
        
        # Company A Configuration - Service & Maintenance Focus (เปลี่ยนจาก Enterprise)
        # ใช้ชื่อ attribute เดิมแต่เปลี่ยนเนื้อหาให้สอดคล้องกับธุรกิจใหม่
        configs['company-a'] = TenantConfig(
            tenant_id='company-a',
            name='Siamtemp Bangkok HQ',  # ชื่อเดิม
            db_host=os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
            db_port=int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
            db_name=os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
            db_user=os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
            db_password=os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123'),
            model_name=os.getenv('MODEL_COMPANY_A', 'llama3.1:8b'),
            language='th',
            # 🔧 เปลี่ยนประเภทธุรกิจจาก 'enterprise_software' เป็น 'service_maintenance'
            business_type='service_maintenance',
            # 🔧 เปลี่ยน key_metrics ให้สอดคล้องกับธุรกิจบริการซ่อมบำรุงและอะไหล่
            # เดิม: ['salary', 'budget', 'project_count', 'team_size']
            # ใหม่: ตัวชี้วัดที่เกี่ยวข้องกับงานบริการ ลูกค้า และอะไหล่
            key_metrics=[
                'customer_count',      # จำนวนลูกค้า (แทน salary)
                'service_revenue',     # รายได้จากงานบริการ (แทน budget)
                'maintenance_jobs',    # จำนวนงานบำรุงรักษา (แทน project_count)
                'spare_parts_stock'    # สต็อกอะไหล่ (แทน team_size)
            ]
        )
        
        # Company B Configuration - Tourism Focus (คงเดิม เพราะยังไม่เปลี่ยน)
        configs['company-b'] = TenantConfig(
            tenant_id='company-b',
            name='Siamtemp Chiang Mai Regional',
            db_host=os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
            db_port=int(os.getenv('POSTGRES_PORT_COMPANY_B', '5433')),
            db_name=os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
            db_user=os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
            db_password=os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123'),
            model_name=os.getenv('MODEL_COMPANY_B', 'gemma2:9b'),
            language='th',
            business_type='tourism_hospitality',
            key_metrics=['client_count', 'regional_budget', 'tourism_projects', 'local_team']
        )
        
        # Company C Configuration - International Focus (คงเดิม)
        configs['company-c'] = TenantConfig(
            tenant_id='company-c',
            name='Siamtemp International Office',
            db_host=os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
            db_port=int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
            db_name=os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
            db_user=os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
            db_password=os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123'),
            model_name=os.getenv('MODEL_COMPANY_C', 'llama3.2:3b'),
            language='en',
            business_type='international_software',
            key_metrics=['global_clients', 'revenue_usd', 'international_projects', 'cross_border_team']
        )
        
        logger.info(f"✅ Loaded {len(configs)} tenant configurations")
        # 🔧 เพิ่ม log เพื่อยืนยันการเปลี่ยนแปลงของ Company A
        logger.info(f"📋 Company A business type: {configs['company-a'].business_type}")
        logger.info(f"📊 Company A key metrics: {configs['company-a'].key_metrics}")
        
        return configs
    
    def get_config(self, tenant_id: str) -> TenantConfig:
        """Get configuration for specific tenant"""
        if tenant_id not in self.tenant_configs:
            # Default fallback - ยังคงใช้ company-a เป็น default
            logger.warning(f"⚠️ Tenant {tenant_id} not found, using company-a as default")
            return self.tenant_configs['company-a']
        
        return self.tenant_configs[tenant_id]
    
    def get_all_tenants(self) -> List[str]:
        """Get list of all tenant IDs"""
        return list(self.tenant_configs.keys())
    
    def get_business_context(self, tenant_id: str) -> Dict[str, str]:
        """🔧 เพิ่ม method ใหม่เพื่อให้ส่วนอื่นๆ ของระบบเข้าถึงบริบทธุรกิจได้ง่ายขึ้น"""
        config = self.get_config(tenant_id)
        
        # สร้าง business context ที่เหมาะสมกับแต่ละประเภทธุรกิจ
        business_contexts = {
            'service_maintenance': {
                'industry': 'Air Conditioning Service & Maintenance',
                'primary_customers': 'Industrial facilities, Office buildings, Hotels',
                'main_services': 'Preventive Maintenance (PM), Repair, Spare Parts Supply',
                'key_equipment': 'Chillers, Air Conditioners, HVAC Systems',
                'business_model': 'Service contracts, On-demand repairs, Parts sales'
            },
            'tourism_hospitality': {
                'industry': 'Tourism & Hospitality Technology',
                'primary_customers': 'Hotels, Tourist attractions, Regional businesses',
                'main_services': 'Website development, Mobile apps, Management systems',
                'key_equipment': 'Servers, POS systems, Booking platforms',
                'business_model': 'Project-based development, Regional partnerships'
            },
            'international_software': {
                'industry': 'International Software Development',
                'primary_customers': 'Global corporations, Multinational companies',
                'main_services': 'Enterprise software, Cross-border solutions',
                'key_equipment': 'Cloud infrastructure, International servers',
                'business_model': 'Global contracts, Multi-currency billing'
            }
        }
        
        return business_contexts.get(config.business_type, business_contexts['service_maintenance'])
    
    def is_service_business(self, tenant_id: str) -> bool:
        """🔧 Helper method เพื่อตรวจสอบว่าเป็นธุรกิจบริการหรือไม่"""
        config = self.get_config(tenant_id)
        return config.business_type in ['service_maintenance', 'tourism_hospitality']
    
    def get_relevant_tables(self, tenant_id: str) -> List[str]:
        """🔧 Method ใหม่เพื่อกำหนดตารางที่เกี่ยวข้องตามประเภทธุรกิจ"""
        config = self.get_config(tenant_id)
        
        if config.business_type == 'service_maintenance':
            # สำหรับธุรกิจบริการซ่อมบำรุง ตารางที่สำคัญคือ
            return ['sales2024', 'sales2023', 'sales2022', 'sales2025', 
                   'spare_part', 'spare_part2', 'work_force']
        elif config.business_type == 'tourism_hospitality':
            # สำหรับธุรกิจท่องเที่ยว ตารางเดิม
            return ['employees', 'projects', 'employee_projects', 'clients']
        else:
            # Default หรือธุรกิจอื่นๆ
            return ['employees', 'projects', 'employee_projects']