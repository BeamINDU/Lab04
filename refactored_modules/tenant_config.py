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
        
        # Company A Configuration - Enterprise Focus
        configs['company-a'] = TenantConfig(
            tenant_id='company-a',
            name='Siamtemp Bangkok HQ',
            db_host=os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
            db_port=int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
            db_name=os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
            db_user=os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
            db_password=os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123'),
            model_name=os.getenv('MODEL_COMPANY_A', 'llama3.1:8b'),
            language='th',
            business_type='enterprise_software',
            key_metrics=['salary', 'budget', 'project_count', 'team_size']
        )
        
        # Company B Configuration - Tourism Focus  
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
        
        # Company C Configuration - International Focus
        configs['company-c'] = TenantConfig(
            tenant_id='company-c',
            name='Siamtemp International',
            db_host=os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
            db_port=int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
            db_name=os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
            db_user=os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
            db_password=os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123'),
            model_name=os.getenv('MODEL_COMPANY_C', 'phi3:14b'),
            language='en',
            business_type='global_operations',
            key_metrics=['usd_revenue', 'international_clients', 'global_projects', 'multi_currency']
        )
        
        return configs
    
    def get_config(self, tenant_id: str) -> TenantConfig:
        """Get configuration for specific tenant"""
        if tenant_id not in self.tenant_configs:
            raise ValueError(f"Unknown tenant: {tenant_id}")
        return self.tenant_configs[tenant_id]
    
    def list_tenants(self) -> List[str]:
        """List all available tenants"""
        return list(self.tenant_configs.keys())
    
    def validate_tenant(self, tenant_id: str) -> bool:
        """Validate if tenant exists"""
        return tenant_id in self.tenant_configs