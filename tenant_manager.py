import os
import yaml
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class TenantConfig:
    """Configuration for a single tenant"""
    tenant_id: str
    name: str
    database_config: Dict[str, Any]
    knowledge_base_config: Dict[str, Any]
    api_keys: Dict[str, str]
    settings: Dict[str, Any]
    webhooks: Dict[str, str]
    contact_info: Dict[str, str]
    
    def __post_init__(self):
        # Validate required fields
        required_db_fields = ['host', 'port', 'database', 'user', 'password']
        for field in required_db_fields:
            if field not in self.database_config:
                raise ValueError(f"Missing required database field: {field}")

class TenantManager:
    """Manages multiple tenants and their configurations"""
    
    def __init__(self, config_file: str = "tenant_config.yaml"):
        self.config_file = Path(config_file)
        self.tenants: Dict[str, TenantConfig] = {}
        self.default_tenant = "company-a"
        self._load_config()
    
    def _load_config(self):
        """Load tenant configurations from YAML file"""
        try:
            if not self.config_file.exists():
                logger.warning(f"Config file {self.config_file} not found. Creating default config.")
                self._create_default_config()
                return
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # Load tenants
            for tenant_id, tenant_data in config_data.get('tenants', {}).items():
                self.tenants[tenant_id] = TenantConfig(
                    tenant_id=tenant_id,
                    name=tenant_data.get('name', f'Tenant {tenant_id}'),
                    database_config=tenant_data.get('database', {}),
                    knowledge_base_config=tenant_data.get('knowledge_base', {}),
                    api_keys=tenant_data.get('api_keys', {}),
                    settings=tenant_data.get('settings', {})
                )
            
            # Set default tenant
            if 'default_tenant' in config_data:
                self.default_tenant = config_data['default_tenant']
                
            logger.info(f"Loaded {len(self.tenants)} tenants: {list(self.tenants.keys())}")
            
        except Exception as e:
            logger.error(f"Error loading tenant config: {e}")
            self._create_default_config()
    
    def _create_default_config(self):
        """Create default tenant configuration"""
        default_config = {
            'default_tenant': 'company-a',
            'tenants': {
                'company-a': {
                    'name': 'SiamTech Main Office',
                    'database': {
                        'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
                        'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
                        'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtech_company_a'),
                        'user': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
                        'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
                    },
                    'knowledge_base': {
                        'id': os.getenv('KNOWLEDGE_BASE_ID_COMPANY_A', 'KJGWQPHFSM'),
                        'prefix': 'company-a',
                        'bucket': 'siamtech-kb-company-a'
                    },
                    'api_keys': {
                        'bedrock': os.getenv('AWS_ACCESS_KEY_ID', ''),
                        'openai': os.getenv('OPENAI_API_KEY', '')
                    },
                    'settings': {
                        'max_tokens': 1000,
                        'temperature': 0.7,
                        'allow_hybrid_search': True
                    },
                    'webhooks': {
                        'n8n_endpoint': f'http://n8n:5678/webhook/{tenant_id}-chat',
                        'health_check': f'http://n8n:5678/webhook/{tenant_id}-health'
                    },
                    'contact_info': {
                        'email': 'info@siamtech.co.th',
                        'phone': '02-123-4567',
                        'address': 'Bangkok, Thailand'
                    }
                },
                'company-b': {
                    'name': 'SiamTech Regional Office',
                    'database': {
                        'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
                        'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5432')),
                        'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtech_company_b'),
                        'user': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
                        'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
                    },
                    'knowledge_base': {
                        'id': os.getenv('KNOWLEDGE_BASE_ID_COMPANY_B', 'KJGWQPHFSM'),
                        'prefix': 'company-b',
                        'bucket': 'siamtech-kb-company-b'
                    },
                    'api_keys': {
                        'bedrock': os.getenv('AWS_ACCESS_KEY_ID', ''),
                        'openai': os.getenv('OPENAI_API_KEY', '')
                    },
                    'settings': {
                        'max_tokens': 1000,
                        'temperature': 0.7,
                        'allow_hybrid_search': True
                    }
                },
                'company-c': {
                    'name': 'SiamTech International',
                    'database': {
                        'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
                        'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5432')),
                        'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtech_company_c'),
                        'user': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
                        'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
                    },
                    'knowledge_base': {
                        'id': os.getenv('KNOWLEDGE_BASE_ID_COMPANY_C', 'KJGWQPHFSM'),
                        'prefix': 'company-c',
                        'bucket': 'siamtech-kb-company-c'
                    },
                    'api_keys': {
                        'bedrock': os.getenv('AWS_ACCESS_KEY_ID', ''),
                        'openai': os.getenv('OPENAI_API_KEY', '')
                    },
                    'settings': {
                        'max_tokens': 1000,
                        'temperature': 0.7,
                        'allow_hybrid_search': True
                    }
                }
            }
        }
        
        # Save default config
        with open(self.config_file, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Created default tenant config at {self.config_file}")
        
        # Load the created config
        self._load_config()
    
    def get_tenant(self, tenant_id: str) -> Optional[TenantConfig]:
        """Get tenant configuration by ID"""
        return self.tenants.get(tenant_id)
    
    def get_tenant_or_default(self, tenant_id: Optional[str] = None) -> TenantConfig:
        """Get tenant configuration or default if not found"""
        if not tenant_id:
            tenant_id = self.default_tenant
        
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            logger.warning(f"Tenant {tenant_id} not found, using default: {self.default_tenant}")
            tenant = self.tenants.get(self.default_tenant)
        
        if not tenant:
            raise ValueError(f"No tenant configuration found for {tenant_id} or default {self.default_tenant}")
        
        return tenant
    
    def list_tenants(self) -> Dict[str, str]:
        """List all available tenants"""
        return {tid: config.name for tid, config in self.tenants.items()}
    
    def validate_tenant(self, tenant_id: str) -> bool:
        """Validate if tenant exists and is properly configured"""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False
        
        # Check database config
        required_fields = ['host', 'port', 'database', 'user', 'password']
        for field in required_fields:
            if not tenant.database_config.get(field):
                logger.error(f"Tenant {tenant_id} missing database field: {field}")
                return False
        
        # Check knowledge base config
        if not tenant.knowledge_base_config.get('id'):
            logger.error(f"Tenant {tenant_id} missing knowledge base ID")
            return False
        
        return True
    
    def get_database_config(self, tenant_id: str) -> Dict[str, Any]:
        """Get database configuration for a tenant"""
        tenant = self.get_tenant_or_default(tenant_id)
        return tenant.database_config
    
    def get_knowledge_base_config(self, tenant_id: str) -> Dict[str, Any]:
        """Get knowledge base configuration for a tenant"""
        tenant = self.get_tenant_or_default(tenant_id)
        return tenant.knowledge_base_config
    
    def get_api_keys(self, tenant_id: str) -> Dict[str, str]:
        """Get API keys for a tenant"""
        tenant = self.get_tenant_or_default(tenant_id)
        return tenant.api_keys
    
    def get_settings(self, tenant_id: str) -> Dict[str, Any]:
        """Get settings for a tenant"""
        tenant = self.get_tenant_or_default(tenant_id)
        return tenant.settings
    
    def add_tenant(self, tenant_id: str, config: Dict[str, Any]):
        """Add a new tenant configuration"""
        if tenant_id in self.tenants:
            raise ValueError(f"Tenant {tenant_id} already exists")
        
        # Validate required fields
        required_sections = ['name', 'database', 'knowledge_base']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required section: {section}")
        
        # Create tenant config
        tenant_config = TenantConfig(
            tenant_id=tenant_id,
            name=config['name'],
            database_config=config['database'],
            knowledge_base_config=config['knowledge_base'],
            api_keys=config.get('api_keys', {}),
            settings=config.get('settings', {})
        )
        
        self.tenants[tenant_id] = tenant_config
        
        # Save to file
        self._save_config()
        
        logger.info(f"Added new tenant: {tenant_id}")
    
    def _save_config(self):
        """Save current configuration to file"""
        config_data = {
            'default_tenant': self.default_tenant,
            'tenants': {}
        }
        
        for tenant_id, tenant in self.tenants.items():
            config_data['tenants'][tenant_id] = {
                'name': tenant.name,
                'database': tenant.database_config,
                'knowledge_base': tenant.knowledge_base_config,
                'api_keys': tenant.api_keys,
                'settings': tenant.settings
            }
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
    
    def test_tenant_connections(self, tenant_id: str) -> Dict[str, Any]:
        """Test connections for a tenant"""
        tenant = self.get_tenant_or_default(tenant_id)
        results = {
            'tenant_id': tenant_id,
            'tenant_name': tenant.name,
            'database': {'status': 'unknown', 'error': None},
            'knowledge_base': {'status': 'unknown', 'error': None}
        }
        
        # Test database connection
        try:
            import psycopg2
            conn = psycopg2.connect(**tenant.database_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            results['database']['status'] = 'connected'
        except Exception as e:
            results['database']['status'] = 'error'
            results['database']['error'] = str(e)
        
        # Test knowledge base (placeholder)
        try:
            # This would test Knowledge Base connection
            # For now, just check if config exists
            if tenant.knowledge_base_config.get('id'):
                results['knowledge_base']['status'] = 'configured'
            else:
                results['knowledge_base']['status'] = 'not_configured'
        except Exception as e:
            results['knowledge_base']['status'] = 'error'
            results['knowledge_base']['error'] = str(e)
        
        return results


# Global instance
tenant_manager = TenantManager()

# Convenience functions
def get_tenant_config(tenant_id: str) -> TenantConfig:
    """Get tenant configuration"""
    return tenant_manager.get_tenant_or_default(tenant_id)

def get_tenant_database_config(tenant_id: str) -> Dict[str, Any]:
    """Get database configuration for tenant"""
    return tenant_manager.get_database_config(tenant_id)

def get_tenant_knowledge_base_config(tenant_id: str) -> Dict[str, Any]:
    """Get knowledge base configuration for tenant"""
    return tenant_manager.get_knowledge_base_config(tenant_id)

def list_available_tenants() -> Dict[str, str]:
    """List all available tenants"""
    return tenant_manager.list_tenants()

def validate_tenant_id(tenant_id: str) -> bool:
    """Validate tenant ID"""
    return tenant_manager.validate_tenant(tenant_id)


# Test usage
if __name__ == "__main__":
    # Test tenant manager
    print("🏢 Available Tenants:")
    for tid, name in list_available_tenants().items():
        print(f"  - {tid}: {name}")
    
    print("\n🔧 Testing tenant configurations:")
    for tenant_id in ['company-a', 'company-b', 'company-c']:
        print(f"\n--- {tenant_id} ---")
        config = get_tenant_config(tenant_id)
        print(f"Name: {config.name}")
        print(f"Database: {config.database_config['host']}:{config.database_config['port']}")
        print(f"Knowledge Base: {config.knowledge_base_config['id']}")
        print(f"Valid: {validate_tenant_id(tenant_id)}")
        
        # Test connections
        test_results = tenant_manager.test_tenant_connections(tenant_id)
        print(f"Database Status: {test_results['database']['status']}")
        print(f"Knowledge Base Status: {test_results['knowledge_base']['status']}")