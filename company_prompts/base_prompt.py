from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseCompanyPrompt(ABC):
    """🎯 Base class สำหรับ Company-specific prompts"""
    
    def __init__(self, company_config: Dict[str, Any]):
        self.company_id = company_config.get('company_id')
        self.company_name = company_config.get('name')
        self.business_type = company_config.get('business_type')
        self.language = company_config.get('language', 'th')
        self.model = company_config.get('model')
        
        # Load company-specific configurations
        self.business_rules = self._load_business_rules()
        self.schema_mappings = self._load_schema_mappings()
        self.response_style = self._load_response_style()
        
        # Statistics
        self.usage_stats = {
            'queries_processed': 0,
            'successful_generations': 0,
            'last_used': None
        }
        
        logger.info(f"✅ {self.__class__.__name__} initialized for {self.company_name}")
    
    @abstractmethod
    def generate_sql_prompt(self, question: str, schema_info: Dict[str, Any]) -> str:
        """🎯 Generate company-specific SQL prompt"""
        pass
    
    @abstractmethod
    def format_response(self, question: str, results: List[Dict], metadata: Dict) -> str:
        """🎨 Format response in company-specific style"""
        pass
    
    @abstractmethod 
    def _load_business_rules(self) -> Dict[str, Any]:
        """📋 Load company-specific business rules"""
        pass
    
    @abstractmethod
    def _load_schema_mappings(self) -> Dict[str, Any]:
        """🗄️ Load schema mappings for this company"""
        pass
    
    def _load_response_style(self) -> Dict[str, Any]:
        """🎨 Load response formatting preferences"""
        return {
            'currency_format': 'THB',
            'number_format': 'comma_separated',
            'date_format': 'DD/MM/YYYY',
            'tone': 'professional'
        }
    
    def validate_sql(self, sql: str) -> bool:
        """🔍 Basic SQL validation"""
        # Common validation rules
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
        sql_upper = sql.upper()
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                logger.warning(f"🚨 Dangerous SQL keyword detected: {keyword}")
                return False
        
        return True
    
    def get_statistics(self) -> Dict[str, Any]:
        """📊 Get usage statistics"""
        success_rate = 0
        if self.usage_stats['queries_processed'] > 0:
            success_rate = (self.usage_stats['successful_generations'] / 
                          self.usage_stats['queries_processed']) * 100
        
        return {
            'company_id': self.company_id,
            'company_name': self.company_name,
            'prompt_class': self.__class__.__name__,
            'queries_processed': self.usage_stats['queries_processed'],
            'successful_generations': self.usage_stats['successful_generations'],
            'success_rate': round(success_rate, 2),
            'last_used': self.usage_stats['last_used']
        }