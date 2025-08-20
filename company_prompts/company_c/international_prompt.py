# company_prompts/company_c/international_prompt.py
# 🔧 Fixed version

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from company_prompts.base_prompt import BaseCompanyPrompt
from typing import Dict, Any, List
from datetime import datetime
from shared_components.logging_config import logger

class InternationalPrompt(BaseCompanyPrompt):
    """🌍 FIXED International Prompt - Compatible with BaseCompanyPrompt"""
    
    def __init__(self, company_config: Dict[str, Any]):
        # 🔧 Initialize parent class first
        super().__init__(company_config)
        
        # 🌍 International data - moved AFTER super().__init__()
        self.international_data = {
            'markets': {
                'north_america': {
                    'countries': ['USA', 'Canada'],
                    'currency': 'USD',
                    'clients': ['MegaCorp International']
                },
                'europe': {
                    'countries': ['UK', 'Germany'],
                    'currency': 'EUR/GBP',
                    'clients': ['Education Global Network']
                },
                'asia_pacific': {
                    'countries': ['Singapore', 'Australia'],
                    'currency': 'SGD/AUD',
                    'clients': ['Global Finance Corp']
                }
            },
            'currencies': {
                'primary': ['USD', 'EUR', 'GBP'],
                'secondary': ['SGD', 'AUD'],
                'rates': {'USD': 1.0, 'EUR': 0.85, 'GBP': 0.80}
            },
            'major_clients': [
                'MegaCorp International (USA)',
                'Education Global Network (UK)', 
                'Global Finance Corp (Singapore)'
            ],
            'keywords': {
                'financial': ['revenue', 'budget', 'usd', 'profit'],
                'global': ['international', 'global', 'worldwide'],
                'clients': ['client', 'customer', 'megacorp'],
                'projects': ['project', 'platform', 'system']
            }
        }
        
        logger.info(f"🌍 InternationalPrompt initialized for {self.company_name}")
    
    # ========================================================================
    # 🎯 REQUIRED METHODS from BaseCompanyPrompt
    # ========================================================================
    
    async def process_question(self, question: str) -> Dict[str, Any]:
        """🎯 Main processing method for international queries"""
        
        try:
            self.usage_stats['queries_processed'] += 1
            self.usage_stats['last_used'] = datetime.now().isoformat()
            
            if self._is_greeting(question):
                return self._create_international_greeting()
            elif self._is_financial_query(question):
                return self._create_financial_response(question)
            elif self._is_global_query(question):
                return self._create_global_response(question)
            else:
                return self._create_general_response(question)
                
        except Exception as e:
            logger.error(f"❌ International processing failed: {e}")
            return {
                'success': False,
                'answer': f"System error: {str(e)}",
                'error': str(e),
                'tenant_id': self.company_id
            }
    
    def generate_sql_prompt(self, question: str, schema_info: Dict[str, Any]) -> str:
        """🎯 Generate international business SQL prompt"""
        
        query_focus = self._detect_query_focus(question)
        currency_hint = self._get_currency_hint(query_focus)
        
        prompt = f"""You are an International Business Analyst for {self.company_name}

🌍 Business Context: Global Software Solutions & Cross-border Operations
💱 Currencies: USD (primary), EUR, GBP, SGD, AUD
🌎 Markets: North America, Europe, Asia-Pacific
🎯 Focus: {query_focus}

📊 Database Schema:
• employees: id, name, department, position, salary, hire_date, email
• projects: id, name, client, budget, status, start_date, end_date, temp_stack
• employee_projects: employee_id, project_id, role, allocation

💱 Currency Context: {currency_hint}

Question: {question}

Generate PostgreSQL query for international operations:"""

        return prompt
    
    def format_response(self, question: str, results: List[Dict], metadata: Dict) -> str:
        """🎨 Format international business response"""
        
        if not results:
            return f"No international data found for: {question}"
        
        response = f"🌍 Global Business Analysis - {self.company_name}\n\n"
        response += f"Query: {question}\n\n"
        
        for i, row in enumerate(results[:15], 1):
            response += f"{i:2d}. "
            for key, value in row.items():
                if 'budget' in key.lower() and isinstance(value, (int, float)):
                    response += f"{key}: ${value:,.0f} USD, "
                elif 'client' in key.lower() and value:
                    region = self._get_client_region(value)
                    response += f"{key}: {value} {region}, "
                else:
                    response += f"{key}: {value}, "
            response = response.rstrip(', ') + "\n"
        
        response += f"\n💡 Global Operations: {len(results)} records found"
        
        return response
    
    def _load_business_rules(self) -> Dict[str, Any]:
        """📋 International business rules"""
        return {
            'focus': 'international_operations_multi_currency',
            'primary_currency': 'USD',
            'supported_currencies': list(self.international_data['currencies']['rates'].keys()),
            'client_base': 'global_enterprise'
        }
    
    def _load_schema_mappings(self) -> Dict[str, Any]:
        """🗄️ International schema mappings"""
        return {
            'main_tables': ['employees', 'projects', 'employee_projects'],
            'currency_fields': ['budget', 'salary'],
            'international_keywords': self.international_data['keywords']
        }
    
    # ========================================================================
    # 🔧 HELPER METHODS
    # ========================================================================
    
    def _is_greeting(self, question: str) -> bool:
        greetings = ['hello', 'hi', 'help', 'who are you', 'สวัสดี']
        return any(word in question.lower() for word in greetings)
    
    def _is_financial_query(self, question: str) -> bool:
        return any(keyword in question.lower() 
                  for keyword in self.international_data['keywords']['financial'])
    
    def _is_global_query(self, question: str) -> bool:
        return any(keyword in question.lower()
                  for keyword in self.international_data['keywords']['global'])
    
    def _detect_query_focus(self, question: str) -> str:
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['revenue', 'budget', 'usd']):
            return 'financial_analysis'
        elif any(word in question_lower for word in ['client', 'customer']):
            return 'client_management'
        else:
            return 'general_operations'
    
    def _get_currency_hint(self, query_focus: str) -> str:
        hints = {
            'financial_analysis': 'Multi-currency revenue analysis, USD conversion required',
            'client_management': 'Client contracts in various currencies',
            'general_operations': 'Global operations with multi-currency support'
        }
        return hints.get(query_focus, 'Multi-currency international operations')
    
    def _get_client_region(self, client_name: str) -> str:
        client_lower = client_name.lower()
        
        for market, data in self.international_data['markets'].items():
            for client in data['clients']:
                if any(word in client_lower for word in client.lower().split()):
                    region_names = {
                        'north_america': '🇺🇸',
                        'europe': '🇪🇺', 
                        'asia_pacific': '🌏'
                    }
                    return region_names.get(market, '')
        return ''
    
    def _create_international_greeting(self) -> Dict[str, Any]:
        answer = f"""Hello! I'm the AI Assistant for {self.company_name}

🌍 Global Software Solutions & Cross-border Operations

Our International Expertise:
• Multi-currency software platforms (USD, EUR, GBP, SGD, AUD)
• Cross-border payment systems
• Global compliance frameworks

🌎 Market Coverage:
• North America: USA, Canada
• Europe: UK, Germany
• Asia-Pacific: Singapore, Australia

🏢 Major Clients:
• MegaCorp International (USA) - $2.8M USD
• Global Finance Corp (Singapore) - $3.2M USD

How can I help you with our global operations today?"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'international_greeting_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_financial_response(self, question: str) -> Dict[str, Any]:
        answer = f"""🌍 Global Financial Analysis - {self.company_name}

Query: {question}

💰 Multi-Currency Revenue Portfolio:

1. Global E-commerce Platform - MegaCorp International 🇺🇸
   • Contract Value: $2,800,000 USD
   • Status: Active

2. International Banking API - Global Finance Corp 🌏
   • Contract Value: $3,200,000 USD

💱 Currency Breakdown:
• USD Revenue: $6,000,000+ (direct + converted)
• Active Currencies: USD, GBP, SGD, AUD"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'international_financial_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_global_response(self, question: str) -> Dict[str, Any]:
        answer = f"""🌎 Global Operations Overview - {self.company_name}

Query: {question}

🌍 International Project Portfolio:

1. Cross-border Payment System 🇦🇺
   • Budget: $1,800,000 USD
   • Status: Completed

2. Global Compliance Platform 🇩🇪
   • Budget: $3,850,000 USD
   • Status: In Development

🌐 Global Infrastructure:
• Time Zone Coverage: 24/7 operations
• Team Distribution: 8 international developers"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'international_global_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_general_response(self, question: str) -> Dict[str, Any]:
        answer = f"""🌍 {self.company_name} - Global Software Solutions

Question: {question}

Our International Capabilities:
• Cross-border software development
• Multi-currency system architecture
• Global compliance and security

🌎 Geographic Presence:
• Development Teams: Bangkok (HQ), Remote Global
• Client Base: USA, UK, Singapore, Australia

💡 Ask me about:
• International project portfolios
• Multi-currency revenue analysis  
• Global client relationships"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'international_general_{self.model}',
            'tenant_id': self.company_id
        }