import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import logging
from config.settings import EXCHANGE_API_KEY, EXCHANGE_API_URL, RAW_DIR

logger = logging.getLogger(__name__)

class ExchangeRateExtractor:
    def __init__(self):
        self.api_key = EXCHANGE_API_KEY
        self.raw_dir = RAW_DIR / "exchange_rates"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_all_rates(self, base_currency: str = "EUR") -> Dict:
        """Extrai TODAS as taxas de câmbio da API (sem filtros)"""
        params = {
            "base": base_currency  # Podemos usar EUR como base padrão
        }
        
        headers = {
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(
                EXCHANGE_API_URL,
                params=params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("success"):
                logger.info(f"Taxas extraídas com base {base_currency}. Total: {len(data.get('rates', {}))} moedas")
            else:
                logger.warning("API retornou success=False")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na API de câmbio: {e}")
            raise
    
    def extract_with_base(self, base_currency: str = "USD") -> Dict:
        """Extrai taxas com uma base específica"""
        return self.extract_all_rates(base_currency)
    
    def save_daily_json(self, data: Dict, base_currency: str = "USD") -> str:
        """Salva dados como JSON com timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exchange_rates_{base_currency}_{timestamp}.json"
        filepath = self.raw_dir / filename
        
        # Adiciona metadados
        data_with_metadata = {
            **data,
            "_extraction_metadata": {
                "timestamp": timestamp,
                "base_currency": base_currency,
                "source": "apilayer.com",
                "total_currencies": len(data.get("rates", {}))
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(data_with_metadata, f, indent=2)
        
        logger.info(f"Dados salvos em: {filepath}")
        return str(filepath)
    
    def get_latest_rate_file(self) -> Optional[Path]:
        """Obtém o arquivo mais recente"""
        json_files = list(self.raw_dir.glob("*.json"))
        if not json_files:
            return None
        return max(json_files, key=lambda x: x.stat().st_mtime)
    
    def get_rate_from_file(self, base_currency: str, target_currency: str, 
                          filepath: Optional[Path] = None) -> Optional[float]:
        """Obtém taxa específica de um arquivo JSON"""
        if filepath is None:
            filepath = self.get_latest_rate_file()
        
        if filepath is None:
            return None
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        rates = data.get("rates", {})
        
        # Se a base do arquivo é diferente, precisamos converter
        file_base = data.get("base", "EUR")
        
        if file_base == base_currency:
            # Base igual, retorna direto
            return rates.get(target_currency)
        else:
            # Precisa converter
            # Exemplo: arquivo tem base EUR, queremos USD->GBP
            # taxa = (EUR->GBP) / (EUR->USD)
            usd_rate = rates.get(base_currency)  # EUR->USD
            gbp_rate = rates.get(target_currency)  # EUR->GBP
            
            if usd_rate and gbp_rate and usd_rate != 0:
                return gbp_rate / usd_rate
        
        return None

def extract_and_save_exchange_rate(base_currency: str = "USD") -> Dict:
    """Função principal de extração (sem filtro de target)"""
    extractor = ExchangeRateExtractor()
    data = extractor.extract_with_base(base_currency)
    extractor.save_daily_json(data, base_currency)
    return data

# Função de compatibilidade (mantém API antiga)
def extract_exchange_rate_old(base_currency: str = "USD", target_currency: str = "GBP") -> Dict:
    """Função antiga para compatibilidade (não usar filtro)"""
    logger.warning("Esta função está depreciada. Use extract_and_save_exchange_rate()")
    return extract_and_save_exchange_rate(base_currency)