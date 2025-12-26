import requests
import pandas as pd
from io import StringIO
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional  
from config.settings import BANKS_WIKI_URL, RAW_DIR

logger = logging.getLogger(__name__)

class BanksExtractor:
    def __init__(self):
        self.url = BANKS_WIKI_URL
        self.raw_dir = RAW_DIR / "banks"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_table(self) -> pd.DataFrame:
        """Extrai tabela de bancos da Wikipedia"""
        headers = {
            "User-Agent": "Mozilla/5.0 (ETL Pipeline)"
        }
        
        try:
            response = requests.get(self.url, headers=headers, timeout=15)
            response.raise_for_status()
            
            html_io = StringIO(response.text)
            tables = pd.read_html(html_io)
            
            df = tables[0].copy()
            df['_extracted_at'] = datetime.now()
            
            logger.info(f"Extraídas {len(df)} linhas da Wikipedia")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao extrair da Wikipedia: {e}")
            raise
    
    def save_daily_data(self, df: pd.DataFrame) -> str:
        """Salva dados diários em JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"banks_wikipedia_{timestamp}.json"
        filepath = self.raw_dir / filename
        
        # Converte DataFrame para dict com orientação 'records'
        data = {
            "data": df.to_dict(orient='records'),
            "_metadata": {
                "timestamp": timestamp,
                "source": "wikipedia.org",
                "rows": len(df),
                "columns": list(df.columns)
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Dados dos bancos salvos em: {filepath}")
        return str(filepath)
    
    def get_latest_banks_file(self) -> Optional[Path]:
        """Obtém o arquivo mais recente"""
        json_files = list(self.raw_dir.glob("*.json"))
        if not json_files:
            return None
        return max(json_files, key=lambda x: x.stat().st_mtime)

def extract_and_save_banks() -> pd.DataFrame:
    """Função principal de extração"""
    extractor = BanksExtractor()
    df = extractor.extract_table()
    extractor.save_daily_data(df)
    return df