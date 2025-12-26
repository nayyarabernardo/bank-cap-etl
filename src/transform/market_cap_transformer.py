import pandas as pd
import json
from pathlib import Path
from typing import Tuple, Optional
import logging
from datetime import datetime
import re
from src.extract.web_extractor import BanksExtractor
from src.extract.api_extractor import ExchangeRateExtractor
from config.settings import PROCESSED_DIR

logger = logging.getLogger(__name__)

class MarketCapTransformer:
    def __init__(self, base_currency: str = "USD", target_currency: str = "GBP"):
        self.base_currency = base_currency
        self.target_currency = target_currency
        self.processed_dir = PROCESSED_DIR
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_exchange_rate(self) -> float:
        """Obtém taxa de câmbio específica dos dados extraídos"""
        extractor = ExchangeRateExtractor()
        filepath = extractor.get_latest_rate_file()
        
        if filepath is None:
            raise FileNotFoundError("Nenhum arquivo de taxa de câmbio encontrado")
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        rates = data.get("rates", {})
        file_base = data.get("base", "EUR")
        
        logger.info(f"Arquivo base: {file_base}, Procurando: {self.base_currency}->{self.target_currency}")
        
        # Método 1: Base igual ao que queremos
        if file_base == self.base_currency:
            rate = rates.get(self.target_currency)
            if rate:
                logger.info(f"Taxa direta encontrada: {rate}")
                return rate
        
        # Método 2: Converter entre bases
        # Se arquivo tem base EUR e queremos USD->GBP:
        # USD->GBP = (EUR->GBP) / (EUR->USD)
        base_rate = rates.get(self.base_currency)  # Ex: EUR->USD
        target_rate = rates.get(self.target_currency)  # Ex: EUR->GBP
        
        if base_rate and target_rate and base_rate != 0:
            calculated_rate = target_rate / base_rate
            logger.info(f"Taxa calculada: {self.base_currency}->{self.target_currency} = {calculated_rate}")
            logger.info(f"  (EUR->{self.base_currency}: {base_rate}, EUR->{self.target_currency}: {target_rate})")
            return calculated_rate
        
        # Método 3: Usar API extractor para calcular
        rate = extractor.get_rate_from_file(self.base_currency, self.target_currency, filepath)
        if rate:
            return rate
        
        # Método 4: Tentar outras bases no arquivo
        logger.error(f"Não foi possível obter taxa {self.base_currency}->{self.target_currency}")
        logger.error(f"Moedas disponíveis no arquivo: {list(rates.keys())}")
        
        raise ValueError(f"Taxa {self.base_currency}->{self.target_currency} não encontrada")
    
    def _find_market_cap_column(self, df: pd.DataFrame) -> str:
        """Encontra a coluna de market cap/assets"""
        target_columns = [
            'Total assets (2025) (US$ billion)',
            'Market capitalization(US$ billion)',
            'Market cap (US$ billion)',
            'market_cap_usd_billion',
            'Total assets',
            'Assets'
        ]
        
        for col in target_columns:
            if col in df.columns:
                logger.info(f"Coluna encontrada: {col}")
                return col
        
        raise ValueError(f"Nenhuma coluna de market cap encontrada. Colunas: {list(df.columns)}")
    
    def _clean_numeric_value(self, value) -> float:
        """Converte qualquer valor para float"""
        if pd.isna(value):
            return 0.0
        
        try:
            if isinstance(value, (int, float)):
                return float(value)
            
            str_value = str(value).strip()
            str_value = re.sub(r'[^\d\.\,\-]', '', str_value)
            
            if ',' in str_value and '.' not in str_value:
                str_value = str_value.replace(',', '.')
            
            str_value = str_value.replace(',', '')
            
            if not str_value or str_value == '-':
                return 0.0
            
            return float(str_value)
            
        except Exception as e:
            logger.warning(f"Erro ao converter valor '{value}': {e}")
            return 0.0
    
    def load_latest_data(self) -> Tuple[pd.DataFrame, float]:
        """Carrega dados mais recentes"""
        # Bancos
        banks_extractor = BanksExtractor()
        banks_file = banks_extractor.get_latest_banks_file()
        
        if not banks_file:
            raise FileNotFoundError("Nenhum arquivo de bancos encontrado")
        
        with open(banks_file, 'r') as f:
            banks_data = json.load(f)
        
        banks_df = pd.DataFrame(banks_data["data"])
        logger.info(f"Carregados {len(banks_df)} bancos")
        
        # Taxa de câmbio (FILTRO AQUI!)
        exchange_rate = self._get_exchange_rate()
        logger.info(f"Taxa {self.base_currency}->{self.target_currency}: {exchange_rate}")
        
        return banks_df, exchange_rate
    
    def transform(self) -> pd.DataFrame:
        """Transforma assets/market cap para moeda destino"""
        banks_df, exchange_rate = self.load_latest_data()
        
        # Cópia do DataFrame
        df = banks_df.copy()
        
        # Identifica coluna
        asset_column = self._find_market_cap_column(df)
        
        # Limpa e converte valores
        df['assets_usd_billion'] = df[asset_column].apply(self._clean_numeric_value)
        
        # Converte para moeda destino (FILTRO APLICADO AQUI)
        target_col = f'assets_{self.target_currency.lower()}_billion'
        df[target_col] = (df['assets_usd_billion'] * exchange_rate).round(3)
        
        # Adiciona metadados
        df['_transformed_at'] = datetime.now()
        df['_exchange_rate'] = exchange_rate
        df['_exchange_from'] = self.base_currency
        df['_exchange_to'] = self.target_currency
        df['_exchange_date'] = datetime.now().strftime("%Y-%m-%d")
        
        # Log de exemplo
        logger.info("Exemplo de conversão:")
        for idx, row in df.head(3).iterrows():
            bank_name = row.get('Bank name', f"Bank_{idx}")
            logger.info(f"  {bank_name}: "
                       f"{self.base_currency} {row['assets_usd_billion']}B → "
                       f"{self.target_currency} {row[target_col]}B")
        
        logger.info(f"Transformação concluída. Total: {len(df)} registros")
        
        return df

def transform_market_cap_to_currency(
    base_currency: str = "USD", 
    target_currency: str = "GBP"
) -> pd.DataFrame:
    """Função principal com filtros de moeda"""
    transformer = MarketCapTransformer(base_currency, target_currency)
    return transformer.transform()

# Função de compatibilidade (mantém nome antigo)
def transform_market_cap_to_gbp() -> pd.DataFrame:
    """Função antiga para compatibilidade (USD->GBP padrão)"""
    return transform_market_cap_to_currency("USD", "GBP")

