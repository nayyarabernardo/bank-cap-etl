import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging
from config.settings import PROCESSED_DIR

logger = logging.getLogger(__name__)

class IncrementalCSVLoader:
    def __init__(self, output_dir: Path = PROCESSED_DIR):
        self.output_dir = output_dir / "outputs"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.output_dir / "load_history.json"
    
    def _load_history(self) -> dict:
        """Carrega histórico de cargas"""
        if self.history_file.exists():
            with open(self.history_file, 'r') as f:
                return json.load(f)
        return {"loads": []}
    
    def _save_history(self, history: dict):
        """Salva histórico de cargas"""
        with open(self.history_file, 'w') as f:
            json.dump(history, f, indent=2)
    
    def load_incremental(self, df: pd.DataFrame, filename: str = "bank_market_cap_gbp") -> str:
        """Carrega dados incrementalmente mantendo histórico"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        daily_file = self.output_dir / f"{filename}_{date_str}.csv"
        
        timestamped_file = self.output_dir / f"{filename}_{timestamp}.csv"
        
        if daily_file.exists():
            existing_df = pd.read_csv(daily_file)
            
            key_column = 'Bank name' if 'Bank name' in df.columns else df.columns[0]
            
            existing_keys = set(existing_df[key_column].astype(str))
            new_records = df[~df[key_column].astype(str).isin(existing_keys)]
            
            if not new_records.empty:
                updated_df = pd.concat([existing_df, new_records], ignore_index=True)
                updated_df.to_csv(daily_file, index=False)
                
                logger.info(f"Adicionadas {len(new_records)} novas linhas ao arquivo diário")
            else:
                updated_df = existing_df
                logger.info("Nenhum novo registro para adicionar")
        else:
            df.to_csv(daily_file, index=False)
            updated_df = df
            logger.info(f"Criado novo arquivo diário com {len(df)} linhas")
        
        updated_df.to_csv(timestamped_file, index=False)
        
        history = self._load_history()
        history["loads"].append({
            "timestamp": timestamp,
            "date": date_str,
            "file": str(timestamped_file.name),
            "daily_file": str(daily_file.name),
            "rows_loaded": len(updated_df),
            "new_rows": len(df) if not daily_file.exists() else len(new_records) if 'new_records' in locals() else 0
        })
        self._save_history(history)
        
        self._update_consolidated(updated_df, filename)
        
        return str(timestamped_file)
    
    def _update_consolidated(self, df: pd.DataFrame, filename: str):
        """Atualiza arquivo consolidado com todos os dados"""
        consolidated_file = self.output_dir / f"{filename}_consolidated.csv"
        
        if consolidated_file.exists():
            consolidated_df = pd.read_csv(consolidated_file)
            key_column = 'Bank name' if 'Bank name' in df.columns else df.columns[0]
            consolidated_df = consolidated_df[
                ~consolidated_df[key_column].astype(str).isin(df[key_column].astype(str))
            ]
            final_df = pd.concat([consolidated_df, df], ignore_index=True)
        else:
            final_df = df
        
        final_df.to_csv(consolidated_file, index=False)
        logger.info(f"Arquivo consolidado atualizado: {len(final_df)} linhas")
    
    def get_load_stats(self) -> dict:
        """Obtém estatísticas das cargas"""
        history = self._load_history()
        if not history["loads"]:
            return {"total_loads": 0}
        
        last_load = history["loads"][-1]
        return {
            "total_loads": len(history["loads"]),
            "last_load": last_load,
            "first_load": history["loads"][0] if history["loads"] else None
        }

def load_to_csv(df: pd.DataFrame, filename: str = "bank_market_cap_gbp") -> str:
    """Função principal de carga"""
    loader = IncrementalCSVLoader()
    return loader.load_incremental(df, filename)