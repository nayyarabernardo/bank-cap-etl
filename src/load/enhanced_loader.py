import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class EnhancedLoader:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_multiple_formats(self, df: pd.DataFrame, filename_prefix: str, 
                            base_curr: str, target_curr: str) -> Dict[str, str]:
        """Salva dados em múltiplos formatos"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        files_created = {}
        
        # 1. CSV (padrão)
        csv_file = self.output_dir / f"{filename_prefix}_{base_curr}_{target_curr}_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        files_created['csv'] = str(csv_file)
        
        # 2. JSON (estruturado)
        json_file = self.output_dir / f"{filename_prefix}_{base_curr}_{target_curr}_{timestamp}.json"
        data = {
            "metadata": {
                "conversion": f"{base_curr}_to_{target_curr}",
                "timestamp": timestamp,
                "exchange_rate": df['_exchange_rate'].iloc[0] if '_exchange_rate' in df.columns else 0,
                "total_records": len(df)
            },
            "data": df.to_dict(orient='records')
        }
        
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        files_created['json'] = str(json_file)
        
        # 3. Excel (se necessário)
        try:
            excel_file = self.output_dir / f"{filename_prefix}_{base_curr}_{target_curr}_{timestamp}.xlsx"
            df.to_excel(excel_file, index=False)
            files_created['excel'] = str(excel_file)
        except ImportError:
            logger.warning("openpyxl não instalado. Pulando exportação Excel.")
        
        # 4. Relatório em texto
        from src.transform.enhanced_transformer import EnhancedTransformer
        transformer = EnhancedTransformer()
        report = transformer.generate_report(df, base_curr, target_curr)
        
        report_file = self.output_dir / f"report_{base_curr}_{target_curr}_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        files_created['report'] = str(report_file)
        
        # 5. CSV simplificado (apenas colunas principais)
        main_cols = ['Rank', 'Bank name', 
                    'assets_usd_billion', f'assets_{target_curr.lower()}_billion',
                    '_exchange_rate', '_exchange_date']
        available_cols = [col for col in main_cols if col in df.columns]
        
        simple_csv = self.output_dir / f"summary_{base_curr}_{target_curr}_{timestamp}.csv"
        df[available_cols].to_csv(simple_csv, index=False)
        files_created['summary_csv'] = str(simple_csv)
        
        logger.info(f"Dados salvos em {len(files_created)} formatos")
        for format_name, filepath in files_created.items():
            logger.info(f"  • {format_name}: {Path(filepath).name}")
        
        return files_created