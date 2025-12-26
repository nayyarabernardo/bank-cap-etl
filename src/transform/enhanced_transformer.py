import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class EnhancedTransformer:
    def __init__(self):
        self.currency_symbols = {
            'USD': '$',
            'EUR': '€',
            'GBP': '£',
            'JPY': '¥',
            'BRL': 'R$'
        }
    
    def format_currency_columns(self, df: pd.DataFrame, base_curr: str, target_curr: str) -> pd.DataFrame:
        """Adiciona colunas formatadas para exibição"""
        df = df.copy()
        
        # Colunas numéricas
        usd_col = 'assets_usd_billion'
        target_col = f'assets_{target_curr.lower()}_billion'
        
        if usd_col in df.columns and target_col in df.columns:
            # Formata como string com símbolo
            base_symbol = self.currency_symbols.get(base_curr, base_curr)
            target_symbol = self.currency_symbols.get(target_curr, target_curr)
            
            df[f'{base_curr}_formatted'] = df[usd_col].apply(
                lambda x: f"{base_symbol}{x:,.2f}B"
            )
            
            df[f'{target_curr}_formatted'] = df[target_col].apply(
                lambda x: f"{target_symbol}{x:,.2f}B"
            )
            
            # Diferença percentual
            df['conversion_rate'] = df[target_col] / df[usd_col]
            
            logger.info(f"Colunas formatadas adicionadas: {base_curr}_formatted, {target_curr}_formatted")
        
        return df
    
    def create_summary_stats(self, df: pd.DataFrame, base_curr: str, target_curr: str) -> Dict:
        """Cria estatísticas resumidas"""
        usd_col = 'assets_usd_billion'
        target_col = f'assets_{target_curr.lower()}_billion'
        
        if usd_col not in df.columns or target_col not in df.columns:
            return {}
        
        stats = {
            'total_banks': len(df),
            'total_assets_usd': df[usd_col].sum(),
            'total_assets_converted': df[target_col].sum(),
            'average_assets_usd': df[usd_col].mean(),
            'average_assets_converted': df[target_col].mean(),
            'top_bank_usd': df.loc[df[usd_col].idxmax(), 'Bank name'] if 'Bank name' in df.columns else 'N/A',
            'top_bank_converted': df.loc[df[target_col].idxmax(), 'Bank name'] if 'Bank name' in df.columns else 'N/A',
            'conversion_rate': df['_exchange_rate'].iloc[0] if '_exchange_rate' in df.columns else 0,
            'conversion_date': df['_exchange_date'].iloc[0] if '_exchange_date' in df.columns else 'N/A'
        }
        
        return stats
    
    def generate_report(self, df: pd.DataFrame, base_curr: str, target_curr: str, 
                       output_path: str = None) -> str:
        """Gera um relatório em texto"""
        stats = self.create_summary_stats(df, base_curr, target_curr)
        
        report_lines = [
            "=" * 70,
            f"RELATÓRIO DE CONVERSÃO: {base_curr} → {target_curr}",
            "=" * 70,
            f"Data da conversão: {stats.get('conversion_date', 'N/A')}",
            f"Taxa de câmbio: 1 {base_curr} = {stats.get('conversion_rate', 0):.6f} {target_curr}",
            "",
            f"ESTATÍSTICAS:",
            f"  • Total de bancos analisados: {stats.get('total_banks', 0)}",
            f"  • Ativos totais em {base_curr}: {stats.get('total_assets_usd', 0):,.2f} bilhões",
            f"  • Ativos totais em {target_curr}: {stats.get('total_assets_converted', 0):,.2f} bilhões",
            f"  • Média por banco ({base_curr}): {stats.get('average_assets_usd', 0):,.2f} bilhões",
            f"  • Média por banco ({target_curr}): {stats.get('average_assets_converted', 0):,.2f} bilhões",
            f"  • Maior banco ({base_curr}): {stats.get('top_bank_usd', 'N/A')}",
            f"  • Maior banco ({target_curr}): {stats.get('top_bank_converted', 'N/A')}",
            "",
            "TOP 5 BANCOS POR ATIVOS:",
        ]
        
        # Ordena por ativos em USD
        if 'assets_usd_billion' in df.columns and 'Bank name' in df.columns:
            top_5 = df.nlargest(5, 'assets_usd_billion')[['Bank name', 'assets_usd_billion', 
                                                         f'assets_{target_curr.lower()}_billion']]
            
            for idx, row in top_5.iterrows():
                report_lines.append(
                    f"  {int(row.name)+1}. {row['Bank name']}: "
                    f"{base_curr} {row['assets_usd_billion']:,.2f}B → "
                    f"{target_curr} {row[f'assets_{target_curr.lower()}_billion']:,.2f}B"
                )
        
        report_lines.extend([
            "",
            "=" * 70,
            "FIM DO RELATÓRIO",
            "=" * 70
        ])
        
        report = "\n".join(report_lines)
        
        if output_path:
            from pathlib import Path
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                f.write(report)
            
            logger.info(f"Relatório salvo em: {output_file}")
        
        return report