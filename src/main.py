import logging
from datetime import datetime
from pathlib import Path
from src.extract.api_extractor import extract_and_save_exchange_rate
from src.extract.web_extractor import extract_and_save_banks
from src.transform.market_cap_transformer import transform_market_cap_to_currency

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_etl_pipeline(
    base_currency: str = "USD",
    target_currency: str = "GBP"
):
    """Executa o pipeline ETL completo"""
    logger.info("=" * 50)
    logger.info(f"INICIANDO PIPELINE ETL: {base_currency} -> {target_currency}")
    logger.info("=" * 50)
    
    try:
        logger.info("Fase 1: Extração de dados")
        
        logger.info(f"Extraindo taxas de câmbio (base: {base_currency})...")
        exchange_data = extract_and_save_exchange_rate(base_currency)
        
        logger.info("Extraindo dados dos bancos da Wikipedia...")
        banks_df = extract_and_save_banks()
        
        logger.info("Fase 2: Transformação de dados")
        logger.info(f"Convertendo para {target_currency}...")
        
        transformed_df = transform_market_cap_to_currency(
            base_currency=base_currency,
            target_currency=target_currency
        )
        
        logger.info("Fase 3: Carga de dados")
        from src.load.file_loader import load_to_csv
        output_file = load_to_csv(transformed_df, f"bank_market_cap_{target_currency.lower()}")
        
        logger.info(f"Pipeline concluído com sucesso!")
        logger.info(f"Arquivo gerado: {output_file}")
        logger.info(f"Total de registros: {len(transformed_df)}")
        logger.info(f"Taxa usada: {base_currency}1 = {target_currency}{transformed_df['_exchange_rate'].iloc[0]}")
        
        return transformed_df
        
    except Exception as e:
        logger.error(f"ERRO no pipeline ETL: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_etl_pipeline("USD", "GBP")  # USD para GBP
    
    # run_etl_pipeline("USD", "EUR")  # USD para Euro
    # run_etl_pipeline("EUR", "JPY")  # Euro para Yen Japonês