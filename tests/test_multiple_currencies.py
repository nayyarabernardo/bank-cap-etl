# test_multiple_currencies.py
from src.main import run_etl_pipeline

# Testa várias conversões
conversions = [
    ("USD", "GBP"),    # Dólar para Libra
    ("USD", "EUR"),    # Dólar para Euro
    ("USD", "JPY"),    # Dólar para Yen
    ("EUR", "GBP"),    # Euro para Libra
]

for base, target in conversions:
    print(f"\n{'='*60}")
    print(f"Executando conversão: {base} -> {target}")
    print('='*60)
    
    try:
        df = run_etl_pipeline(base, target)
        print(f"✓ Sucesso: {len(df)} registros convertidos")
    except Exception as e:
        print(f"✗ Erro: {e}")