from src.database_utils.db_values.preprocess import make_db_lsh
from src.database_utils.db_catalog.preprocess import make_db_context_vec_db


for db_id in ['debit_card_specializing', 'financial', 'regional_sales']:
    make_db_lsh(
        f'/home/chenjie/projects/chase-sql-on-finch/data/bird/{db_id}', 
        signature_size=100, 
        n_gram=3, 
        threshold=0.01,
        verbose=True)
    make_db_context_vec_db(
        f'/home/chenjie/projects/chase-sql-on-finch/data/bird/{db_id}',
        use_value_description=True)

print("Done!")