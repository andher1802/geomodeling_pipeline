from utils import api_keys, data_base_management

def set_up_database():
    DB_params = {
    'PGHOST': api_keys.PGHOST,
    'PGDATABASE': api_keys.PGDATABASE,
    'PGUSER': api_keys.PGUSER,
    'PGPASSWORD': api_keys.PGPASSWORD,
    'TIMEOUT': api_keys.TIMEOUT,
    'PORT': api_keys.PORT
    }
    db_setting_manager = data_base_management.DB_manager(DB_params)
    query_params = {
        'columns': '*',
        'schema': 'andres',
        'table': 'test_phenology'
    }
    db_query_manager = data_base_management.DB_query(db_setting_manager.get_conn(), query_params)
    return db_query_manager
