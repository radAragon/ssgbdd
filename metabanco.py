

def estrutura_metadados(db):
    cur = db.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tabelas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabela_nome TEXT
    )''')
    print('PRONTA TABELAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS colunas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabelas_id TEXT,
        coluna_nome TEXT,
        coluna_tipo TEXT
    )''')
    print('PRONTA COLUNAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS regras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabelas_id INTEGER,
        site_id INTEGER,
        colunas_id INTEGER,
        criterio TEXT
    )''')
    print('PRONTA REGRAS')
