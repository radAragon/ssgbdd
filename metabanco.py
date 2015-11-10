metabanco = None

def estrutura_metadados(db):
    cur = db.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tabela (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabela_nome TEXT
    )''')
    print('PRONTA TABELAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS coluna (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabela_id TEXT,
        coluna_nome TEXT,
        coluna_tipo TEXT
    )''')
    print('PRONTA COLUNAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS regra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabela_id INTEGER,
        coluna_id INTEGER,
        site_id INTEGER,
        criterio TEXT
    )''')
    print('PRONTA REGRAS')
    global metabanco
    metabanco = db


def cria_regras(cmd):
    pass
