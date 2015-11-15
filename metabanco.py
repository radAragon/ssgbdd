DB = None


def estrutura_metadados(connection):
    global DB
    DB = connection
    cur = DB.cursor()
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
    DB.commit()
    print('PRONTA REGRAS')


def testa_sql(create_query):
    cur = DB.cursor()
    cur.execute(create_query)
    print('SQL avaliado com sucesso')
    return None


def cria_meta_tabela(create_query):
    query_parts = create_query.partition('(')
    create_table = query_parts[0].split()
    table_name_index = 2
    if (create_table[1] in ['TEMP', 'TEMPORARY']):  # temporary table
        table_name_index += 1
    if (create_table[table_name_index] == 'IF'):  # if not exists
        table_name_index += 3
    table_name = create_table[table_name_index]
    schema_format = table_name.split('.')
    if (len(schema_format) > 1):  # schema name
        table_name = schema_format[1]
    print('Criando tabela %s' % table_name)
    cur = DB.cursor()
    cur.execute('''
    INSERT INTO tabelas (tabela_nome)
    VALUES (:nome)
    ''', {
        'nome': table_name,
    })
    return None
