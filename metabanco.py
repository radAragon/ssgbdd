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


def cria_meta_tabela(table_part):
    create_table = table_part.split()
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
    return cur.lastrowid


def cria_meta_colunas(table_id, col_part):
    cols = col_part.split(',')
    col_ids = list()
    for col in cols:
        col_parts = col.split()
        cur = DB.cursor()
        cur.execute('''
        INSERT INTO colunas (tabelas_id, coluna_nome, coluna_tipo)
        VALUES (:table, :col_name, :col_type)
        ''', {
                'table': table_id,
                'col_name': col_parts[0],
                'col_type': col_parts[1]
             })
        col_ids.append(cur.lastrowid)
    return col_ids


def cria_meta_regras(column_ids, rules_query):
    pass


def cria_metadados(create_cmd):
    create_query = create_cmd[0]
    table_part = create_query.partition('(')
    table_id = cria_meta_tabela(table_part[0])
    col_part = table_part[2].partition(')')
    column_ids = cria_meta_colunas(table_id, col_part[0])
    if (create_cmd[1] != ''):
        metabanco.cria_meta_regras(column_ids, create_cmd[2])
