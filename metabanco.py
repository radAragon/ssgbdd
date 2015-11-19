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
        colunas_id INTEGER,
        site_id INTEGER,
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
    table_def = table_part.split()
    table_name_index = 2
    if (table_def[1] in ['TEMP', 'TEMPORARY']):  # temporary table
        table_name_index += 1
    if (table_def[table_name_index] == 'IF'):  # if not exists
        table_name_index += 3
    table_name = table_def[table_name_index]
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


def cria_meta_colunas(table_id, columns_part):
    columns = columns_part.split(',')
    col_ids = dict()
    for col in columns:
        column_def = col.split()
        cur = DB.cursor()
        cur.execute('''
        INSERT INTO colunas (tabelas_id, coluna_nome, coluna_tipo)
        VALUES (:table, :col_name, :col_type)
        ''', {
                'table': table_id,
                'col_name': column_def[0],
                'col_type': column_def[1]
             })
        col_ids[column_def[0]] = cur.lastrowid
    return col_ids


def cria_meta_regras(column_id, rules_part):
    rules = rules_part[0].split(',')
    rules_ids = list()
    for rule in rules:
        rule_def = rule.split()
        cur = DB.cursor()
        cur.execute('''
        INSERT INTO regras (colunas_id, site_id, criterio)
        VALUES (:column, :site, :criterio)
        ''', {
                'column': column_id,
                'site': rule_def[0].rpartition('%')[2],
                'criterio': rule_def[1] + rule_def[2]
             })
        rules_ids.append(cur.lastrowid)
    return rules_ids


def cria_metadados(create_cmd):
    create_query = create_cmd[0]
    table_part = create_query.partition('(')
    table_id = cria_meta_tabela(table_part[0])
    columns_part = table_part[2].partition(')')
    columns = cria_meta_colunas(table_id, columns_part[0])
    if (create_cmd[1] == 'PARTITION'):
        partition_rules = create_cmd[2].partition('(')
        column_name = partition_rules[0].strip()
        column_id = columns[column_name]
        print('Criando partições para %s (%d)' % (column_name, column_id))
        rules_part = partition_rules[2].partition(')')
        cria_meta_regras(column_id, rules_part)
    return None
