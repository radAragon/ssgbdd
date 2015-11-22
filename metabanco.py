DB = None


def estrutura_metadados(connection):
    global DB
    DB = connection
    cur = DB.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tabelas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabela_nome TEXT,
        site_id INTEGER,
        tabelas_id_primaria INTEGER
    )''')
    print('    PRONTA TABELAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS colunas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabelas_id TEXT,
        coluna_nome TEXT,
        coluna_tipo TEXT,
        ref_colunas_id INTEGER
    )''')
    print('    PRONTA COLUNAS')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS regras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        colunas_id INTEGER,
        site_id INTEGER,
        criterio TEXT
    )''')
    DB.commit()
    print('    PRONTA REGRAS')


def identifica_tabela(table_name):
    cur = DB.cursor()
    cur.execute('''
    SELECT * FROM tabelas
    WHERE tabela_nome = ?
    ''', [table_name])
    result = cur.fetchone()
    if not result:
        raise Exception('Tabela não identificada')

    return result


def identifica_colunas(table_id, column_parts):
    columns = column_parts.upper().split(',')
    # limpa todos os espaços ao redor dos nomes
    clean_columns = [c.strip() for c in columns]
    cur = DB.cursor()
    # constrói um SELECT com todas as colunas descritas
    statement = '''
    SELECT id, coluna_nome FROM colunas
    WHERE tabelas_id = ?
    AND coluna_nome IN ({0})
    '''.format(','.join(['?'] * len(clean_columns)))
    cur.execute(statement, [table_id] + clean_columns)
    result = cur.fetchall()
    if len(result) != len(columns):
        raise Exception('Coluna não identificada')

    return result


def identifica_regras(table_id, column_list):
    cur = DB.cursor()
    # constrói um SELECT com todas as colunas
    statement = '''
    SELECT r.id, c.coluna_nome, r.site_id, r.criterio FROM regras as r
    JOIN colunas as c ON (c.id = r.colunas_id)
    WHERE c.tabelas_id = ?
    AND c.coluna_nome IN ({0})
    '''.format(','.join(['?'] * len(column_list)))
    cur.execute(statement, [table_id] + column_list)
    result = cur.fetchall()
    return result


def colunas_tabela(table_id):
    cur = DB.cursor()
    cur.execute('''
    SELECT id, coluna_nome FROM colunas
    WHERE tabelas_id = ?
    ''', [table_id])
    result = cur.fetchall()
    return result


def testa_create_table_query(create_table):
    table_def = create_table.partition('(')
    words = table_def[0].split()
    if words[1] != 'TABLE':  # temporary table
        raise Exception('Não é permitido criar tabela temporária')

    if words[2] == 'IF':  # if not exists
        raise Exception('Não usar IF NOT EXISTS')

    table_name = words[2]
    cur = DB.cursor()
    cur.execute(create_table)
    print('SQL avaliado com sucesso')
    return table_name


def testa_insert_query(insert):
    cur = DB.cursor()
    cur.execute(insert)
    # verifica tabela
    table_parts = insert.upper().split()
    for i in range(0, len(table_parts)):
        if table_parts[i] == 'INTO':
            table_name = table_parts[i+1]
            break
    statement = 'SELECT * FROM ' + table_name
    cur.execute(statement)
    rows = cur.fetchall()
    # print([tuple(row) for row in rows])
    return table_name, rows


def cria_meta_tabela(table_name, table_part):
    schema_format = table_name.split('.')
    if (len(schema_format) > 1):  # schema name
        table_name = schema_format[1]

    print('Criando tabela %s' % table_name)
    cur = DB.cursor()
    cur.execute('''
    INSERT INTO tabelas (tabela_nome)
    VALUES (:nome)
    ''', {
        'nome': table_name
    })
    return cur.lastrowid


def cria_meta_colunas(table_id, columns_part):
    columns = columns_part.split(',')
    columns_def = dict()
    ref_table_id = None
    for col in columns:
        column_words = col.split()
        ref_col_id = None
        if column_words[0] == 'ID':
            if len(column_words) > 2:
                if column_words[2] != 'PRIMARY':
                    raise Exception('Coluna ID precisa ser PRIMARY KEY')
        else:
            if len(column_words) > 2:
                if column_words[2] == 'REFERENCES':
                    ref_table_name = column_words[3]
                    table = identifica_tabela(ref_table_name)
                    ref_table_id = table['id']
                    ref_col_id = identifica_colunas(ref_table_id, 'ID')[0][0]

                else:
                    raise Exception('Definição de coluna %s não suportado' % column_words[0])

        cur = DB.cursor()
        cur.execute('''
        INSERT INTO colunas (
            tabelas_id,
            coluna_nome,
            coluna_tipo,
            ref_colunas_id
        )
        VALUES (:table, :col_name, :col_type, :ref_col)
        ''', {
            'table': table_id,
            'col_name': column_words[0],
            'col_type': column_words[1],
            'ref_col': ref_col_id
        })
        columns_def[column_words[0]] = cur.lastrowid

    return columns_def, ref_table_id


def cria_meta_regras(column_id, rules_part):
    rules = rules_part.split(',')
    rules_ids = list()
    for rule in rules:
        rule_def = rule.split(':')
        cur = DB.cursor()
        cur.execute('''
        INSERT INTO regras (colunas_id, site_id, criterio)
        VALUES (:column, :site, :criterio)
        ''', {
            'column': column_id,
            'site': int(rule_def[0]),
            'criterio': rule_def[1].strip()
        })
        rules_ids.append(cur.lastrowid)

    return rules_ids


def define_site_tabela(table_id, site):
    cur = DB.cursor()
    cur.execute('''
    UPDATE tabelas
    SET site_id = :site
    WHERE id = :table
    ''', {
        'site': site,
        'table': table_id
    })
    if cur.rowcount < 1:
        raise Exception('Erro ao definir site de tabela')

    return None


def define_tabela_primaria(table_id, ref_table_id):
    cur = DB.cursor()
    cur.execute('''
    UPDATE tabelas
    SET tabelas_id_primaria = :ref
    WHERE id = :table
    ''', {
        'ref': ref_table_id,
        'table': table_id
    })
    if cur.rowcount < 1:
        raise Exception('Erro ao definir tabela primária')

    return None


def cria_metadados(table_name, create_cmd):
    create_query = create_cmd[0]
    # define tabela
    table_part = create_query.partition('(')
    table_id = cria_meta_tabela(table_name, table_part[0])
    # define colunas
    columns_part = table_part[2].partition(')')
    columns, ref_table_id = cria_meta_colunas(table_id, columns_part[0])
    # define destribuição
    if (create_cmd[1] == 'PARTITION'):
        partition_rules = create_cmd[2].partition('(')
        partition_name = partition_rules[0].strip()
        if (partition_name in columns):
            column_id = columns[partition_name]
            print('Criando partições para %s' % partition_name)
            rules_part = partition_rules[2].partition(')')
            cria_meta_regras(column_id, rules_part[0])
        else:
            raise Exception('Partition nome_coluna não é uma coluna')

    elif (create_cmd[1] == 'SITE'):
        site = int(create_cmd[2])
        define_site_tabela(table_id, site)
    elif (create_cmd[1] == 'REFERENCES'):
        define_tabela_primaria(table_id, ref_table_id)

    return None
