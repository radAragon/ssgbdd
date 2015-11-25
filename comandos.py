import time
import logging
import metabanco


def interpreta_create(cmd, instances, current_site):
    print('')
    # separa regras
    upper_cmd = cmd.upper()
    if 'PARTITION' in upper_cmd:
        cmd_parts = upper_cmd.partition('PARTITION')
    elif 'SITE' in upper_cmd:
        cmd_parts = upper_cmd.partition('SITE')
    elif 'REFERENCES' in upper_cmd:
        cmd_parts = [upper_cmd, 'REFERENCES']
    else:
        raise Exception('Precisa definir PARTITION, SITE ou conter REFERENCES')

    create_table = cmd_parts[0]
    table_name = None
    try:
        table_name = metabanco.testa_create_table_query(create_table)
        metabanco.cria_metadados(table_name, cmd_parts)
        for i in instances:
            i['comm'].send({
                'execute': 'SIMPLE',
                'query': create_table
            })
        for i in instances:
            resp = i['comm'].recv()
            if not resp['result']:
                raise Exception('Falha ao aplicar em instância')
        metabanco.DB.commit()

    except Exception as e:
        print('Erro:', e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()
        if table_name:
            metabanco.DB.execute('DROP TABLE %s' % table_name)

    return None


def interpreta_insert(cmd, instances, current_site):
    print('')
    try:
        table, rows = metabanco.testa_insert_query(cmd)
        table_name = table['tabela_nome']
        table_id = table['id']
        site_id = table['site_id']
        owner_id = table['tabelas_id_primaria']
        if site_id:
            instances[site_id]['comm'].send({
                'execute': 'SIMPLE',
                'query': cmd
            })
            resp = instances[site_id]['comm'].recv()
            if not resp['result']:
                raise Exception('Falha ao aplicar em instância')
            else:
                print('Inseridos:', resp['rowcount'])

        elif owner_id:
            cur = metabanco.DB.cursor()
            statement = '''
            SELECT c.coluna_nome,
                   ref_t.tabela_nome as ref_tabela_nome,
                   ref_c.coluna_nome as ref_coluna_nome
            FROM colunas as c
            JOIN colunas as ref_c ON (ref_c.id = c.ref_colunas_id)
            JOIN tabelas as ref_t ON (ref_t.id = ref_c.tabelas_id)
            WHERE c.tabelas_id = ?
            '''
            cur.execute(statement, [table_id])
            ref = cur.fetchone()
            print('Distribuição secundária para tabela %s' % ref['ref_tabela_nome'])
            # print(tuple(ref))
            for x in range(len(instances)):
                i = instances[x]
                statement = 'SELECT ID FROM %s' % ref['ref_tabela_nome']
                obj = {
                    'execute': 'SIMPLE',
                    'query': statement
                }
                i['comm'].send(obj)
                resp = i['comm'].recv()
                if not resp['result']:
                    raise Exception('Falha ao ler em instância')
                # print([tuple(row) for row in resp['rows']])
                statement = '''
                SELECT * FROM %s
                WHERE %s IN ({0})
                '''.format(','.join(['?'] * len(resp['rows']))) % (table_name, ref['coluna_nome'])
                cur.execute(statement, [row[0] for row in resp['rows']])
                selected_rows = cur.fetchall()
                if len(selected_rows) > 0:
                    statement = '''
                    INSERT INTO %s VALUES ({0})
                    '''.format(','.join(['?'] * len(rows[0].keys()))) % table_name
                    obj = {
                        'execute': 'MANY',
                        'query': statement,
                        'values': [tuple(row) for row in selected_rows]
                    }
                    i['comm'].send(obj)
                    resp = i['comm'].recv()
                    if not resp['result']:
                        raise Exception('Falha ao aplicar em instância')
                    else:
                        print('[%d] Inseridos:' % i['id'], resp['rowcount'])

        else:
            rules = metabanco.identifica_regras(table_id, rows[0].keys())
            # print([tuple(rule) for rule in rules])
            cur = metabanco.DB.cursor()
            for rule in rules:
                statement = '''
                SELECT * FROM %s
                WHERE %s %s
                ''' % (table_name,
                       rule['coluna_nome'],
                       rule['criterio'])
                cur.execute(statement)
                rows = cur.fetchall()
                if (len(rows) > 0):
                    site_id = rule['site_id']
                    print('Site %d: %s %s' % (site_id,
                                              rule['coluna_nome'],
                                              rule['criterio']))
                    # print([tuple(row) for row in rows])
                    i = instances[site_id - 1]
                    statement = '''
                    INSERT INTO %s VALUES ({0})
                    '''.format(','.join(['?'] * len(rows[0].keys()))) % table_name
                    obj = {
                        'execute': 'MANY',
                        'query': statement,
                        'values': [tuple(row) for row in rows]
                    }
                    i['comm'].send(obj)
                    resp = i['comm'].recv()
                    if not resp['result']:
                        raise Exception('Falha ao aplicar em instância')
                    else:
                        print('Inseridos:', resp['rowcount'])

        # exclui linhas do metabanco
        cur.execute('DELETE FROM %s' % table_name)
        metabanco.DB.commit()

    except Exception as e:
        # logging.exception('Erro')
        print(e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()

    return None


def interpreta_select(cmd, instances, current_site):
    print('')
    initial_time = time.time()
    if not current_site:
        raise Exception('Precisa definir SITE atual')

    upper_cmd = cmd.upper()

    limit_cmd = upper_cmd.partition('LIMIT')
    upper_cmd = limit_cmd[0]

    order_cmd = upper_cmd.partition('ORDER BY')
    upper_cmd = order_cmd[0]

    group_cmd = upper_cmd.partition('GROUP BY')
    upper_cmd = group_cmd[0]

    try:
        tables, columns = metabanco.testa_select_query(cmd)
        union = list()
        for instance in instances:
            obj = {
                'execute': 'SIMPLE',
                'query': upper_cmd,
                'current_site': current_site
            }
            instance['comm'].send(obj)
            resp = instance['comm'].recv()
            if not resp['result']:
                raise Exception('Falha ao ler em instância')
            else:
                union.extend(resp['rows'])

        if limit_cmd[1] or order_cmd[1] or group_cmd[1]:
            cur = metabanco.DB.cursor()
            union_table_name = 'TEMP_UNION_TABLE'
            statement = '''
            CREATE TEMPORARY TABLE %s ({0})
            '''.format(','.join(columns)) % union_table_name
            cur.execute(statement)
            statement = '''
            INSERT INTO %s VALUES ({0})
            '''.format(','.join(['?'] * len(columns))) % union_table_name
            cur.executemany(statement, union)
            statement = '''
            SELECT {0} FROM %s
            '''.format(','.join(columns)) % union_table_name
            if group_cmd[1]:
                statement += group_cmd[1] + group_cmd[2]

            if order_cmd[1]:
                statement += order_cmd[1] + order_cmd[2]

            if limit_cmd[1]:
                statement += limit_cmd[1] + limit_cmd[2]

            cur.execute(statement)
            union = cur.fetchall()

        final_time = time.time()
        exibe_linhas(columns, union)
        print('''
                                        Consulta realizada em %.10fs
        ''' % (final_time - initial_time))

    except Exception as e:
        # logging.exception('Erro')
        print(e)
        metabanco.DB.rollback()

    finally:
        try:
            if union_table_name:
                metabanco.DB.execute('DROP TABLE %s' % union_table_name)
        except:
            pass


def exibe_linhas(columns, rows):
    print(tuple(columns))
    for row in rows:
        print(tuple(row))
