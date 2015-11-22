import re
import metabanco


def interpreta_create(cmd, instances):
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


def interpreta_insert(cmd, instances):
    print('')
    try:
        table_name, rows = metabanco.testa_insert_query(cmd)
        table = metabanco.identifica_tabela(table_name)
        table_id = table['id']
        site_id = table['site_id']
        table_owner = table['tabelas_id_primaria']
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

        elif table_owner:
            pass
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
                    statement = 'INSERT INTO %s VALUES ({0})'.format(','.join(['?'] * len(rows[0].keys()))) % table_name
                    i = instances[site_id - 1]
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
        metabanco.DB.rollback()

    except Exception as e:
        print('Erro:', e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()

    return None
