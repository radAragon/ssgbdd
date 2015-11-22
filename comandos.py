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
    else:
        raise Exception('Precisa definir PARTITION ou SITE')

    create_table = cmd_parts[0]
    table_name = None
    try:
        table_name = metabanco.testa_create_table_query(create_table)
        metabanco.cria_metadados(table_name, cmd_parts)
        for i in instances:
            i['comm'].send(create_table)
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
        table_id, site_id = metabanco.identifica_tabela(table_name)
        if site_id:
            instances[site_id]['comm'].send(cmd)
            resp = instances[site_id]['comm'].recv()
            if not resp['result']:
                raise Exception('Falha ao aplicar em instância')
            else:
                print('Inseridos:', resp['rowcount'])
            # exclui linhas do metabanco
            metabanco.DB.rollback()
        else:
            rules = metabanco.identifica_regras(table_id, rows[0].keys())
            print([tuple(rule) for rule in rules])

    except Exception as e:
        print('Erro:', e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()

    return None
