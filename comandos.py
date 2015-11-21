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
        cmd_parts = cmd.partition('(')
        # verifica tabela
        table_parts = cmd_parts[0].upper().split()
        table_name = None
        if table_parts[1] == 'INTO':
            table_name = table_parts[2]
        else:
            raise Exception('INSERT OR não suportado')
        table_id, site_id = metabanco.identifica_tabela(table_name)
        if site_id:
            instances[site_id]['comm'].send(cmd)
            resp = instances[site_id]['comm'].recv()
            if not resp['result']:
                raise Exception('Falha ao aplicar em instância')
            else:
                print('Inseridos:', resp['rowcount'])
        else:
            # verifica colunas
            if (table_parts[3] == 'VALUES'):
                column_list = metabanco.colunas_tabela(table_id)
                values_parts = cmd_parts[2]
            else:
                column_parts = cmd_parts[2].partition(')')
                column_list = metabanco.identifica_colunas(table_id, column_parts[0])
                values_parts = column_parts[2]
            print(column_list)
            # verifica regras
            regras_id = metabanco.identifica_regras(column_list)
            print(regras_id)
            # verifica valores
            values = re.findall(r"[\w']+", values_parts)
            print(values)
            insert_list = dict()
            for row in [values[x:x+len(column_list)] for x in range(0, len(values), len(column_list))]:
                print(row)

    except Exception as e:
        print('Erro:', e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()

    return None
