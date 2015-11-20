import metabanco


def interpreta_create(cmd, instances):
    print('')
    # separa regras
    cmd_parts = cmd.upper().partition('PARTITION')
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


def interpreta_insert(cmd, instances):
    print('')
    try:
        cmd_parts = cmd.partition('(')
        table_parts = cmd_parts[0].upper().split()
        table_name = None
        if table_parts[1] == 'INTO':
            table_name = table_parts[2]
        else:
            raise Exception('INSERT OR não suportado')
        table_id = metabanco.identifica_tabela(table_name)
        column_parts = cmd_parts[2].partition(')')
        column_list = metabanco.identifica_colunas(table_id, column_parts[0])
        # Depois das colunas
        values_parts = column_parts[2]
        print(values_parts)

    except Exception as e:
        print('Erro:', e)
        print('Executando ROLLBACK')
        metabanco.DB.rollback()
