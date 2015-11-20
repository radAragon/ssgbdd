#! python
#
#    Trabalho de BDD - Prof. Daniel de Oliveira - 2015/2
#    Alunos: Larissa Oliveira
#            Luis Fernando Nascimento
#            Priscila
#            Radames Aragon
#    Contato: rad.aragon@gmail.com

import sys
import multiprocessing
import sqlite3
import signal
import metabanco
import comandos

# Queries uteis:
#   Todas as tabelas: SELECT name FROM sqlite_master WHERE type='table';
#   Descricoes: list(map(lambda x: x[0], cursor.description))


def inicia_banco(db_name):
    db = sqlite3.connect(db_name, isolation_level='EXCLUSIVE',
                         detect_types=sqlite3.PARSE_DECLTYPES)
    test_cur = db.cursor()
    test_cur.execute('''SELECT 1''')  # teste OK
    if not test_cur.fetchone():
        raise Exception('Erro na conexão')
    return db


def db_process(id, comm):
    '''Conecta a um banco Sqlite, testa conexao, envia sinal positivo para
    processo pai e fica aguardando comandos SQL. Se receber 'X',
    encerra conexao.
    '''
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    print('[%d] Iniciando instancia de banco' % id)
    db = inicia_banco('bd' + str(id) + '.db')
    cur = db.cursor()
    comm.send(True)
    while True:
        try:
            # aguarda proxima instrucao
            cmd = comm.recv()
            # processa instrucao
            if cmd == 'X':
                break
            else:
                cur.execute(cmd)
                db.commit()
                comm.send({
                             'dados': cur.fetchall(),
                             'result': True
                          })

        except Exception as e:
            print('[b%d] Erro:', e)
            db.rollback()
            comm.send({
                         'dados': None,
                         'result': False
                      })

    print('[b%d] Encerrando conexao' % id)
    db.close()


def abrir_instancias(inst_num):
    ''' Abre [inst_num] processos que se conectam a bancos diferentes e aguarda
    sinal de conexão pronta e testada de cada um.
    Retorna: lista de processos com "id" e comunicacoes respectivas.
    '''
    instances = list()
    for n in range(inst_num):
        instances.insert(n, dict())
        instances[n]['id'] = n
        instances[n]['comm'], child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=db_process,
                                       args=(n+1, child_conn))
        proc.start()
        instances[n]['proc'] = proc

    for n in instances:
        if n['comm'].recv():
            # instancia pronta
            pass
    return instances


if __name__ == '__main__':
    print('''
SSGDB
Sistema Simples de Gerenciamento de Banco de Dados Distribuido
v.1.0


Iniciando banco principal...
    ''')
    db_meta = inicia_banco('metadados.db')
    metabanco.estrutura_metadados(db_meta)
    print('')

    if len(sys.argv) > 1:
        inst_num = int(sys.argv[1])
    else:
        while True:
            try:
                i = input('Instancias distribuidas (SITES): ')
                inst_num = int(i)
                if (inst_num < 1):
                    print('Precisa ser maior ou igual a 1')
                    continue
                break
            except ValueError:
                pass
            except KeyboardInterrupt:
                exit(1)

    instances = abrir_instancias(inst_num)

    menu = {
        'CREATE': comandos.interpreta_create,
        'INSERT': comandos.interpreta_insert
    }

    print('''
Comandos:
    CREATE TABLE nome_tabela (nome_coluna tipo_coluna, )_
        PARTITION nome_coluna (site_id: critério, )
    INSERT INTO nome_tabela (nome_coluna, ) VALUES (valor_coluna, )
    SAIR
''')

    cmd = ''
    while (cmd.upper() != 'SAIR'):
        # recebe instrução
        try:
            cmd = input('> ')
            cmd_type = cmd.partition(' ')[0].upper()
            if (cmd_type in menu):
                menu[cmd_type](cmd, instances)
        except KeyError:
            pass
        except IndexError:
            pass
        except KeyboardInterrupt:
            break

    print('Finalizando')
    db_meta.close()
    for n in instances:
        n['comm'].send('X')
        n['proc'].join()

    print('Fim\n')
