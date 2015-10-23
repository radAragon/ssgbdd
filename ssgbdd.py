#! /usr/bin/python3
import multiprocessing
import sqlite3
import signal


def inicia_banco(db_name):
    db = sqlite3.connect(db_name)
    teste = db.cursor()
    teste.execute('''SELECT 1''') #teste OK
    if not teste.fetchone():
        raise Exception('Erro na conexão')
    return db

def db_process(id, comm):
    '''Conecta a um banco Sqlite, testa conexao, envia sinal positivo para
    processo pai e fica aguardando comandos SQL. Se receber 'X',
    encerra conexao.
    '''
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    print('[b%d] Iniciando instancia de banco' % id)
    db = inicia_banco('bd' + str(id) + '.db')
    comm.send(True)
    while True:
        try:
            #aguarda proxima instrucao
            cmd = comm.recv()
            #processa instrucao
            if cmd == 'X':
                break
        except Exception as e:
            print('[b%d] Erro:', e)
            break
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
        proc = multiprocessing.Process(target=db_process, args=(n, child_conn))
        proc.start()
        instances[n]['proc'] = proc

    for n in instances:
        if n['comm'].recv():
            #instancia pronta
            pass
    return instances


def interpreta_create(db, cmd):
    print('')
    create_query = cmd.partition('{')
    cur = db.cursor()
    try:
        cur.execute(create_query[0])
        status = cur.fetchone()
        print(status)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    print('''
Trabalho de SGBDD
Simples Sistema de Gerenciamento de Banco de Dados Distribuido
    ''')

    print('''
    Iniciando banco principal...
    ''')
    db_main = inicia_banco('metadados.db')

    while True:
        try:
            i = input('Instancias distribuídas: ')
            inst_num = int(i)
            break
        except ValueError:
            pass
        except KeyboardInterrupt:
            exit(1)

    instances = abrir_instancias(inst_num)

    menu = {
        'CREATE': interpreta_create,
    }

    print('''
Comandos: fdgd
    CREATE TABLE nome (COLUNAS, ) {REGRAS, }
    SAIR
    ''')

    cmd = None
    while (cmd != 'SAIR'):
        #recebe instrucao
        try:
            cmd = input('> ').upper()
            cmd_type = cmd.partition(' ')[0]
            if (cmd_type in menu):
                menu[cmd_type](db_main, cmd)
        except KeyError:
            pass
        except IndexError:
            pass
        except KeyboardInterrupt:
            break

    print('Finalizando')
    db_main.close()
    for n in instances:
        n['comm'].send('X')
        n['proc'].join()

    print('Fim\n')
