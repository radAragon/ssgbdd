#! /usr/bin/python3
import multiprocessing
import sqlite3
import signal


def db_process(id, comm):
    '''Conecta a um banco Sqlite, testa conexao, envia sinal positivo para
    processo pai e fica aguardando comandos SQL. Se receber 'X',
    encerra conexao.
    '''
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    print('[b%d] Iniciando instancia de banco' % id)
    db = sqlite3.connect(':memory:')
    cur = db.cursor()
    cur.execute('''SELECT 1''')
    print('[b%d] Testando conexao: ' % id, cur.fetchone())
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

def criar_tabela(name, objs):
    '''Constroi tabela e colunas como parte do comando CRIAR.
    Retorna: nada
    '''
    for col in objs:
        col_parts = col.split()
        col_name = col_parts[0]
        col_type = col_parts[1]

def criar_regra(name, objs):
    raise NotImplemented()

def menu_criar(cmd):
    '''Interpreta comando CRIAR.
    Retorna: nada.
    '''
    cmd_parts = cmd.partition('(')
    objs = cmd_parts[1].partition(')').split(',')
    try:
        name_parts = cmd_parts[0].split()

        if name_parts[0] == 'TABELA':
            return criar_tabela(name_parts[1], objs)

        elif name_parts[0] == 'REGRA':
            return criar_regra(name_parts[1], objs)

    except IndexError:
        print('Falta parametro: CRIAR [nome_tabela]')
        return None




if __name__ == '__main__':
    print('''
Trabalho de SGBDD
Simples Sistema de Gerenciamento de Banco de Dados Distribuido
    ''')

    print('Iniciando banco principal...')
    db_main = sqlite3.connect(':memory:')
    cur_main = db_main.cursor()
    cur_main.execute('''SELECT 1''') #teste OK

    while True:
        try:
            i = input('Instancias: ')
            inst_num = int(i)
            break
        except ValueError:
            pass
        except KeyboardInterrupt:
            exit(1)

    instances = abrir_instancias(inst_num)

    print('''
    Comandos: CRIAR, SAIR
    ''')
    menu = {
        'CRIAR': menu_criar,
    }

    cmd = None
    while cmd != 'SAIR':
        #recebe instrucao
        try:
            cmd = input('> ').upper()
            cmd_parts = cmd.partition(' ')
            menu[cmd_parts[0]](cmd_parts[2])
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
