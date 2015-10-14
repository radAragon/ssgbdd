#! /usr/bin/python3
import multiprocessing
import sqlite3
import signal


def db_instance(id, conn):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    print('[b%d] Iniciando instancia de banco' % id)
    db = sqlite3.connect(':memory:')
    cur = db.cursor()
    cur.execute('''SELECT 1''')
    print('[b%d] Testando conexao: ' % id, cur.fetchone())
    conn.send(True)
    while True:
        try:
            #aguarda proxima instrucao
            cmd = conn.recv()
            #processa instrucao
            if cmd == 'X':
                break
        except Exception as e:
            print('[b%d] Erro:', e)
            break
    print('[b%d] Encerrando conexao' % id)
    db.close()

def menu_criar_col(cmd):
    raise NotImplemented()
    pass

def menu_criar(cmd):
    name_index = 1
    try:
        if cmd[1] == 'TABELA':
            name_index += 1
        table_name = cmd[name_index]
    except IndexError:
        print('Falta parametro: CRIAR [nome_tabela]')
        return

    print('Tabela:', table_name)
    print('Comandos: [C]oluna, [R]egra, [P]ronto, [D]esistir')

    submenu = {
        'C': menu_criar_col,
    }

    subcmd = None
    while subcmd != 'D':
        try:
            subcmd = input('... ').upper().split()
            submenu[subcmd[0]](subcmd)
        except KeyError:
            pass
        except IndexError:
            pass
        except KeyboardInterrupt:
            break

    if cmd == 'D':
        return


print('''
Trabalho de SGBDD
Simples Sistema de Gerenciamento de Banco de Dados Distribuido
''')

while True:
    try:
        i = input('Instancias: ')
        inst_num = int(i)
        break
    except ValueError:
        pass
    except KeyboardInterrupt:
        exit(1)

instances = list()
for n in range(inst_num):
    instances.insert(n, dict())
    instances[n]['id'] = n
    instances[n]['conn'], child_conn = multiprocessing.Pipe()
    proc = multiprocessing.Process(target=db_instance, args=(n, child_conn))
    proc.start()
    instances[n]['proc'] = proc

for n in instances:
    if n['conn'].recv():
        #instancia pronta
        pass

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
        cmd = input('> ').upper().split()
        menu[cmd[0]](cmd)
    except KeyError:
        pass
    except IndexError:
        pass
    except KeyboardInterrupt:
        break

print('Finalizando')
for n in instances:
    n['conn'].send('X')
    n['proc'].join()

print('Fim\n')
