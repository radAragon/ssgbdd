#! /usr/bin/python3
import multiprocessing
import sqlite3


def db_instance(id, conn):
    print('[%d] Iniciando instancia de banco' % id)
    db = sqlite3.connect(':memory:')
    cur = db.cursor()
    cur.execute('''SELECT 1''')
    print('[%d] Testando conexao: ' % id, cur.fetchone())
    conn.send(True)
    while True:
        try:
            #aguarda proxima instrucao
            cmd = conn.recv()
            #processa instrucao
            if cmd == 'X':
                break
        except Exception as e:
            print('[%d] Erro:', e)
            break
    print('[%d] Encerrando conexao' % id)
    db.close()


print('''
Trabalho de SGBDD
Simples Sistema de Gerenciamento de Banco de Dados Distribuido
''')
inst_num = input('Instancias: ')
instances = list()
for n in range(0, int(inst_num)):
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
Comandos: SAIR
''')

while True:
    #recebe instrucao
    cmd = input('> ')
    if cmd == 'SAIR':
        print('Finalizando')
        break

for n in instances:
    n['conn'].send('X')
    n['proc'].join()

print('Fim.')
