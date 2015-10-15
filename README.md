# ssgbdd
Trabalho de Bancos de Dados Distribuídos - 2015-2

- Python 3.4

Para executar: `./ssgbdd.py` ou `python3 ssgbdd.py`


## Fluxo principal:

* Função Main (if __name__ == __main__):
1. Inicia o programa e pede o número (x) de instâncias ao usuário.
1. Abre (x) _pipelines_ de comunicação entre processos.
1. Abre (x) processos em paralelo da função `db_instance` com _pipeline_ respectivo.
1. Aguarda processos enviarem "ok".
1. Mostra menu de ações e prompt para usuário.
1. Se usuário escolhe sair, encerra conexões e processos paralelos.

* Função db_instance:
1. Inicia uma conexão com banco de dados Sqlite (em memória).
1. Testa conexão.
1. Envia sinal "ok".
1. Aguarda comando.
1. Previsto: executa comando no banco conectado.

* Função menu_criar:
1. Se usuário escolhe criar, abre submenu de opções.
