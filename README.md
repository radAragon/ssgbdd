# ssgbdd
Trabalho de Bancos de Dados Distribuídos - 2015-2

Requerimentos:
* Python 3.4

### Para executar: `python ssgbdd.py [$1]`

### Fluxo principal:

* Função Main (`if __name__ == __main__`):
  1. Inicia o programa e pede o número de instâncias ao usuário, ou recebe parâmetro `$1` como número de instâncias `x`.
  1. Abre `x` _pipelines_ de comunicação entre processos.
  1. Abre `x` processos em paralelo da função `db_instance` com _pipeline_ respectivo.
  1. Aguarda processos enviarem "ok".
  1. Mostra menu de ações e prompt para usuário.
  1. Se usuário escolhe sair, encerra conexões e processos paralelos.


* Função db_instance:
  1. Inicia uma conexão com banco de dados Sqlite (em arquivo).
  1. Testa conexão.
  1. Envia sinal "ok".
  1. Aguarda comando.
  1. Verifica tipo de comando (_simple_, _many_, ou _script_), e chama a função `execute` do Sqlite correspondente com a query recebida e os valores se houver.
  1. Devolve resultado (`fetchall` e `rowcount`)
  1. Se o site não for o corrente (`CURRENT_SITE`), a db_instance aplica `LATENCY` ao tempo de resposta.
