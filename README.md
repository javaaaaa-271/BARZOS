# BARZOS[README.md](https://github.com/user-attachments/files/26168660/README.md)
# BarOS

Aplicacao Flask para menu digital, painel do bar, pedidos persistidos em SQLite e bloco inicial de logistica operacional.

## O que ja existe

- Cardapio publico para cliente montar pedidos
- Login para o painel do bar
- Painel operacional com pedidos pendentes e concluidos
- Banco SQLite com pedidos, itens, estoque e notas do turno
- Estrutura pronta para deploy com `waitress` e `render.yaml`

## Rodando localmente

Essa base permite evoluir para:

- controle de estoque por baixa automatica
- fechamento de caixa por turno
- origem do pedido por QR mesa, balcao ou delivery
- relatorios diarios
