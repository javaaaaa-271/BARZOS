# BarOS

Aplicacao Flask para menu digital, painel do bar, pedidos persistidos em SQLite e bloco inicial de logistica operacional.

## O que ja existe

- Cardapio publico para cliente montar pedidos
- Login para o painel do bar
- Painel operacional com pedidos pendentes e concluidos
- Banco SQLite com pedidos, itens, estoque e notas do turno
- Estrutura pronta para deploy com `waitress` e `render.yaml`

## Rodando localmente

```bash
python -m pip install -r requirements.txt
python app.py
```

Ou em modo mais proximo de producao:

```bash
python serve.py
```

## Variaveis de ambiente

Copie `.env.example` e ajuste:

- `BAROS_SECRET_KEY`: chave de sessao
- `BAROS_USERNAME`: usuario do painel
- `BAROS_PASSWORD`: senha do painel
- `BAROS_DB_PATH`: caminho do banco SQLite
- `BAROS_HOST`: host de execucao
- `BAROS_PORT`: porta de execucao
- `BAROS_DEBUG`: `true` ou `false`

## Publicando na internet

### Render

1. Suba este projeto para um repositorio Git.
2. Crie um novo Web Service no Render apontando para o repositorio.
3. O Render pode usar o arquivo `render.yaml` automaticamente.
4. Crie um disco persistente montado em `/var/data`.
5. Defina uma senha real em `BAROS_PASSWORD`.
6. Depois do deploy, abra a URL publica gerada pelo Render.

### Variaveis esperadas no Render

- `BAROS_SECRET_KEY`
- `BAROS_USERNAME`
- `BAROS_PASSWORD`
- `BAROS_DB_PATH=/var/data/baros.db`

### Observacao importante sobre SQLite

Se voce publicar sem disco persistente, o arquivo do banco pode ser perdido quando a instancia reiniciar.
Para manter os dados, use o disco persistente do provedor ou depois migre para Postgres.

### O que considerar antes de ir para producao

- Trocar `admin` e a senha padrao
- Mover de SQLite para Postgres quando houver mais volume ou varios atendentes
- Colocar HTTPS e dominio proprio
- Adicionar backup do banco e logs centralizados

## Como os dados e a logistica estao organizados

### Pedidos e consumo

- `bebidas`: nome, preco de venda e custo estimado
- `pedidos`: codigo de retirada, horario, status e valor total
- `itens_pedido`: itens vendidos por pedido

### Logistica

- `inventory_items`: estoque basico e nivel minimo
- `shift_notes`: checklist e recados do turno

Essa base permite evoluir para:

- controle de estoque por baixa automatica
- fechamento de caixa por turno
- origem do pedido por QR mesa, balcao ou delivery
- relatorios diarios
