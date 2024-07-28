WITH
    categorias as (
        SELECT *
        FROM {{ ref('stg_erp__categorias') }}
    ),

    produtos as (
        SELECT *
        FROM {{ ref('stg_erp__produtos') }}
    ),

    joined as (
        SELECT
            PK_PRODUTO
            , produtos.FK_FORNECEDOR
            , produtos.NOME_PRODUTO
            , categorias.NOME_CATEGORIA
            , produtos.QUANTIDADE_POR_UNIDADE
            , produtos.PRECO_POR_UNIDADE
            , produtos.UNIDADE_EM_ESTOQUE
            , produtos.UNIDADE_POR_PEDIDO
            , produtos.NIVEL_DO_PEDIDO
            , produtos.EH_DISCONTINUADO
        FROM produtos
        LEFT JOIN categorias
            ON produtos.fk_categoria = categorias.pk_categoria
    )
SELECT *
FROM joined