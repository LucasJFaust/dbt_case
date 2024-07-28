WITH
    rennamed as (
        SELECT
            cast(ID as int) as pk_produto,
            cast(SUPPLIERID as int) as fk_fornecedor,
            cast(CATEGORYID as int) as fk_categoria,
            cast(PRODUCTNAME  as varchar) as nome_produto,
            cast(QUANTITYPERUNIT as varchar) as quantidade_por_unidade,
            cast(UNITPRICE as numeric(18,2)) as preco_por_unidade,
            cast(UNITSINSTOCK as int) as unidade_em_estoque,
            cast(UNITSONORDER as int) as unidade_por_pedido,
            cast(REORDERLEVEL as int) as nivel_do_pedido,
            CASE
                WHEN DISCONTINUED = '1' THEN false
                WHEN DISCONTINUED = '0' THEN true
                else null
            END as eh_discontinuado
        FROM {{ source('erp', 'product') }}
    )

SELECT *
FROM rennamed