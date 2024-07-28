WITH
    rennamed as (
        SELECT
            cast(orderid as int) as fk_pedido,
            cast(productid as int) as fk_produto,
            cast(discount as numeric(18,2)) as desconto_prec,
            cast(unitprice as numeric(18,2)) as preco_da_unidade,
            cast(quantity as int) as quantidade
        FROM {{ source('erp', 'orderdetail') }}
    )

SELECT *
FROM rennamed
