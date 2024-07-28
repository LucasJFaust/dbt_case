WITH
    rennamed as (
        SELECT
            cast(ID as int) as pk_pedido,
            cast(EMPLOYEEID as int) as fk_funcionario,
            cast(CUSTOMERID as varchar) as fk_cliente,
            cast(SHIPVIA as int) as fk_transportadora,
            cast(ORDERDATE as date) as data_do_pedido,
            cast(SHIPPEDDATE as date) as data_do_envio,
            cast(REQUIREDDATE as date) as data_requerida_entrega,
            cast(FREIGHT as numeric) as frete,
            cast(SHIPNAME as varchar) as nome_destinatario,
            cast(SHIPCITY as varchar) as cidade_destinatario,
            cast(SHIPREGION as varchar) as regiao_destinatario,
            cast(SHIPCOUNTRY as varchar) as pais_destinatario
        FROM {{ source('erp', '_order_') }}
    )

SELECT *
FROM rennamed
