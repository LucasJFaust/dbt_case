WITH
    ordens as (
        SELECT *
        FROM {{ ref('stg_erp__ordens') }}
    ),

    ordens_detalhes as (
        SELECT *
        FROM {{ ref('stg_erp__ordens_detalhes') }}
    ),

    joined as (
        SELECT
            ordens_detalhes.FK_PEDIDO,
            ordens_detalhes.FK_PRODUTO,
            ordens.FK_FUNCIONARIO,
            ordens.FK_CLIENTE,
            ordens.FK_TRANSPORTADORA,
            ordens.DATA_DO_PEDIDO,
            ordens.DATA_DO_ENVIO,
            ordens.DATA_REQUERIDA_ENTREGA,
            ordens_detalhes.DESCONTO_PREC,
            ordens_detalhes.PRECO_DA_UNIDADE,
            ordens_detalhes.QUANTIDADE,
            ordens.FRETE,
            ordens.NOME_DESTINATARIO,
            ordens.CIDADE_DESTINATARIO,
            ordens.REGIAO_DESTINATARIO,
            ordens.PAIS_DESTINATARIO
        FROM ordens_detalhes
        INNER JOIN ordens
            ON ordens_detalhes.fk_pedido = ordens.pk_pedido
    ),

    metricas as (
        SELECT
        *,
        PRECO_DA_UNIDADE * QUANTIDADE as total_bruto,
        PRECO_DA_UNIDADE * (1- DESCONTO_PREC) * QUANTIDADE as total_liquido,
        FRETE / count(*) over(partition by fk_pedido) as frate_rateado,
        PRECO_DA_UNIDADE * (1- DESCONTO_PREC) * QUANTIDADE - FRETE / count(*) over(partition by fk_pedido) as lucro
        FROM joined
    ),

    chave_primaria as (
        SELECT
        fk_pedido::varchar || '-' || fk_produto::varchar as sk_vendas,
        *
        FROM metricas
    )

SELECT *
FROM chave_primaria