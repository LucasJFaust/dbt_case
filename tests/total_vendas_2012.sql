WITH
    vendas_em_2012 as (
        SELECT 
            sum(total_bruto) as total_bruto
        FROM 
            {{ ref('fct_vendas') }}
        WHERE 
            data_do_pedido between '2012-01-01' and '2012-12-31'
    ) --26298.5

SELECT total_bruto
FROM vendas_em_2012
WHERE total_bruto != 26298.5