WITH
    rennamed as (
        SELECT
            cast(ID as int) as pk_categoria,
            cast(CATEGORYNAME as varchar) as nome_categoria,
            cast(DESCRIPTION as varchar) as descricao_categoria
        FROM {{ source('erp', 'category') }}
    )

SELECT *
FROM rennamed
