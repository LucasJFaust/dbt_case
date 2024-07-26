WITH
    stg_funcionarios as (
        SELECT *
        FROM {{ ref('stg_erp__funcionarios') }}
    ),

    joined as (
        SELECT
            funcionarios.PK_FUNCIONARIO,
            funcionarios.NOME_FUNCIONARIO,
            gerentes.NOME_FUNCIONARIO as nome_gerente,
            funcionarios.FUNCAO_FUNCIONARIO,
            funcionarios.DT_NASCIMENTO,
            funcionarios.DT_CONTRATACAO,
            funcionarios.CIDADE_FUNCIONARIO,
            funcionarios.PAIS_FUNCIONARIO
        FROM stg_funcionarios as funcionarios
        LEFT JOIN stg_funcionarios as gerentes
            ON funcionarios.fk_gerente = gerentes.pk_funcionario
    )

SELECT *
FROM joined






    