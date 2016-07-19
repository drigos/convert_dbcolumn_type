#!/usr/bin/env python
# -*- cod.ing: utf8 -*-

"""Docstring here."""

import sys

import psycopg2
import psycopg2.extras

import dbconf
import utils

messages = utils.Messages()

# Definições

TAB = "  "

views = (
    'raw_pk_view',
    'pk_view',
    'raw_fk_view',
    'fk_view',
    'unique_view',
    'attribute_view'
)

# Conexão

try:
    conn = psycopg2.connect(dbconf.DSN)
except:
    messages.fail("Unable to connect to the database")
    sys.exit()

# Para modificar o DB deve-se alterar o nível de isolação
conn.set_isolation_level(0)

# cur = conn.cursor()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Algoritmo
#  1) Criar a extensão "uuid-ossp"
#  2) Criar VIEWS de suporte
#  3) Armazenar informações das tabelas que usam SERIAL
#  *) Para cada tabela
#  4)    Armazenar informação das tabelas dependentes
#  5)    Criar coluna "uuid" com valores gerados automaticamente
#  6)    Remover primary key da coluna "id"
#  7)    Adicionar primary key na coluna "uuid"
#  *)    Para cada tabela dependente
#  8)       Criar coluna *_uuid
#  9)       Atualizar valores na coluna *_uuid baseado na coluna *_id
# 10)       Encontrar configurações adicionais na coluna *_id
# 11)       Remover coluna *_id
# 12)       Renomear coluna *_uuid para *_id
# 13)       Adicionar chave estrangeira a nova coluna *_id
# 14)       Aplicar configurações adicionais encontradas sobre a coluna *_id
# 15)    Remover a coluna "id"
# 16)    Alterar o nome da coluna "uuid" para "id"
# 17) Armazenar geradores de sequência que ainda não foram removidos
#  *) Para cada gerador de sequência
# 18)    Remover todos geradores de sequência

print("Preparação do banco de dados")

# Etapa 1

try:
    cur.execute("""CREATE EXTENSION "uuid-ossp";""")
except:
    messages.info("Unable CREATE EXTENSION uuid-ossp", TAB)

messages.ok("Create extension 'uuid-ossp'", TAB)

# Etapa 2

for view in reversed(views):
    try:
        cur.execute("""DROP VIEW """ + view + """;""")
    except:
        pass

try:
    cur.execute(
        """
        CREATE VIEW raw_pk_view AS
        SELECT
           conname, conrelid,
           conkey[i] AS conkey
        FROM (
           SELECT
              conname, conrelid, conkey,
              generate_series(1, array_upper(conkey, 1)) AS i
           FROM pg_constraint
           WHERE
              contype = 'p'
           ) AS ss;
        """
    )
except:
    messages.fail("Unable CREATE VIEW raw_pk_view")
    sys.exit()

messages.ok("Create view 'raw_pk_view'", TAB)

try:
    cur.execute(
        """
        CREATE VIEW pk_view AS
        SELECT
           t.relname AS table_name,
           p.conname AS pk_name,
           a.attname AS column_name,
           d.adsrc   AS default_value
        FROM
           raw_pk_view AS p
           JOIN pg_class AS t        ON t.oid = p.conrelid
           JOIN pg_attribute AS a    ON a.attrelid = t.oid
                                    AND a.attnum = p.conkey
           LEFT JOIN pg_attrdef AS d ON d.adrelid = t.oid
                                    AND d.adnum = a.attnum;
        """
    )
except:
    messages.fail("Unable CREATE VIEW pk_view")
    sys.exit()

messages.ok("Create view 'pk_view'", TAB)

try:
    cur.execute(
        """
        CREATE VIEW raw_fk_view AS
        SELECT
           conname, conrelid, confrelid,
           conkey[i] AS conkey, confkey[i] AS confkey
        FROM (
           SELECT
              conname, conrelid, confrelid, conkey, confkey,
              generate_series(1, array_upper(conkey, 1)) AS i
           FROM pg_constraint
           WHERE
              contype = 'f'
           ) AS ss;
        """
    )
except:
    messages.fail("Unable CREATE VIEW raw_fk_view")
    sys.exit()

messages.ok("Create view 'raw_fk_view'", TAB)

try:
    cur.execute(
        """
        CREATE VIEW fk_view AS
        SELECT
           tf.relname AS t_name,
           af.attname AS t_id,
           t.relname  AS dt_name,
           a.attname  AS dt_id,
           conname    AS fk_name
        FROM
           raw_fk_view AS f
           JOIN pg_attribute AS af ON af.attnum = f.confkey
                                  AND af.attrelid = f.confrelid
           JOIN pg_class AS tf     ON tf.oid = f.confrelid
           JOIN pg_attribute AS a  ON a.attnum = f.conkey
                                  AND a.attrelid = f.conrelid
           JOIN pg_class AS t      ON t.oid = f.conrelid;
        """
    )
except:
    messages.fail("Unable CREATE VIEW fk_view")
    sys.exit()

messages.ok("Create view 'fk_view'", TAB)

try:
    cur.execute(
        """
        CREATE VIEW unique_view AS
        SELECT
           t.relname AS table_name,
           i.relname AS index_name,
           a.attname AS column_name
        FROM
           pg_class AS t
           JOIN pg_attribute AS a    ON a.attrelid = t.oid
           JOIN pg_index AS ix       ON t.oid = ix.indrelid
                                    AND a.attnum = ANY(ix.indkey)
           JOIN pg_class AS i        ON i.oid = ix.indexrelid
           LEFT JOIN pg_attrdef AS d ON d.adrelid = t.oid
                                    AND d.adnum = a.attnum
        WHERE
           t.relkind = 'r'
           AND ix.indisunique IS TRUE
           AND ix.indisprimary IS FALSE
           AND t.relname NOT LIKE 'pg_%'; -- exclui tabelas padrões
        """
    )
except:
    messages.fail("Unable CREATE VIEW unique_view")
    sys.exit()

messages.ok("Create view 'unique_view'", TAB)

try:
    cur.execute(
        """
        CREATE VIEW attribute_view AS
        SELECT
           t.relname    AS table_name,
           a.attname    AS column_name,
           a.attnotnull AS not_null
        FROM
           pg_class AS t
           JOIN pg_attribute AS a    ON a.attrelid = t.oid
        WHERE
           t.relkind = 'r'
           AND a.attstattarget = -1 -- exclui atributos padrões
           AND t.relname NOT LIKE 'pg_%' -- exclui tabelas padrões
           AND t.relname NOT LIKE 'sql_%'; -- exclui tabelas padrões
        """
    )
except:
    messages.fail("Unable CREATE VIEW attribute_view")
    sys.exit()

messages.ok("Create view 'attribute_view'", TAB)

# Etapa 3

try:
    cur.execute(
        """
        SELECT table_name, pk_name
        FROM pk_view
        WHERE default_value LIKE 'nextval(%seq%)';
        """
    )
except:
    messages.fail("Unable SELECT FROM pk_view")
    sys.exit()

serial_tables = cur.fetchall()
for master_table in serial_tables:

    table_name = master_table["table_name"]
    pk_name = master_table["pk_name"]
    print("Refactory in '" + table_name + "'")

    # Etapa 4

    try:
        cur.execute(
            """
            SELECT dt_name, dt_id, fk_name
            FROM fk_view
            WHERE t_name = '""" + table_name + """';
            """
        )
    except:
        messages.fail("Unable SELECT FROM fk_view")
        sys.exit()

    dependent_tables = cur.fetchall()

    # Etapa 5

    try:
        cur.execute(
            """
            ALTER TABLE """ + table_name + """
            ADD COLUMN uuid uuid
            DEFAULT uuid_generate_v4();
            """
        )
    except:
        messages.fail("Unable ADD COLUMN uuid in " + table_name)
        sys.exit()

    # Etapa 6

    try:
        cur.execute(
            """
            ALTER TABLE """ + table_name + """
            DROP CONSTRAINT """ + table_name + """_pkey CASCADE;
            """
        )
    except:
        messages.fail("Unable DROP PRIMARY KEY from " + table_name)
        sys.exit()

    # Etapa 7

    try:
        cur.execute(
            """
            ALTER TABLE """ + table_name + """
            ADD CONSTRAINT """ + table_name + """_pkey
            PRIMARY KEY (uuid);
            """
        )
    except:
        messages.fail("Unable ADD PRIMARY KEY in " + table_name)
        sys.exit()

    messages.ok("Create new column for primary key with UUID type", TAB)

    for slave_table in dependent_tables:

        dt_name = slave_table["dt_name"]
        dt_id = slave_table["dt_id"]
        dt_uuid = dt_id[:-3] + "_uuid"
        fk_name = slave_table["fk_name"]

        # Etapa 8

        try:
            cur.execute(
                """
                ALTER TABLE """ + dt_name + """
                ADD COLUMN """ + dt_uuid + """ uuid;
                """
            )
        except:
            messages.fail("Unable ADD COLUMN " + dt_uuid + " in " + dt_name)
            sys.exit()

        # Etapa 9

        try:
            cur.execute(
                """
                UPDATE """ + dt_name + """ AS t
                SET """ + dt_uuid + """ = (
                    SELECT uuid
                    FROM """ + table_name + """
                    WHERE id = t.""" + dt_id + """
                );
                """
            )
        except:
            messages.fail("Unable UPDATE " + dt_uuid + " in " + dt_name)
            sys.exit()

        # Etapa 10: verificação de UNIQUE

        try:
            cur.execute(
                """
                SELECT index_name
                FROM unique_view
                WHERE
                    table_name = '""" + dt_name + """'
                    AND column_name = '""" + dt_id + """';
                """
            )
        except:
            messages.fail("Unable SELECT FROM unique_view")
            sys.exit()

        dt_unique_name = cur.fetchall()
        if dt_unique_name:
            dt_unique_name = dt_unique_name[0][0]

            try:
                cur.execute(
                    """
                    SELECT column_name, table_name
                    FROM unique_view
                    WHERE index_name = '""" + dt_unique_name + """';
                    """
                )
            except:
                messages.fail("Unable SELECT FROM unique_view")
                sys.exit()

            dt_unique_columns = cur.fetchall()
            columns_tmp = ""
            for column in dt_unique_columns:
                columns_tmp += column[0] + ", "
            dt_unique_columns = columns_tmp[:-2]

        # Etapa 10: verificação de PRIMARY KEY

        try:
            cur.execute(
                """
                SELECT pk_name
                FROM pk_view
                WHERE
                    table_name = '""" + dt_name + """'
                    AND column_name = '""" + dt_id + """';
                """
            )
        except:
            messages.fail("Unable SELECT FROM pk_view")
            sys.exit()

        dt_pk_name = cur.fetchall()
        if dt_pk_name:
            dt_pk_name = dt_pk_name[0][0]

            try:
                cur.execute(
                    """
                    SELECT column_name, table_name
                    FROM pk_view
                    WHERE pk_name = '""" + dt_pk_name + """';
                    """
                )
            except:
                messages.fail("Unable SELECT FROM pk_view")
                sys.exit()

            dt_pk_columns = cur.fetchall()
            columns_tmp = ""
            for column in dt_pk_columns:
                columns_tmp += column[0] + ", "
            dt_pk_columns = columns_tmp[:-2]

        # Etapa 10: verificação de NOT NULL

        try:
            cur.execute(
                """
                SELECT not_null
                FROM attribute_view
                WHERE
                    table_name = '""" + dt_name + """'
                    AND column_name = '""" + dt_id + """';
                """
            )
        except:
            messages.fail("Unable SELECT FROM attribute_view")
            sys.exit()

        dt_not_null = cur.fetchall()
        dt_not_null = dt_not_null[0][0]

        # Etapa 11

        try:
            cur.execute(
                """
                ALTER TABLE """ + dt_name + """
                DROP COLUMN """ + dt_id + """ CASCADE;
                """
            )
        except:
            messages.fail("Unable DROP COLUMN " + dt_id + " in " + dt_name)
            sys.exit()

        # Etapa 12

        try:
            cur.execute(
                """
                ALTER TABLE """ + dt_name + """
                RENAME COLUMN """ + dt_uuid + """
                    TO """ + dt_id + """;
                """
            )
        except:
            messages.fail("Unable RENAME COLUMN " + dt_uuid + " in " + dt_name)
            sys.exit()

        # Etapa 13

        try:
            cur.execute(
                """
                ALTER TABLE """ + dt_name + """
                ADD CONSTRAINT """ + fk_name + """
                    FOREIGN KEY (""" + dt_id + """)
                    REFERENCES """ + table_name + """ (uuid)
                    ON UPDATE CASCADE ON DELETE CASCADE;
                """
            )
        except:
            messages.fail("Unable ADD FOREIGN KEY in " + dt_id +
                          " in " + dt_name)
            sys.exit()

        # Etapa 14: tratamento de UNIQUE

        if dt_unique_name:
            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX """ + dt_unique_name + """
                    ON """ + dt_name + """ (""" + dt_unique_columns + """);
                    """
                )
            except:
                messages.fail("Unable CREATE UNIQUE INDEX in " + dt_name)
                sys.exit()

        # Etapa 14: tratamento de PRIMARY KEY

        if dt_pk_name:
            try:
                cur.execute(
                    """
                    ALTER TABLE """ + dt_name + """
                    ADD CONSTRAINT """ + dt_pk_name + """
                    PRIMARY KEY (""" + dt_pk_columns + """);
                    """
                )
            except:
                messages.fail("Unable ADD PRIMARY KEY in " + dt_name)
                sys.exit()

        # Etapa 14: tratamento de NOT NULL

        if dt_not_null:
            try:
                cur.execute(
                    """
                    ALTER TABLE """ + dt_name + """
                    ALTER COLUMN """ + dt_id + """
                    SET NOT NULL;
                    """
                )
            except:
                messages.fail("Unable SET NOT NULL in " + dt_id +
                              " in " + dt_name)
                sys.exit()

        messages.ok("Adjustments in the dependent table: " + dt_name, TAB)

    # Etapa 15

    try:
        cur.execute(
            """
            ALTER TABLE """ + table_name + """
            DROP COLUMN id;
            """
        )
    except:
        messages.fail("Unable DROP COLUMN id " + " in " + table_name)
        sys.exit()

    # Etapa 16

    try:
        cur.execute(
            """
            ALTER TABLE """ + table_name + """
            RENAME COLUMN uuid TO id;
            """
        )
    except:
        messages.fail("Unable RENAME COLUMN uuid in " + table_name)
        sys.exit()

    messages.ok("Final adjustments", TAB)

# try:
#     cur.execute(
#         """
#         SELECT
#             t.relname AS table_name,
#             a.attname AS column_name,
#             d.adsrc   AS default_value
#         FROM
#             pg_attrdef AS d
#             JOIN pg_class AS t     ON t.oid = d.adrelid
#             JOIN pg_attribute AS a ON a.attrelid = t.oid
#                                   AND a.attnum = d.adnum
#         WHERE
#             d.adsrc LIKE 'nextval%';
#         """
#     )
# except:
#     messages.fail("Unable SELECT FROM pg_class")
#     sys.exit()

# Etapa 17

try:
    cur.execute("""SELECT relname FROM pg_class WHERE relkind = 'S';""")
except:
    messages.fail("Unable SELECT FROM pg_class")
    sys.exit()

# Etapa 18

seq_gen = cur.fetchall()
print("Remove remaining sequence generators")
for sequence in seq_gen:
    try:
        cur.execute("""DROP SEQUENCE """ + sequence[0] + """;""")
    except:
        messages.fail("Unable DROP SEQUENCE " + sequence[0])
        sys.exit()

    messages.ok(sequence[0], TAB)

messages.ok("Final status of refactory")
