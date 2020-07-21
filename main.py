from typing import Optional, List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from MySQLdb import _mysql

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# @app.on_event("startup")
# async def startup():
db = _mysql.connect(host="127.0.0.1", user="root", passwd="", db="comex")


@app.get("/")
async def root():
    return {"message": "It's works"}


@app.get("/imports")
async def get_imports(year_ref: int, reg_unit: Optional[List[int]] = Query(None), mode: Optional[str] = '1'):

    where_reg_unit = ""
    where_year_ref = ""

    where_year_ref = "WHERE import_ano = {}".format(year_ref)

    if reg_unit != None:
        if len(reg_unit) == 1:
            where_reg_unit = "AND municipios.municipios_cod_geo like '{}%'" .format(
                reg_unit[0])
        else:
            where_reg_unit = "AND municipios.municipios_cod_geo in{}".format(
                tuple(reg_unit))

    if mode == '1':
        query = """
          SELECT DISTINCT import_id, import_ano, import_mes, import_kg_liquido, import_valor_fob, 
          municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
          paises_cod, paises_nome as pais_origem, 
          sh_cod_sh4, sh_nome_sh4
          FROM imports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = imports.fk_municipio
          LEFT JOIN paises
          ON paises.paises_cod = imports.fk_pais
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = imports.fk_sh4 
          {} {} ORDER BY import_mes, municipios_cod_geo
          """.format(where_year_ref, where_reg_unit)

    elif mode == '2':
        query = """
          SELECT DISTINCT import_ano, SUM(import_kg_liquido) as total_import_kg_liquido, SUM(import_valor_fob) as total_import_valor_fob, 
          municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
          sh_cod_sh4, sh_nome_sh4
          FROM imports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = imports.fk_municipio
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = imports.fk_sh4 
          {} {} 
          GROUP BY municipios_cod_geo, sh_cod_sh4, sh_nome_sh4, municipios_nome_cap, municipios_sigla_uf
          ORDER BY municipios_cod_geo
          """.format(where_year_ref, where_reg_unit)

    elif mode == "3":
        query = """
          SELECT DISTINCT import_ano, SUM(import_kg_liquido) as total_import_kg_liquido, SUM(import_valor_fob) as total_import_valor_fob, 
          municipios_sigla_uf, 
          sh_cod_sh4, sh_nome_sh4
          FROM imports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = imports.fk_municipio
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = imports.fk_sh4 
          {} {}
          GROUP BY sh_cod_sh4, sh_nome_sh4, municipios_sigla_uf
          ORDER BY sh_cod_sh4
          """.format(where_year_ref, where_reg_unit)
    elif mode == "4":
        query = """
      SELECT DISTINCT import_ano, SUM(import_kg_liquido) as total_import_kg_liquido, SUM(import_valor_fob) as total_import_valor_fob, 
        municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
        paises_cod, paises_nome as pais_origem, 
        sh_cod_sh4, sh_nome_sh4
        FROM imports
        LEFT JOIN municipios 
        ON municipios.municipios_cod_geo = imports.fk_municipio
        LEFT JOIN paises
        ON paises.paises_cod = imports.fk_pais
        LEFT JOIN sistema_harmonizado 
        ON sistema_harmonizado.sh_cod_sh4 = imports.fk_sh4 
        {} {}
        GROUP BY municipios_cod_geo, sh_cod_sh4, sh_nome_sh4, municipios_nome_cap, municipios_sigla_uf, paises_cod, pais_origem
        ORDER BY municipios_cod_geo;
    """.format(where_year_ref, where_reg_unit)
    elif mode == "5":
        query = """
    SELECT DISTINCT import_ano, SUM(import_kg_liquido) as total_import_kg_liquido, SUM(import_valor_fob) as total_import_valor_fob, 
        municipios_sigla_uf, 
        paises_cod, paises_nome as pais_origem, 
        sh_cod_sh4, sh_nome_sh4
        FROM imports
        LEFT JOIN municipios 
        ON municipios.municipios_cod_geo = imports.fk_municipio
        LEFT JOIN paises
        ON paises.paises_cod = imports.fk_pais
        LEFT JOIN sistema_harmonizado 
        ON sistema_harmonizado.sh_cod_sh4 = imports.fk_sh4 
        {} {}
        GROUP BY sh_cod_sh4, sh_nome_sh4, municipios_sigla_uf, paises_cod, pais_origem
        ORDER BY sh_cod_sh4
        """.format(where_year_ref, where_reg_unit)
    else:
        return {"error": "Modo de agregação {} não encontrado" .format(mode)}

    db.query(query)

    # r = db.store_result()
    r = db.use_result()
    fetch_result = r.fetch_row(maxrows=0, how=1)

    return fetch_result


@app.get("/exports")
async def get_exports(year_ref: int, reg_unit: Optional[List[int]] = Query(None), mode: Optional[str] = '1'):
    """
      mode: Optional[str]
        1 : 'Dados totalmente desagregados'
        2 : 'Total exportado de cada produto de determinado(s) municípios' obs: O resultado é o total daquele ano.
        3 : 'Total exportado de cada produto daquela região' obs: Soma os produtos independente dos municípios.
    """
    where_reg_unit = ""
    where_year_ref = ""

    where_year_ref = "WHERE export_ano = {}".format(year_ref)

    if reg_unit != None:
        if len(reg_unit) == 1:
            where_reg_unit = "AND municipios.municipios_cod_geo like '{}%'" .format(
                reg_unit[0])
        else:
            where_reg_unit = "AND municipios.municipios_cod_geo in{}".format(
                tuple(reg_unit))

    query = ""

    if mode == '1':
        query = """
          SELECT DISTINCT export_id, export_ano, export_mes, export_kg_liquido, export_valor_fob, 
          municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
          paises_cod, paises_nome as pais_destino, 
          sh_cod_sh4, sh_nome_sh4
          FROM exports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = exports.fk_municipio
          LEFT JOIN paises
          ON paises.paises_cod = exports.fk_pais
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = exports.fk_sh4 
          {} {} ORDER BY export_mes, municipios_cod_geo
          """.format(where_year_ref, where_reg_unit)
    elif mode == '2':
        query = """
          SELECT DISTINCT export_ano, SUM(export_kg_liquido) as total_export_kg_liquido, SUM(export_valor_fob) as total_export_valor_fob, 
          municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
          sh_cod_sh4, sh_nome_sh4
          FROM exports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = exports.fk_municipio
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = exports.fk_sh4 
          {} {} 
          GROUP BY municipios_cod_geo, sh_cod_sh4, sh_nome_sh4, municipios_nome_cap, municipios_sigla_uf
          ORDER BY municipios_cod_geo
          """.format(where_year_ref, where_reg_unit)

    elif mode == "3":
        query = """
          SELECT DISTINCT export_ano, SUM(export_kg_liquido) as total_export_kg_liquido, SUM(export_valor_fob) as total_export_valor_fob, 
          municipios_sigla_uf, 
          sh_cod_sh4, sh_nome_sh4
          FROM exports
          LEFT JOIN municipios 
          ON municipios.municipios_cod_geo = exports.fk_municipio
          LEFT JOIN sistema_harmonizado 
          ON sistema_harmonizado.sh_cod_sh4 = exports.fk_sh4 
          {} {}
          GROUP BY sh_cod_sh4, sh_nome_sh4, municipios_sigla_uf
          ORDER BY sh_cod_sh4
          """.format(where_year_ref, where_reg_unit)
    elif mode == "4":
        query = """
            SELECT DISTINCT export_ano, SUM(export_kg_liquido) as total_export_kg_liquido, SUM(export_valor_fob) as total_export_valor_fob, 
            municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf, 
            paises_cod, paises_nome as pais_origem, 
            sh_cod_sh4, sh_nome_sh4
            FROM exports
            LEFT JOIN municipios 
            ON municipios.municipios_cod_geo = exports.fk_municipio
            LEFT JOIN paises
            ON paises.paises_cod = exports.fk_pais
            LEFT JOIN sistema_harmonizado 
            ON sistema_harmonizado.sh_cod_sh4 = exports.fk_sh4 
            {} {}
            GROUP BY municipios_cod_geo, sh_cod_sh4, sh_nome_sh4, municipios_nome_cap, municipios_sigla_uf, paises_cod, pais_origem
            ORDER BY municipios_cod_geo;
        """.format(where_year_ref, where_reg_unit)
    elif mode == "5":
        query = """
            SELECT DISTINCT export_ano, SUM(export_kg_liquido) as total_export_kg_liquido, SUM(export_valor_fob) as total_export_valor_fob, 
            municipios_sigla_uf, 
            paises_cod, paises_nome as pais_destino, 
            sh_cod_sh4, sh_nome_sh4
            FROM exports
            LEFT JOIN municipios 
            ON municipios.municipios_cod_geo = exports.fk_municipio
            LEFT JOIN paises
            ON paises.paises_cod = exports.fk_pais
            LEFT JOIN sistema_harmonizado 
            ON sistema_harmonizado.sh_cod_sh4 = exports.fk_sh4 
            {} {}
            GROUP BY sh_cod_sh4, sh_nome_sh4, municipios_sigla_uf, paises_cod, pais_origem
            ORDER BY sh_cod_sh4
            """.format(where_year_ref, where_reg_unit)
    else:
        return {"error": "Modo de agregação {} não encontrado" .format(mode)}

    db.query(query)

    r = db.use_result()
    fetch_result = r.fetch_row(maxrows=0, how=1)

    return fetch_result


@app.get('/harmonized_system')
async def fetch_sh():
    db.query(
        """
      SELECT DISTINCT sh_cod_sh4, sh_nome_sh4 FROM sistema_harmonizado
    """)

    r = db.use_result()
    fetch_result = r.fetch_row(maxrows=0, how=1)

    return fetch_result


@app.get('/regional_unit')
async def fetch_regional_unit():
    db.query(
        """
      SELECT municipios_cod_geo, municipios_nome_cap, municipios_sigla_uf FROM municipios ORDER BY municipios_sigla_uf
    """
    )

    r = db.use_result()
    fetch_result = r.fetch_row(maxrows=0, how=1)

    return fetch_result
