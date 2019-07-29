# -*- coding: utf-8 -*-
"""
/***************************************************************************
    begin                :    19/07/19
    git sha              :    :%H$
    copyright            :    (C) 2019 by Yesid Polanía (BSF-Swissphoto)
    email                :    yesidpol.3@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import re
from .db_connector import (DBConnector, DBConnectorError)
from qgis.core import QgsDataSourceUri
from PyQt5.QtSql import *


class OraConnector(DBConnector):
    METADATA_TABLE = 'T_ILI2DB_TABLE_PROP'
    METAATTRS_TABLE = 'T_ILI2DB_META_ATTRS'

    def __init__(self, uri, schema):
        DBConnector.__init__(self, uri, schema)

        db = QSqlDatabase.addDatabase("QOCISPATIAL")

        data_source_uri = QgsDataSourceUri(uri)

        if not db.isValid():
            error = 'ERROR: ' + db.lastError().text()
            raise DBConnectorError(error)

        host = data_source_uri.host()
        if not host:
            host = 'localhost'

        db.setHostName(host)
        db.setDatabaseName(data_source_uri.database())
        db.setUserName(data_source_uri.username())
        db.setPassword(data_source_uri.password())

        port = data_source_uri.port()

        if port:
            try:
                port_int = int(port)
            except ValueError:
                raise DBConnectorError("The port must be an integer")

            db.setPort(port_int)

        if not db.open():
            error = db.lastError().text()
            raise DBConnectorError(error)

        self.conn = db
        self.schema = schema

        self._bMetadataTable = self._metadata_exists()
        # TODO ilicode??

    def db_or_schema_exists(self):
        result=False
        if self.schema:
            query = QSqlQuery(self.conn)
            stmt = """SELECT count(USERNAME) AS COUNT FROM DBA_USERS where USERNAME='{}'""".format(self.schema)
            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)
            if query.next():
                result=bool(query.value(0))
        return result

    def metadata_exists(self):
        return self._bMetadataTable

    def _metadata_exists(self):
        return self._table_exists(OraConnector.METADATA_TABLE)

    def get_tables_info(self):
        res = []
        if self.schema:
            ln = "\n"
            stmt = ""

            metadata_exists = self.metadata_exists()

            stmt += ln + "SELECT DISTINCT "
            stmt += ln + "      tbls.OWNER AS schemaname"
            stmt += ln + "    , tbls.TABLE_NAME AS tablename"
            stmt += ln + "    , Col.COLUMN_NAME AS primary_key"
            stmt += ln + "    , clm.COLUMN_NAME AS geometry_column"
            if metadata_exists:
                stmt += ln + "    , tsrid.SETTING as srid"
                stmt += ln + "    , p.SETTING AS kind_settings"
                stmt += ln + "    , alias.SETTING AS table_alias"
                stmt += ln + "    , SUBSTR(c.ILINAME, 0,INSTR(ILINAME,'.')-1) AS model"
                stmt += ln + "    , c.ILINAME AS ili_name"
                stmt += ln + "    , ( SELECT LISTAGG(SETTING, ',') WITHIN GROUP (ORDER BY CASE"
                stmt += ln + "            WHEN cprop.tag='ch.ehi.ili2db.c1Min' THEN 1"
                stmt += ln + "            WHEN cprop.tag='ch.ehi.ili2db.c2Min' THEN 2"
                stmt += ln + "            WHEN cprop.tag='ch.ehi.ili2db.c1Max' THEN 3"
                stmt += ln + "            WHEN cprop.tag='ch.ehi.ili2db.c2Max' THEN 4 END)"
                stmt += ln + "        FROM \"{schema}\".T_ILI2DB_COLUMN_PROP cprop"
                stmt += ln + "        WHERE tbls.TABLE_NAME = UPPER(cprop.TABLENAME) AND clm.COLUMN_NAME = UPPER(cprop.COLUMNNAME)"
                stmt += ln + "            AND cprop.tag IN ('ch.ehi.ili2db.c1Min', 'ch.ehi.ili2db.c2Min', 'ch.ehi.ili2db.c1Max', 'ch.ehi.ili2db.c2Max')"
                stmt += ln + "        ) AS extent"
                stmt += ln + "    , tgeomtype.SETTING as simple_type"
                stmt += ln + "    , NULL AS formatted_type"
            stmt += ln + "FROM ALL_CONSTRAINTS Tab"
            stmt += ln + "INNER JOIN ALL_CONS_COLUMNS Col"
            stmt += ln + "    ON Col.CONSTRAINT_NAME = Tab.CONSTRAINT_NAME"
            stmt += ln + "    AND Col.TABLE_NAME = Tab.TABLE_NAME"
            stmt += ln + "    AND Tab.OWNER = Col.OWNER"
            stmt += ln + "    AND Tab.CONSTRAINT_TYPE= 'P'"
            stmt += ln + "RIGHT JOIN ALL_TABLES tbls"
            stmt += ln + "    ON Tab.TABLE_NAME = tbls.TABLE_NAME "
            stmt += ln + "    AND Tab.OWNER = tbls.OWNER"
            stmt += ln + "LEFT JOIN ALL_TAB_COLUMNS clm"
            stmt += ln + "    ON clm.TABLE_NAME = tbls.TABLE_NAME"
            stmt += ln + "    AND clm.OWNER = tbls.OWNER"
            stmt += ln + "    AND clm.DATA_TYPE = 'SDO_GEOMETRY'"
            if metadata_exists:
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_TABLE_PROP p"
                stmt += ln + "    ON UPPER(p.TABLENAME) = tbls.TABLE_NAME "
                stmt += ln + "    AND p.TAG = 'ch.ehi.ili2db.tableKind' "
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_TABLE_PROP alias"
                stmt += ln + "    ON UPPER(alias.TABLENAME) = tbls.TABLE_NAME"
                stmt += ln + "    AND alias.TAG = 'ch.ehi.ili2db.dispName'"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_CLASSNAME c"
                stmt += ln + "    ON tbls.TABLE_NAME = UPPER(c.SQLNAME)"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_COLUMN_PROP tsrid"
                stmt += ln + "    ON tbls.TABLE_NAME = UPPER(tsrid.TABLENAME) "
                stmt += ln + "    AND clm.COLUMN_NAME = UPPER(tsrid.COLUMNNAME)"
                stmt += ln + "    AND tsrid.TAG='ch.ehi.ili2db.srid'"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_COLUMN_PROP tgeomtype"
                stmt += ln + "    ON tbls.TABLE_NAME = UPPER(tgeomtype.TABLENAME) "
                stmt += ln + "    AND clm.COLUMN_NAME = UPPER(tgeomtype.COLUMNNAME)"
                stmt += ln + "    AND tgeomtype.TAG='ch.ehi.ili2db.geomType'"
            stmt += ln + "WHERE tbls.OWNER = '{schema}'"
            stmt = stmt.format(schema=self.schema)

            query = QSqlQuery(self.conn)

            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)

            res = self._get_dict_result(query)
            for row in res:
                row['srid'] = int(row['srid']) if row['srid'] else None
                row['type'] = row['simple_type']
        return res

    def _table_exists(self, tablename):
        result = False

        if self.schema:
            query = QSqlQuery(self.conn)
            stmt = """SELECT COUNT(TABLE_NAME) AS count FROM ALL_TABLES
                WHERE OWNER='{}' AND TABLE_NAME='{}'""".format(self.schema, tablename)
            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)
            if query.next():
                result = bool(query.value(0))
        return result

    def get_meta_attrs(self, ili_name):
        if not self._table_exists(OraConnector.METAATTRS_TABLE):
            return []
        result = []

        if self.schema:
            query = QSqlQuery(self.conn)
            stmt = """SELECT ATTR_NAME, ATTR_VALUE FROM {schema}.{metaattrs_table} WHERE ILIELEMENT='{ili_name}'"""\
                .format(schema=self.schema, metaattrs_table=OraConnector.METAATTRS_TABLE, ili_name=ili_name)

            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)

            result = self._get_dict_result(query)
        return result

    def get_fields_info(self, table_name):
        res = []
        # Get all fields for this table
        if self.schema:
            ln = "\n"
            stmt = ""
            metadata_exists = self.metadata_exists()

            stmt += ln + "SELECT DISTINCT"
            stmt += ln + "      c.COLUMN_NAME"
            stmt += ln + "    , c.DATA_TYPE AS data_type"
            stmt += ln + "    , c.DATA_SCALE AS numeric_scale -- rev"
            if metadata_exists:
                stmt += ln + "    , unit.setting AS unit"
                stmt += ln + "    , txttype.setting AS texttype"
                stmt += ln + "    , alias.setting AS column_alias"
                stmt += ln + "    , full_name.iliname AS fully_qualified_name"
            stmt += ln + "    , cmmts.COMMENTS AS \"COMMENT\" -- rev"
            stmt += ln + "FROM ALL_TAB_COLUMNS c"
            if metadata_exists:
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_COLUMN_PROP unit"
                stmt += ln + "    ON c.TABLE_NAME = UPPER(unit.TABLENAME)"
                stmt += ln + "    AND c.COLUMN_NAME = UPPER(unit.COLUMNNAME)"
                stmt += ln + "    AND unit.TAG = 'ch.ehi.ili2db.unit'"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_COLUMN_PROP txttype"
                stmt += ln + "    ON c.TABLE_NAME = UPPER(txttype.TABLENAME)"
                stmt += ln + "    AND c.COLUMN_NAME = UPPER(txttype.columnname)"
                stmt += ln + "    AND txttype.TAG = 'ch.ehi.ili2db.textKind'"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_COLUMN_PROP alias"
                stmt += ln + "    ON c.TABLE_NAME = UPPER(alias.TABLENAME)"
                stmt += ln + "    AND c.COLUMN_NAME = UPPER(alias.COLUMNNAME)"
                stmt += ln + "    AND alias.TAG = 'ch.ehi.ili2db.dispName'"
                stmt += ln + "LEFT JOIN \"{schema}\".T_ILI2DB_ATTRNAME full_name"
                stmt += ln + "    ON  c.TABLE_NAME =UPPER(full_name.OWNER)"
                stmt += ln + "    AND c.COLUMN_NAME=UPPER(full_name.sqlname)"
            stmt += ln + "LEFT JOIN ALL_COL_COMMENTS cmmts"
            stmt += ln + "    ON c.OWNER = cmmts.OWNER"
            stmt += ln + "    AND c.TABLE_NAME = cmmts.TABLE_NAME"
            stmt += ln + "    AND c.COLUMN_NAME = cmmts.COLUMN_NAME"
            stmt += ln + "WHERE c.TABLE_NAME = UPPER('{table}')"
            stmt += ln + "    AND c.OWNER = '{schema}'"
            stmt = stmt.format(schema=self.schema,table=table_name)

            query = QSqlQuery(self.conn)

            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)

            res = self._get_dict_result(query)

        return res

    def get_constraints_info(self, table_name):
        res = {}

        if self.schema:
            ln = "\n"
            stmt = ""

            stmt += ln + "SELECT SEARCH_CONDITION"
            stmt += ln + "FROM ALL_CONSTRAINTS"
            stmt += ln + "WHERE"
            stmt += ln + "    CONSTRAINT_TYPE='C'"
            stmt += ln + "    AND OWNER = '{schema}'"
            stmt += ln + "    AND TABLE_NAME=UPPER('{table}')"

            stmt = stmt.format(schema=self.schema, table=table_name)

            query = QSqlQuery(self.conn)

            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)

            regex = r"[\t ]+([A-Za-z_0-9]+) BETWEEN ([+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?) AND ([+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)"

            while query.next():
                constraint_expression = query.value(0)
                m = re.match(regex, constraint_expression)

                if m:
                    res[m.group(1)] = (m.group(2), m.group(3))

        return res

    def get_relations_info(self, filter_layer_list=[]):
        res = []

        if self.schema:
            ln = "\n"
            stmt = ""

            stmt += ln + "SELECT "
            stmt += ln + "      RR.CONSTRAINT_NAME AS constraint_name"
            stmt += ln + "    , FK_COL.TABLE_NAME AS referencing_table"
            stmt += ln + "    , FK_COL.COLUMN_NAME AS referencing_column"
            stmt += ln + "    , RR.OWNER AS constraint_schema"
            stmt += ln + "    , PK_COL.TABLE_NAME AS referenced_table"
            stmt += ln + "    , PK_COL.COLUMN_NAME AS referenced_column"
            stmt += ln + "    , FK_COL.POSITION AS ordinal_position"
            stmt += ln + "FROM ALL_CONSTRAINTS  RR"
            stmt += ln + "INNER JOIN ALL_CONS_COLUMNS FK_COL"
            stmt += ln + "    ON RR.OWNER = FK_COL.OWNER"
            stmt += ln + "    AND RR.CONSTRAINT_NAME = FK_COL.CONSTRAINT_NAME"
            stmt += ln + "INNER JOIN ALL_CONS_COLUMNS PK_COL"
            stmt += ln + "    ON RR.R_OWNER = PK_COL.OWNER"
            stmt += ln + "    AND RR.R_CONSTRAINT_NAME = PK_COL.CONSTRAINT_NAME"
            stmt += ln + "WHERE RR.CONSTRAINT_TYPE = 'R'"
            stmt += ln + "    AND RR.OWNER='{schema}'"
            if filter_layer_list:
                stmt += ln + "    AND RR.TABLE_NAME IN ('{}')".format("','".join(filter_layer_list))
            stmt += ln + "ORDER BY RR.CONSTRAINT_NAME, FK_COL.POSITION"

            stmt = stmt.format(schema=self.schema)

            query = QSqlQuery(self.conn)

            if not query.exec(stmt):
                error = query.lastError().text()
                raise DBConnectorError(error)

            res = self._get_dict_result(query)

        return res

    @staticmethod
    def _get_dict_result(query):
        record = query.record()
        result = []
        while query.next():
            my_rec = dict()
            for x in range(record.count()):
                my_rec[record.fieldName(x).lower()] = query.value(x)
            result.append(my_rec)
        return result
