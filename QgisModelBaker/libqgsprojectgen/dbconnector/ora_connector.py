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

    def _get_dict_result(self, query):
        record = query.record()
        result = []
        while query.next():
            my_rec = dict()
            for x in range(record.count()):
                my_rec[record.fieldName(x).lower()] = query.value(x)
            result.append(my_rec)
        return result
