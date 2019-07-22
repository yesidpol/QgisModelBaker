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

        # TODO is there a field missing?
        db.setHostName(data_source_uri.host())
        db.setDatabaseName(data_source_uri.database())
        db.setUserName(data_source_uri.username())
        db.setPassword(data_source_uri.password())

        if not db.open():
            error = 'ERROR: ' + db.lastError().text()
            raise DBConnectorError(error)

        self.conn = db
        self.schema = schema

        self._bMetadataTable = self._metadata_exists()
        # TODO ilicode??
        db.close()

    def metadata_exists(self):
        return self._bMetadataTable

    def _metadata_exists(self):
        return self._table_exists(OraConnector.METADATA_TABLE)

    def _table_exists(self, tablename):
        result = False

        if self.schema:
            cur = QSqlQuery(self.conn)
            query = """SELECT COUNT(TABLE_NAME) AS count FROM ALL_TABLES
                WHERE OWNER='{}' AND TABLE_NAME='{}'""".format(self.schema, tablename)
            cur.exec(query)
            if cur.next():
                result = bool(cur.value(0))
        return result
