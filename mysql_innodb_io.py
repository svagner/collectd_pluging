#!/usr/bin/env python
"""
<Plugin python>
    ModulePath "/path/to/modules/"
    LogTraces true
    Interactive false
    Import "mysql_tables_stat"
    <Module "mysql_tables_stat">
            Host "10.0.0.10"
            User "root"
            Password "mysecretpassword"
            Socket "/tmp/mysql.sock"
            Port 3306
            Verbose true
    </Module>
</Plugin>
"""
import collectd
import MySQLdb

__author__ = 'Stan P. <root.vagner@gmail.com>'


class MySQLIOStat(object):
    """Collectd mysql io stat per innodb table.
    This stat will be get only if performance_scheme was started.
    Support MySQL >= 5.5"""
    def __init__(self):
        """ Set default config options """
        self.config = {
            "Host": "127.0.0.1",
            "Port": "3306",
            "User": "root",
            "Password": "",
            "Socket": "",
            "Verbose": False,
        }
        self.mysql = {
            'connector': None,
            'lasterror': None,
            'reconnect': 0,
        }
        self.lastdata = dict()

    def connector(self):
        """Return the connector to the MySQL."""
        self.__debug_print__("Truing to connect to the mysql: "+str(self.config))
        if self.config['Socket'] == "":
            try:
                self.mysql['connector'] = MySQLdb.connect(
                    host=self.config['Host'],
                    user=self.config['User'],
                    passwd=self.config['Password'],
                    db="performance_schema",
                    port=int(self.config['Port'])
                )
            except Exception, e:
                collectd.error("mysql_innodb_io plugin: Couldn't to mysql:"+repr(e))
                self.mysql['reconnect'] = 10
        else:
            try:
                self.mysql['connector'] = MySQLdb.connect(
                    unix_socket=self.config['Socket'],
                    user=self.config['User'],
                    passwd=self.config['Password'],
                    db="performance_schema"
                )
            except Exception, e:
                collectd.error("mysql_innodb_io plugin: Couldn't to mysql:"+repr(e))
                self.mysql['reconnect'] = 10

    def configure(self, config):
        """ use generator: ignore all invalid nodes """
        if len(config.children) == 0:
            collectd.error('Module configuration missing')
        gen = (node for node in config.children if node.key in self.config)
        for node in gen:
            self.config[node.key] = node.values[0]
            self.__debug_print__("SetUp config key "+node.key+" to value ["+str(node.values[0])+"]")

    def getstat(self):
        """ send stat to the collectd """
        if self.mysql['reconnect'] == 1:
            self.connector()
            self.mysql['reconnect'] = 0
        if self.mysql['reconnect'] != 0:
            self.mysql['reconnect'] -= 1
            return
        mdata = self.__getmysqldata__()
        if mdata is None:
            return
        result = self.__collectdatagen__(mdata)

        for table in result.keys():
            for metric in result[table].keys():
                self.dispatch_value(result, table, metric, 'gauge')

    def __getmysqldata__(self):
        try:
            cursor = self.mysql['connector'].cursor()
        except Exception, e:
            collectd.error("mysql_innodb_io plugin: Couldn't get mysql connector:"+repr(e))
            self.mysql['reconnect'] = 10
            return None
        cursor.execute("""
                       select
                       SUBSTRING_INDEX(SUBSTRING_INDEX(FILE_NAME, '/', -1), '.', 1) as name,
                       COUNT_READ, COUNT_WRITE,
                       sum_number_of_bytes_read as bytes_read,
                       SUM_NUMBER_OF_BYTES_WRITE as BYTES_WRITE
                       from performance_schema.file_summary_by_instance where
                       EVENT_NAME='wait/io/file/innodb/innodb_data_file'
                       order by COUNT_READ desc""")
        data = cursor.fetchall()
        res = dict()
        for rec in data:
            res[rec[0]] = {
                "count_read": int(rec[1]),
                "count_write": int(rec[2]),
                "bytes_read": int(rec[3]),
                "bytes_write": int(rec[4]),
            }
        return res

    def dispatch_value(self, data, table, metric, type, type_instance=None):
        """Read a key from info response data and dispatch a value"""
        if table not in data:
            collectd.warning('mysql_innodb_io plugin: Info key not found: %s' % table)
            return

        if not type_instance:
            type_instance = table+":"+metric

        value = int(data[table][metric])
        self.__debug_print__('Sending value: %s-%s=%s' % (type, type_instance, value))

        val = collectd.Values(plugin='mysql_innodb_io')
        val.type = type
        val.type_instance = type_instance
        val.values = [value]
        val.dispatch()

    def __collectdatagen__(self, data):
        res = dict()
        for table in data.keys():
            if table not in res:
                res[table] = dict()
            for metric in data[table].keys():
                if table in self.lastdata:
                    res[table][metric] = data[table][metric] - self.lastdata[table][metric]
                else:
                    res[table][metric] = 0
        self.lastdata = data
        return res

    def shutdown(self):
        self.mysql["connector"].close()
        return

    def __debug_print__(self, message):
        if self.config["Verbose"]:
            collectd.info("mysql_innodb_io plugin:" + message)

    def __check_performance_scheme__(self):
        return

""" Init plugin """
mstat = MySQLIOStat()
""" Read config """
collectd.register_config(mstat.configure)
""" Collectd startup """
collectd.register_init(mstat.connector)
""" Periodical calling """
collectd.register_read(mstat.getstat)
""" SIGTERM to Collectd """
collectd.register_shutdown(mstat.shutdown)
