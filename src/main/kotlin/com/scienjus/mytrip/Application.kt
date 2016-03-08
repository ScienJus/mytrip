package com.scienjus.mytrip

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.*
import com.scienjus.mytrip.config.Config
import com.scienjus.mytrip.event.DeleteEvent
import com.scienjus.mytrip.event.InsertEvent
import com.scienjus.mytrip.event.LogEvent
import com.scienjus.mytrip.event.UpdateEvent
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

/**
 * @author ScienJus
 * @date 16/3/5.
 */
private val EVENT_QUEUE = LinkedBlockingQueue<LogEvent>()

private val LOGGER = LoggerFactory.getLogger("MyTrip")

fun main(args: Array<String>) {
    initConfig();

    Thread(Runnable {
        val client = TransportClient().addTransportAddress(InetSocketTransportAddress(Config.elasticsearch.host, Config.elasticsearch.port))
        while (true) {
            val bulk = client.prepareBulk()
            var event = EVENT_QUEUE.poll()
            val size = EVENT_QUEUE.size
            if (size > 0) {
                LOGGER.debug("EVENT_QUEUE size: $size")
            }
            while (event != null) {
                LOGGER.info("new event: $event")
                when (event) {
                    is InsertEvent -> indexData(bulk, client, event)
                    is UpdateEvent -> updateData(bulk, client, event)
                    is DeleteEvent -> deleteData(bulk, client, event)
                }
                event = EVENT_QUEUE.poll()
            }
            if (bulk.numberOfActions() > 0) {
                LOGGER.debug("bulk size: ${bulk.numberOfActions()}")
                bulk.execute()
            }
        }
    }).start()

    val client = BinaryLogClient(Config.mysql.host, Config.mysql.port, Config.mysql.username, Config.mysql.password)
    client.registerEventListener { event ->
        val eventData = event.getData<EventData>()
        when (eventData) {
            is TableMapEventData -> createTableInfo(eventData)
            is WriteRowsEventData   -> createInsertEvent(eventData)
            is UpdateRowsEventData  -> createUpdateEvent(eventData)
            is DeleteRowsEventData  -> createDeleteEvent(eventData)
        }
        BinlogPositionStore.store(BinlogPosition(client.binlogFilename, client.binlogPosition))
    }
    val binlogPosition = BinlogPositionStore.get()
    if (binlogPosition != null) {
        LOGGER.info("start with binlog position: $binlogPosition")
        client.binlogFilename = binlogPosition.fileName
        client.binlogPosition = binlogPosition.position
    }
    client.connect()
}

fun <T> JSONObject.getArray(key: String, clazz: Class<T>): List<T> {
    return JSON.parseArray(this.getJSONArray(key).toJSONString(), clazz)
}

fun <T> JSONObject.get(key: String, clazz: Class<T>): T {
    return JSON.parseObject(this.getJSONObject(key).toJSONString(), clazz)
}

fun initConfig() {
    val configFile = TableInfo::class.java.classLoader.getResourceAsStream("config-example.json")
    JSON.parseObject(configFile.reader().readText(), Config::class.java)
}

fun deleteData(bulk: BulkRequestBuilder, client: TransportClient, event: DeleteEvent) {
    val tableInfo = event.tableInfo
    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    bulk.add(client.prepareDelete(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString()))
}

fun indexData(bulk: BulkRequestBuilder, client: TransportClient, event: InsertEvent) {
    val tableInfo = event.tableInfo
    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    bulk.add(client.prepareIndex(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString())
            .setSource(indexData.data))
}

fun configIndexData(tableInfo: TableInfo, data: Map<String, Any?>): IndexData {
    val config = Config.tableInfo.firstOrNull {
        it.tableName == tableInfo.databaseName + "." + tableInfo.tableName
    }
    val indexName = config?.aliasName?.split(".")?.get(0) ?: tableInfo.databaseName
    val typeName = config?.aliasName?.split(".")?.get(1) ?: tableInfo.tableName
    var indexData: MutableMap<String, Any?> = mutableMapOf()
    if (config != null) {
        //去掉配置文件中不需要的字段
        if (config.columns.isNotEmpty()) {
            indexData.putAll(data.filterKeys {
                config.columns.contains(it)
            })
        }
        //根据配置文件中的别名改名
        config.columnInfo.forEach {
            if (indexData.containsKey(it.columnName)) {
                val value = indexData.remove(it.columnName)
                indexData.put(it.aliasName, value)
            }
        }
    } else {
        indexData.putAll(data)
    }
    val map = indexData.mapValues {
        when (it.value) {
            //Date会报错，所以需要转成Long
            is Date -> (it.value as Date).time
            else    -> it.value
        }
    }
    return IndexData(indexName, typeName, map)
}

fun updateData(bulk: BulkRequestBuilder, client: TransportClient, event: UpdateEvent) {
    val tableInfo = event.tableInfo

    val indexData = configIndexData(tableInfo, event.data)
    val primaryKeyValue = event.data.get(tableInfo.primaryKey)
    bulk.add(client.prepareUpdate(indexData.indexName, indexData.typeName, primaryKeyValue!!.toString())
            .setDoc(indexData.data))
}

fun isInclude(databaseName: String, tableName: String): Boolean {
    if (Config.tables.isNotEmpty() && !Config.tables.contains(databaseName + "." + tableName)) {
        return false
    }
    return true
}

fun createDeleteEvent(eventData: DeleteRowsEventData) {
    val tableInfo = getTableInfo(eventData.tableId) ?: return
    val includedColumns = eventData.includedColumns
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.add(DeleteEvent(tableInfo, rowToMap(columnInfos, includedColumns, row)))
    }
}

fun createUpdateEvent(eventData: UpdateRowsEventData) {
    val tableInfo = getTableInfo(eventData.tableId) ?: return
    val rows = eventData.rows;
    val columnInfos = tableInfo.columnInfos
    for (row in rows) {
        val before = rowToMap(columnInfos, eventData.includedColumnsBeforeUpdate, row.key)
        val data = rowToMap(columnInfos, eventData.includedColumns, row.value)
        EVENT_QUEUE.add(UpdateEvent(tableInfo, before, data))
    }
}

fun createInsertEvent(eventData: WriteRowsEventData) {
    val tableInfo = getTableInfo(eventData.tableId) ?: return
    val includedColumns = eventData.includedColumns
    val columnInfos = tableInfo.columnInfos
    for (row in eventData.rows) {
        EVENT_QUEUE.add(InsertEvent(tableInfo, rowToMap(columnInfos, includedColumns, row)))
    }
}

fun rowToMap(columnInfos: List<ColumnInfo>, includedColumns: BitSet, row: Array<Serializable>): Map<String, Any> {
    val data = HashMap<String, Any>()
    for (i in columnInfos.indices) {
        if (includedColumns.get(i)) {
            data.put(columnInfos[i].columnName, row[i])
        }
    }
    return data
}