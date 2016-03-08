package com.scienjus.mytrip

import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.FileOutputStream

/**
 * @author ScienJus
 * @date 2016/3/8.
 */
object BinlogPositionStore {

    val LOGGER = LoggerFactory.getLogger(BinlogPositionStore::class.java)

    val fileName = "binlog.pos"

    fun store(binlogPosition: BinlogPosition) {
        LOGGER.debug("store binlogPosition: ${binlogPosition}")

        val output = FileOutputStream(fileName)
        output.bufferedWriter().use {
            val str = binlogPosition.fileName + "/" + binlogPosition.position
            it.write(str)
        }
    }


    fun get(): BinlogPosition? {
        try {
            val input = FileInputStream(fileName)
            input.bufferedReader().use {
                val str = it.readLine()
                return BinlogPosition(str.split("/")[0], str.split("/")[1].toLong())
            }
        } catch (e: Exception) {
            return null
        }
    }

}