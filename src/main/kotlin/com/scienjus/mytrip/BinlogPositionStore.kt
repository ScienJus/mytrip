package com.scienjus.mytrip

import java.io.FileInputStream
import java.io.FileOutputStream

/**
 * @author XieEnlong
 * @date 2016/3/8.
 */
object BinlogPositionStore {

    val fileName = "binlog.pos"

    fun store(binlogPosition: BinlogPosition) {
        val output = FileOutputStream(fileName)
        output.bufferedWriter().use {
            val str = binlogPosition.fileName + "/" + binlogPosition.position
            println(str)
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