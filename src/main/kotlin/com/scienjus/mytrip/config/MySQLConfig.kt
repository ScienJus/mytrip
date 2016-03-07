package com.scienjus.mytrip.config

/**
 * @author ScienJus
 * @date 16/3/5.
 */
data class MySQLConfig(
        var host: String = "localhost",
        var port: Int = 3306,
        var username: String = "root",
        var password: String = ""
)

