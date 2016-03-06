package com.scienjus.mytrip

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

/**
 * @author ScienJus
 * @date 16/3/5.
 */
fun <T> Connection.use(block : (Connection) -> T) : T {
    try {
        return block(this)
    } finally {
        this.close()
    }
}

fun <R> ResultSet.use(block : (ResultSet) -> R) : R {
    try {
        return block(this)
    } finally {
        this.close()
    }
}

fun <T, S : Statement> S.use(block : (S) -> T) : T {
    try {
        return block(this)
    } finally {
        close()
    }
}

fun <T> javax.sql.DataSource.use(block : (Connection) -> T): T {
    val connection = this.connection
    if (connection != null) {
        try {
            return block(connection)
        } finally {
            connection.close()
        }
    } else {
        throw IllegalStateException("No Connection returned from $this")
    }
}

fun getParameterizedTypes(obj: Any): Array<Type> {
    val superclassType = obj.javaClass.genericSuperclass;
    if (!ParameterizedType::class.java.isAssignableFrom(superclassType.javaClass)) {
        return emptyArray();
    }
    return (superclassType as ParameterizedType).actualTypeArguments;
}