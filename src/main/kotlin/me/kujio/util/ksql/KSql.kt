package me.kujio.me.kujio.util.ksql

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.reflect.KType
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

/**
 * @author kujio
 * @since 1.0
 *
 * 简易的数据库工具
 * 特性：简单，快速，安全，支持事务，可扩展
 */

class KSql(build: Builder.() -> String) {
    private val paramList = mutableListOf<Any?>()
    private val sqlBuilder = StringBuilder(Builder(paramList).build())

    val sql: String get() = oneLineString(sqlBuilder.toString())
    val params: List<Any?> get() = paramList

    companion object {
        /** 连接获取器 */
        var connection: (() -> Connection)? = null

        /** 日志处理器 */
        var logger: ((String) -> Unit)? = { println(it) }

        /** 类型映射 */
        val columnTypes = mutableMapOf<KType, KColumnType>(
            typeOf<Boolean>() to BooleanColumnType,
            typeOf<Int>() to IntColumnType,
            typeOf<Long>() to LongColumnType,
            typeOf<Float>() to FloatColumnType,
            typeOf<Double>() to DoubleColumnType,
            typeOf<String>() to StringColumnType,
        )
    }

    class Builder(private val paramList: MutableList<Any?>) {

        operator fun Any?.unaryPlus(): String {
            this?.let { paramList.add(it) } ?: paramList.add(null)
            return "?"
        }

        fun on(boolean: Boolean, build: Builder.() -> String): String {
            return if (boolean) this.build() else ""
        }

        fun <E : Any?> join(items: Collection<E>, separator: String = ",", build: Builder.(E) -> String): String {
            return items.joinToString(separator) { this.build(it) }
        }
    }

    fun and(build: Builder.() -> String): KSql {
        sqlBuilder.append(" ").append(Builder(paramList).build())
        return this
    }

    private fun prepare(connection: Connection): PreparedStatement {
        logger?.invoke("SQL: ${oneLineString(sql)}\nVALUES: ${paramList.joinToString()}")
        val stm = connection.prepareStatement(sql)
        for (index in paramList.indices) {
            val value = paramList[index]
            if (value == null) {
                stm.setNull(index + 1, java.sql.Types.NULL)
                continue
            }
            val kType = value::class.starProjectedType
            val columnType: KColumnType =
                columnTypes[kType] ?: throw RuntimeException("Unsupported type: ${kType.toString()},param index:$index, param value:$value")
            columnType.toSql(stm, index + 1, value)
        }
        return stm
    }

    fun exec(): Int {
        var isTransaction = false
        val connection = localConnection.get()?.also { isTransaction = true } ?: getConnection().apply { autoCommit = true }
        val statement = prepare(connection)
        val result = statement.executeUpdate()
        statement.close()
        if (!isTransaction) connection.close()
        return result
    }

    fun <T> query(transform: (row: ResultRow) -> T): List<T> {
        var isTransaction = false
        val connection = localConnection.get()?.also { isTransaction = true } ?: getConnection().apply { autoCommit = true }
        val statement = prepare(connection)
        val resultSet = statement.executeQuery()
        val result = mutableListOf<T>()
        while (resultSet.next()) {
            result.add(transform(ResultRow(resultSet)))
        }
        resultSet.close()
        statement.close()
        if (!isTransaction) connection.close()
        return result
    }

    override fun toString(): String {
        return "${sql}\n${params}"
    }
}

private fun getConnection(): Connection {
    return KSql.connection?.invoke() ?: throw RuntimeException("No connection")
}

private val localConnection = ThreadLocal<Connection>()

private val oneLineRegex = Regex("\\s+")

/** 去除换行和多余的空格 */
private fun oneLineString(input: String): String {
    return input.replace(oneLineRegex, " ").replace(oneLineRegex, " ").trim()
}

fun transaction(operation: () -> Unit) {
    KSql.logger?.invoke("Transaction start")
    val connection = getConnection()
    localConnection.set(connection)
    connection.autoCommit = false
    try {
        operation()
        connection.commit()
        KSql.logger?.invoke("Transaction commit")
    } catch (e: Exception) {
        connection.rollback()
        KSql.logger?.invoke("Transaction rollback")
        throw e
    } finally {
        connection.close()
        localConnection.remove()
    }
}

/** 查询结果行 */
class ResultRow(val resultSet: ResultSet) {

    inline operator fun <reified T> get(filed: String): T? {
        val columnType = KSql.columnTypes[T::class.starProjectedType] ?: throw RuntimeException("Unsupported type: ${T::class}")
        val value = columnType.fromSql(resultSet, filed)
        if (resultSet.wasNull()) return null
        return value as T
    }

    fun get(type: KType, filed: String): Any? {
        val realType = if (type.isMarkedNullable) type.withNullability(false) else type
        val columnType = KSql.columnTypes[realType] ?: throw RuntimeException("Unsupported type: ${type}")
        val value = columnType.fromSql(resultSet, filed)
        val wasNull = resultSet.wasNull()
        if (wasNull && type.isMarkedNullable) return null
        if (wasNull && !type.isMarkedNullable) throw RuntimeException("Column $filed is null, but type ${type.toString()} is not nullable")
        return value
    }
}