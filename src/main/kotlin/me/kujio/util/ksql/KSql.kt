package me.kujio.me.kujio.util.ksql

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

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
        val columnTypes = mutableMapOf(
            Boolean::class to BooleanColumnType,
            Int::class to IntColumnType,
            Long::class to LongColumnType,
            Float::class to FloatColumnType,
            Double::class to DoubleColumnType,
            String::class to StringColumnType
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

        fun <E : Any?> join(items: Collection<E>, separator: String, build: Builder.(E) -> String): String {
            return items.joinToString(separator) { this.build(it) }
        }
    }

    fun and(build: Builder.() -> String) {
        sqlBuilder.append(" ").append(Builder(paramList).build())
    }

    private fun prepare(connection: Connection, sql: String, values: List<Any?>): PreparedStatement {
        logger?.invoke("SQL: ${oneLineString(sql)}\nVALUES: ${values.joinToString()}")
        val stm = connection.prepareStatement(sql)
        for (index in values.indices) {
            val value = values[index]
            if (value == null) {
                stm.setNull(index + 1, java.sql.Types.NULL)
                continue
            }
            val columnType: KColumnType = columnTypes[value::class] ?: throw RuntimeException("Unsupported type: ${value::class}")
            columnType.toSql(stm, index + 1, value)
        }
        return stm
    }

    fun exec(): Int {
        var isTransaction = false
        val connection = localConnection.get()?.also { isTransaction = true } ?: getConnection().apply { autoCommit = true }
        val statement = prepare(connection, sql, values)
        val result = statement.executeUpdate()
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

private val oneLineRegex = Regex("\\s+")

/** 去除换行和多余的空格 */
private fun oneLineString(input: String): String {
    return input.replace(oneLineRegex, " ").replace(oneLineRegex, " ").trim()
}

/** 构建SQl语句，返回预处理语句 */
private fun prepare(connection: Connection, sql: String, values: List<Any?>): PreparedStatement {
    KSql.logger?.invoke("SQL: ${oneLineString(sql)}\nVALUES: ${values.joinToString()}")
    val stm = connection.prepareStatement(sql)
    for (index in values.indices) {
        val value = values[index]
        if (value == null) {
            stm.setNull(index + 1, java.sql.Types.NULL)
            continue
        }
        val columnType: KColumnType = KSql.columnTypes[value::class] ?: throw RuntimeException("Unsupported type: ${value::class}")
        columnType.toSql(stm, index + 1, value)
    }
    return stm
}

fun query(builder: SqlBuilder.() -> String): query {
    val sqlBuilder = SqlBuilder()
    val sql = sqlBuilder.builder()
    val values = sqlBuilder.values
    return query(sql, values)
}

class query(private val sql: String, private val values: List<Any?>) {
    fun <T> map(transform: (ResultRow) -> T): List<T> {
        var isTransaction = false
        val connection = localConnection.get()?.also { isTransaction = true } ?: getConnection().apply { autoCommit = true }
        val statement = prepare(connection, sql, values)
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
}

fun exec(builder: SqlBuilder.() -> String): Int {
    val sqlBuilder = SqlBuilder()
    val sql = sqlBuilder.builder()
    val values = sqlBuilder.values
    return exec(sql, values)
}

/**
 * 执行Sql语句
 * @return 受影响的行数
 */
fun exec(sql: String, values: List<Any?>): Int {
    var isTransaction = false
    val connection = localConnection.get()?.also { isTransaction = true } ?: getConnection().apply { autoCommit = true }
    val statement = prepare(connection, sql, values)
    val result = statement.executeUpdate()
    statement.close()
    if (!isTransaction) connection.close()
    return result
}

/**
 * Sql语句构建器
 */
class SqlBuilder {
    val values = mutableListOf<Any?>()
    operator fun Any?.unaryPlus(): String {
        this?.let { values.add(it) } ?: values.add(null)
        return "?"
    }

    fun on(boolean: Boolean, build: SqlBuilder.() -> String): String {
        return if (boolean) this.build() else ""
    }

}

/** 查询结果行 */
class ResultRow(val resultSet: ResultSet) {
    inline operator fun <reified T> get(filed: String): T? {
        val columnType = KSql.columnTypes[T::class] ?: throw RuntimeException("Unsupported type: ${T::class}")
        val value = columnType.fromSql(resultSet, filed)
        if (resultSet.wasNull()) return null
        return value as T
    }
}

/** 类型映射 */
interface KColumnType {
    fun toSql(stm: PreparedStatement, index: Int, value: Any)
    fun fromSql(resultSet: ResultSet, field: String): Any?
}

object BooleanColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setBoolean(index, value as Boolean)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any {
        return resultSet.getBoolean(field)
    }
}

object IntColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setInt(index, value as Int)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any {
        return resultSet.getInt(field)
    }
}

object LongColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setLong(index, value as Long)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any {
        return resultSet.getLong(field)
    }
}

object FloatColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setFloat(index, value as Float)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any {
        return resultSet.getFloat(field)
    }
}

object DoubleColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setDouble(index, value as Double)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any {
        return resultSet.getDouble(field)
    }
}

object StringColumnType : KColumnType {
    override fun toSql(stm: PreparedStatement, index: Int, value: Any) {
        stm.setString(index, value as String)
    }

    override fun fromSql(resultSet: ResultSet, field: String): Any? {
        return resultSet.getString(field)
    }
}