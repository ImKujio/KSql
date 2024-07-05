package me.kujio.me.kujio.util.ksql

import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.reflect.KType
import kotlin.reflect.typeOf


/** 类型映射 */
interface KColumnType {
    fun toSql(stm: PreparedStatement, index: Int, value: Any)
    fun fromSql(resultSet: ResultSet, field: String): Any?
}

inline fun <reified T> getTypeFromInstance(instance: T): KType {
    return typeOf<T>()
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