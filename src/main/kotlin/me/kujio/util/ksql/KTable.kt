package me.kujio.me.kujio.util.ksql

import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

open class KTable<T : Any>(kClass: KClass<T>, table: String? = null) {
    private val tableName: String
    private val constructor: KFunction<Any>
    private val parameters: List<KParameter>
    private var properties: Collection<KProperty1<T, *>>

    init {
        if (!kClass.isData) throw RuntimeException("Not a data class: ${kClass.qualifiedName}")
        tableName = table ?: camelToSnake(kClass.simpleName!!)
        constructor = kClass.primaryConstructor!!
        parameters = constructor.parameters
        properties = kClass.memberProperties
    }

    val columns get() = properties.map { if (enabledSnakeCase) camelToSnake(it.name) else it.name }

    fun select(build: (KSql.Builder.() -> String)? = null): List<T> {
        val sql = KSql { "Select ${join(columns) { it }} From $tableName" }
        if (build != null) sql.and { "Where" }.and(build)
        return sql.query { from(it) }
    }

    fun values(data: T): Map<String, Any?> {
        return properties.associate { it.name to it.get(data) }
    }

    fun insert(data: T): Int {
        val values = values(data)
        return KSql { "Insert into $tableName(${join(values.keys) { it }}) values(${join(values.values) { +it }})" }.exec()
    }

    fun update(data: T, build: KSql.Builder.() -> String): Int {
        val values = values(data)
        val sql = KSql { "Update $tableName set ${join(values.entries) { "${it.key} = ${+it.value}" }} Where" }
        return sql.and(build).exec()
    }

    fun delete(build: KSql.Builder.() -> String): Int {
        val sql = KSql { "Delete from $tableName Where" }
        return sql.and(build).exec()
    }

    @Suppress("UNCHECKED_CAST")
    fun from(row: ResultRow): T {
        return constructor.callBy(parameters.associate {
            val name = if (enabledSnakeCase) camelToSnake(it.name!!) else it.name!!
            it to row.get(it.type,name)
        }) as T
    }

    companion object {

        /** 开启驼峰转下划线 */
        var enabledSnakeCase = false

        private fun camelToSnake(camelCase: String): String {
            return camelCase
                .replace(Regex("([a-z])([A-Z])"), "$1_$2")
                .replace(Regex("([A-Z])([A-Z][a-z])"), "$1_$2")
                .lowercase(Locale.getDefault())
        }
    }
}