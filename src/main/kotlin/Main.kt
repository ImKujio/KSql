package me.kujio

import me.kujio.me.kujio.util.ksql.KSql
import me.kujio.me.kujio.util.ksql.KTable
import java.sql.DriverManager

fun main() {
    testDb()
}

data class Test(
    val bool1: Boolean,
    val bool2: Boolean? = null,
    val int1: Int,
    val int2: Int? = null,
    val long1: Long,
    val long2: Long? = null,
    val float1: Float,
    val float2: Float? = null,
    val double1: Double,
    val double2: Double? = null,
    val string1: String,
    val string2: String? = null,
) {
    companion object : KTable<Test>(Test::class, "A_Test")
}


fun testDb() {
    val dbIp = "127.0.0.1:8829"
    val dbName = "ksql"
    val dbUser = "sa"
    val dbPwd = "kujio.me"

    /** 配置连接 */
    KSql.connection = {
        Class.forName("net.sourceforge.jtds.jdbc.Driver")
        DriverManager.setLoginTimeout(10)
        DriverManager.getConnection("jdbc:jtds:sqlserver://${dbIp}/${dbName}", dbUser, dbPwd)
    }

    /** 开启下划线 */
    KTable.enabledSnakeCase = true

    /** 简单查询 */
    val items1 = Test.select { "string1 = ${+"abc"}" }
    println(items1.joinToString())

    /** 复杂查询 */
    var string1: String? = "abc"
    val items2 = KSql {
        """
            Select ${join(Test.columns) { it }}
            From A_Test
            Where 1 = 1
            ${on(string1 != null) { "and string1 = ${+string1}" }}
        """.trimIndent()
    }.query { Test.from(it) }
    println(items2.joinToString())

    val items3 = KSql{"Select ${join(Test.columns) { it }} From A_Test"}
        .and { "Where 1 = 1 ${on(string1 != null) { "and string1 = ${+string1}" }}" }
        .query { Test.from(it) }
    println(items3.joinToString())

    /** 插入 */
    val newItem = Test(bool1 = true, int1 = 1, long1 = 1L, float1 = 1.0f, double1 = 1.0, string1 = "efg")
    Test.insert(newItem)

    /** 更新 */
    val updatedItem = Test(bool1 = true, int1 = 1, long1 = 1L, float1 = 1.0f, double1 = 1.0, string1 = "hij")
    Test.update(updatedItem) { "string1 = ${+"efg"}" }

    /** 删除 */
    Test.delete { "string1 = ${+"efg"}" }

}