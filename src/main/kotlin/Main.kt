package me.kujio

import me.kujio.me.kujio.util.ksql.KSql

fun main() {
    val entries = mapOf(
        "id" to 1,
        "name" to "xiaoming",
        "age" to 20
    ).entries
    val insertSql = KSql{"insert into users"}
    insertSql.and{"(${join(entries,","){ +it.key }})"}
    insertSql.and{"values(${join(entries,","){ +it.value }})"}
    println(insertSql.toString())
}