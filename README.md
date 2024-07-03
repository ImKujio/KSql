### 基础用法

```kotlin
val entries = mapOf(
    "id" to 1
    "name" to "xiaoming"
    "age" to 20
).entries
val insertSql = sql{"insert into users"}
insertSql.and{"(${join(entries,","){e -> "${+e.key}"}})"}
insertSql.and{"values(${join(entries,","){e -> "${+e.value}"}})"}
insertSql.exec()


val fields = listOf("id","name","age")
val age = 18
val name: String? = "小"

val querySql = sql{"select ${join(fields,","){ e -> "${+e}" }}"}
querySql.and{"from users where age > ${+age}"}
querySql.and{"${on(name != null){ "and name like ${+"%$name%"}" }}"}
val item = querySql.query{row -> Item.form(row)}

```

### 额外用法

```kotlin
@Serializable
data class User(
    val name: String,
    val age: Int
){
    companion object : Table()
}

val newItem = User(name="小明",age=18)

val values : Map<String,Any> = User.values(newItem)
val columns : List<String> = User.columns()

User.insert(newItem)
User.update(newItem){"id = ${+it.id}"}
User.delete{"id = ${+id}"}

val items:List<Item> = User.find{"id = ${+id}"}
```

