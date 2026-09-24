# ANSWERS.md — бонус: `build_summary` за один прохід

`job.py` містить дві реалізації кроку 9:

* `build_summary(events, dimensions)` — базове рішення зі SPEC.md: цикл по
  вимірах, для кожного — окремий `summary_slice` (свій `groupBy` +
  `countDistinct`), результати склеєні `unionByName`.
* `build_summary_single_pass(events, dimensions)` — бонус: будуємо масив
  `struct(dimension, dimension_value)` по одному елементу на вимір,
  розгортаємо його `explode`, і рахуємо все **одним** `groupBy(dimension,
  dimension_value)`.

Обидва варіанти перевірено на реальних даних (`events` після `clean` +
`with_derived`, 29 750 рядків): результати повністю ідентичні —
`25 102` рядки, однакові `columns`, однакові рядки після сортування за
`(dimension, dimension_value)`.

## Різниця планів

`events.cache()` матеріалізується один раз для обох варіантів — тому
`InMemoryTableScan` на вході однаковий, і різниця в планах — це виключно
те, що відбувається **після** сканування кешу.

### `build_summary` (union з чотирьох slice)

Кожен із 4 `summary_slice` — це незалежний `groupBy(dimension_value).agg(count,
countDistinct)`. `countDistinct` у Spark реалізовано як **два** проходи
хеш-агрегації з шаффлом між ними (спочатку групування по
`(dimension_value, repo_name)` аби прибрати дублікати `repo_name`, потім
групування по `dimension_value` для фінальних лічильників). Тобто на кожен
вимір — **2 Exchange**. `Union` сам по собі шаффлу не додає, лише зшиває
результати гілок.

```
Union
├─ HashAggregate → Exchange → HashAggregate → HashAggregate → Exchange → HashAggregate   (event_type)
├─ HashAggregate → Exchange → HashAggregate → HashAggregate → Exchange → HashAggregate   (repo_owner)
├─ HashAggregate → Exchange → HashAggregate → HashAggregate → Exchange → HashAggregate   (actor_login)
└─ HashAggregate → Exchange → HashAggregate → HashAggregate → Exchange → HashAggregate   (hour)
```

**Разом: 4 гілки × 2 Exchange = 8 Exchange** (не рахуючи одноразового
сканування кешу). Кожна гілка незалежно перечитує `InMemoryTableScan`
кешованого `events` — фізично це дешево (дані вже в пам'яті), але Spark
усе одно планує 4 окремих сканування замість одного.

### `build_summary_single_pass` (explode + один groupBy)

```
HashAggregate → Exchange → HashAggregate → HashAggregate → Exchange → HashAggregate
                                                                              ↑
                                                                     Generate (explode)
                                                                              ↑
                                                                InMemoryTableScan (один раз)
```

Один `Generate` (explode) розмножує кожен рядок `events` у 4 рядки-пари
`(dimension, dimension_value)` — тому HashAggregate далі працює над
`4 × 29 750 = 119 000` проміжних рядків замість `29 750`, зате
**сканування кешу й Generate — один раз**, і `countDistinct` (той самий
двопрохідний трюк зі своїми 2 Exchange) виконується теж **один раз** —
над усіма вимірами одразу, бо `dimension` тепер частина ключа групування.

**Разом: 2 Exchange** — рівно ті, що потрібні для одного `countDistinct`,
незалежно від кількості вимірів.

### Висновок про плани

Різниця не в тому, що `union`-варіант «читає events N разів з диска» —
`cache()` це вже виправляє. Різниця в тому, що `union`-варіант виконує
**N незалежних countDistinct-агрегацій** (кожна зі своїм шаффлом), тоді як
`explode`-варіант зводить усе до **однієї** countDistinct-агрегації з
розширеним ключем групування. Кількість Exchange росте лінійно з
кількістю вимірів у `union`-варіанті (`2 × N`) і залишається сталою (`2`)
в `explode`-варіанті. Ціна — ширший проміжний набір даних після `explode`
(`N` разів більше рядків до агрегації), але це набагато дешевше за
повторний шафл.

## Час виконання

Виміряно на тих самих `events` (29 750 рядків, `SUMMARY_DIMENSIONS` — усі
4 виміри), `local[*]`, `spark.sql.shuffle.partitions = 4`. Кеш `events`
прогрітий заздалегідь (виключено з таймінгу). 5 повторів на варіант,
`.count()` для форсування виконання:

| Варіант | Прогони, с | min, с | avg, с |
|---|---|---|---|
| `build_summary` (union) | 1.247, 0.749, 0.488, 0.453, 0.470 | **0.453** | 0.681 |
| `build_summary_single_pass` (explode) | 0.556, 0.351, 0.261, 0.274, 0.231 | **0.231** | 0.335 |

`explode`-варіант приблизно **вдвічі швидший** за `min`- і `avg`-часом на
цьому обсязі даних (36 000 сирих рядків → 29 750 після `clean`). Перший
прогін кожного варіанта помітно повільніший за наступні — це JIT/кодоген
Spark і планування AQE «розігріваються» на перших запусках, тому
показовий саме `min`, а не перший прогін.

## Чому це узгоджується з планами

Прискорення приблизно вдвічі, а не вчетверо (за кількістю вимірів),
логічне: `local[*]`-кластер на 36 000 рядках — задача, де накладні
витрати на планування/диспетчеризацію стадій (constant overhead на
Exchange) відносно великі порівняно з фактичним обсягом даних, який
шаффлиться. `explode`-варіант прибирає 3 з 4 повторних countDistinct-
проходів (та їхні Exchange), але додає один Generate і трохи більший
обсяг даних для єдиного шаффлу (128 000 рядків замість 4 окремих шаффлів
по ~29–30 тис.). На більшому обсязі даних або з більшою кількістю
вимірів перевага `explode`-варіанту зростала б помітніше, бо там overhead
на Exchange (диспетчеризація стадій, broadcast метаданих) масштабується
з кількістю вимірів, а не з обсягом даних.
