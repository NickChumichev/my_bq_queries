WITH a AS (
SELECT -- получить список телефонов зарегистрированных пользователей
  DATE_TRUNC(DATE(create_at),MONTH) AS month_of_registration,
  CAST(id AS STRING) AS id, 
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone
  FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
  WHERE create_at >= '2024-01-01' AND id IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`)
)
, b AS (
SELECT -- определить страну по номеру
  month_of_registration,
  id AS registered_user,
  phone,
  CASE
    WHEN STARTS_WITH(phone,'20')THEN 'Египет'
    WHEN STARTS_WITH(phone,'30')THEN 'Греция'
    WHEN STARTS_WITH(phone,'33')THEN 'Франция'
    WHEN STARTS_WITH(phone,'36')THEN 'Венгрия'
    WHEN STARTS_WITH(phone,'39')THEN 'Италия'
    WHEN STARTS_WITH(phone,'45')THEN 'Дания'
    WHEN STARTS_WITH(phone,'49')THEN 'Германия'
    WHEN STARTS_WITH(phone,'52')THEN 'Мексика'
    WHEN STARTS_WITH(phone,'53')THEN 'Куба'
    WHEN STARTS_WITH(phone,'60')THEN 'Малайзия'
    WHEN STARTS_WITH(phone,'62')THEN 'Индонезия'
    WHEN STARTS_WITH(phone,'81')THEN 'Япония'
    WHEN STARTS_WITH(phone,'82')THEN 'Южная Корея'
    WHEN STARTS_WITH(phone,'91')THEN 'Индия'
    WHEN STARTS_WITH(phone,'95')THEN 'Мьянма'
    WHEN STARTS_WITH(phone,'98')THEN 'Иран'
    WHEN STARTS_WITH(phone,'212')THEN 'Марокко'
    WHEN STARTS_WITH(phone,'220')THEN 'Гамбия'
    WHEN STARTS_WITH(phone,'222')THEN 'Мавритания'
    WHEN STARTS_WITH(phone,'223')THEN 'Мали'
    WHEN STARTS_WITH(phone,'224')THEN 'Гвинея'
    WHEN STARTS_WITH(phone,'225')THEN 'Берег слоновой кости'
    WHEN STARTS_WITH(phone,'230')THEN 'Маврикий'
    WHEN STARTS_WITH(phone,'231')THEN 'Либерия'
    WHEN STARTS_WITH(phone,'233')THEN 'Гана'
    WHEN STARTS_WITH(phone,'240')THEN 'Экваториальная Гвинея'
    WHEN STARTS_WITH(phone,'241')THEN 'Габон'
    WHEN STARTS_WITH(phone,'242')THEN 'Конго'
    WHEN STARTS_WITH(phone,'243')THEN 'Дем. респ. Конго (бывш. Заир)'
    WHEN STARTS_WITH(phone,'245')THEN 'Гвинея Биссау'
    WHEN STARTS_WITH(phone,'246')THEN 'Диего Гарсиа'
    WHEN STARTS_WITH(phone,'251')THEN 'Эфиопия'
    WHEN STARTS_WITH(phone,'253')THEN 'Джибути'
    WHEN STARTS_WITH(phone,'254')THEN 'Кения'
    WHEN STARTS_WITH(phone,'258')THEN 'Мозамбик'
    WHEN STARTS_WITH(phone,'261')THEN 'Мадагаскар'
    WHEN STARTS_WITH(phone,'264')THEN 'Намибия'
    WHEN STARTS_WITH(phone,'265')THEN 'Малави'
    WHEN STARTS_WITH(phone,'266')THEN 'Лессото'
    WHEN STARTS_WITH(phone,'269')THEN 'Коморские о-ва'
    WHEN STARTS_WITH(phone,'291')THEN 'Эритрия'
    WHEN STARTS_WITH(phone,'298')THEN 'Фарерские о-ва'
    WHEN STARTS_WITH(phone,'299')THEN 'Гренландия'
    WHEN STARTS_WITH(phone,'350')THEN 'Гибралтар'
    WHEN STARTS_WITH(phone,'352')THEN 'Люксембург'
    WHEN STARTS_WITH(phone,'353')THEN 'Ирландия'
    WHEN STARTS_WITH(phone,'354')THEN 'Исландия'
    WHEN STARTS_WITH(phone,'356')THEN 'Мальта'
    WHEN STARTS_WITH(phone,'357')THEN 'Кипр'
    WHEN STARTS_WITH(phone,'358')THEN 'Финляндия'
    WHEN STARTS_WITH(phone,'370')THEN 'Литва'
    WHEN STARTS_WITH(phone,'371')THEN 'Латвия'
    WHEN STARTS_WITH(phone,'372')THEN 'Эстония'
    WHEN STARTS_WITH(phone,'373')THEN 'Молдавия'
    WHEN STARTS_WITH(phone,'377')THEN 'Монако'
    WHEN STARTS_WITH(phone,'385')THEN 'Хорватия'
    WHEN STARTS_WITH(phone,'389')THEN 'Македония'
    WHEN STARTS_WITH(phone,'420')THEN 'Чехия'
    WHEN STARTS_WITH(phone,'500')THEN 'Фолклендские о-ва'
    WHEN STARTS_WITH(phone,'502')THEN 'Гватемала'
    WHEN STARTS_WITH(phone,'503')THEN 'Сальвадор'
    WHEN STARTS_WITH(phone,'504')THEN 'Гондурас'
    WHEN STARTS_WITH(phone,'506')THEN 'Коста Рика'
    WHEN STARTS_WITH(phone,'509')THEN 'Гаити'
    WHEN STARTS_WITH(phone,'590')THEN 'Французские Антиллы'
    WHEN STARTS_WITH(phone,'592')THEN 'Гайана'
    WHEN STARTS_WITH(phone,'593')THEN 'Эквадор'
    WHEN STARTS_WITH(phone,'594')THEN 'Французская Гвиана'
    WHEN STARTS_WITH(phone,'596')THEN 'Мартиника'
    WHEN STARTS_WITH(phone,'671')THEN 'Гуам'
    WHEN STARTS_WITH(phone,'674')THEN 'Науру'
    WHEN STARTS_WITH(phone,'679')THEN 'Фиджи'
    WHEN STARTS_WITH(phone,'682')THEN 'О-ва Кука'
    WHEN STARTS_WITH(phone,'686')THEN 'Кирибати'
    WHEN STARTS_WITH(phone,'689')THEN 'Французская полинезия'
    WHEN STARTS_WITH(phone,'691')THEN 'Микронезия'
    WHEN STARTS_WITH(phone,'692')THEN 'Маршалловы о-ва'
    WHEN STARTS_WITH(phone,'850')THEN 'Северная Корея'
    WHEN STARTS_WITH(phone,'852')THEN 'Гонконг'
    WHEN STARTS_WITH(phone,'853')THEN 'Макао'
    WHEN STARTS_WITH(phone,'856')THEN 'Лаос'
    WHEN STARTS_WITH(phone,'960')THEN 'Мальдивские о-ва'
    WHEN STARTS_WITH(phone,'961')THEN 'Ливан'
    WHEN STARTS_WITH(phone,'962')THEN 'Иордания'
    WHEN STARTS_WITH(phone,'964')THEN 'Ирак'
    WHEN STARTS_WITH(phone,'965')THEN 'Кувейт'
    WHEN STARTS_WITH(phone,'972')THEN 'Израиль'
    WHEN STARTS_WITH(phone,'976')THEN 'Монголия'
    WHEN STARTS_WITH(phone,'977')THEN 'Непал'
    WHEN STARTS_WITH(phone,'995')THEN 'Грузия'
    WHEN STARTS_WITH(phone,'996')THEN 'Киргизстан'
    WHEN STARTS_WITH(phone,'1-473')THEN 'Гренада'
    WHEN STARTS_WITH(phone,'1-664')THEN 'Монсеррат'
    WHEN STARTS_WITH(phone,'1-767')THEN 'Доминика'
    WHEN STARTS_WITH(phone,'1-809')THEN 'Доминиканская республика'
    WHEN STARTS_WITH(phone,'1-876')THEN 'Ямайка'
    WHEN STARTS_WITH(phone,'21')THEN 'Алжир'
    WHEN STARTS_WITH(phone,'32')THEN 'Бельгия'
    WHEN STARTS_WITH(phone,'43')THEN 'Австрия'
    WHEN STARTS_WITH(phone,'54')THEN 'Аргентина'
    WHEN STARTS_WITH(phone,'55')THEN 'Бразилия'
    WHEN STARTS_WITH(phone,'56')THEN 'Чили'
    WHEN STARTS_WITH(phone,'57')THEN 'Колумбия'
    WHEN STARTS_WITH(phone,'61')THEN 'Австралия'
    WHEN STARTS_WITH(phone,'86')THEN 'Китай'
    WHEN STARTS_WITH(phone,'93')THEN 'Афганистан'
    WHEN STARTS_WITH(phone,'226')THEN 'Буркина Фасо'
    WHEN STARTS_WITH(phone,'229')THEN 'Бенин'
    WHEN STARTS_WITH(phone,'235')THEN 'Чад'
    WHEN STARTS_WITH(phone,'236')THEN 'ЦАР'
    WHEN STARTS_WITH(phone,'237')THEN 'Камерун'
    WHEN STARTS_WITH(phone,'238')THEN 'Капе Верде'
    WHEN STARTS_WITH(phone,'244')THEN 'Ангола'
    WHEN STARTS_WITH(phone,'247')THEN 'Асеньон'
    WHEN STARTS_WITH(phone,'257')THEN 'Бурунди'
    WHEN STARTS_WITH(phone,'267')THEN 'Ботсвана'
    WHEN STARTS_WITH(phone,'297')THEN 'Аруба'
    WHEN STARTS_WITH(phone,'355')THEN 'Албания'
    WHEN STARTS_WITH(phone,'359')THEN 'Болгария'
    WHEN STARTS_WITH(phone,'374')THEN 'Армения'
    WHEN STARTS_WITH(phone,'375')THEN 'Белоруссия'
    WHEN STARTS_WITH(phone,'376')THEN 'Андорра'
    WHEN STARTS_WITH(phone,'387')THEN 'Босния и Герцеговина'
    WHEN STARTS_WITH(phone,'501')THEN 'Белиз'
    WHEN STARTS_WITH(phone,'591')THEN 'Боливия'
    WHEN STARTS_WITH(phone,'672')THEN 'Австралийские внешние территории'
    WHEN STARTS_WITH(phone,'673')THEN 'Бруней'
    WHEN STARTS_WITH(phone,'684')THEN 'Американское Самоа'
    WHEN STARTS_WITH(phone,'855')THEN 'Камбоджа'
    WHEN STARTS_WITH(phone,'880')THEN 'Бангладеш'
    WHEN STARTS_WITH(phone,'973')THEN 'Бахрейн'
    WHEN STARTS_WITH(phone,'975')THEN 'Бутан'
    WHEN STARTS_WITH(phone,'994')THEN 'Азербайджан'
    WHEN STARTS_WITH(phone,'1-242')THEN 'Багамы'
    WHEN STARTS_WITH(phone,'1-246')THEN 'Барбадос'
    WHEN STARTS_WITH(phone,'1-264')THEN 'Ангуилла'
    WHEN STARTS_WITH(phone,'1-268')THEN 'Антигуа и Барбуда'
    WHEN STARTS_WITH(phone,'1-284')THEN 'Британские Вирджинские о-ва'
    WHEN STARTS_WITH(phone,'1-345')THEN 'Каймановы о-ва'
    WHEN STARTS_WITH(phone,'1-441')THEN 'Бермудские о-ва'
    WHEN STARTS_WITH(phone,'1-670')THEN 'Содружество северных Мариански'
    WHEN STARTS_WITH(phone,'31')THEN 'Нидерланды'
    WHEN STARTS_WITH(phone,'683')THEN 'НИУЭ'
    WHEN STARTS_WITH(phone,'505')THEN 'Никарагуа'
    WHEN STARTS_WITH(phone,'64')THEN 'Новая Зеландия'
    WHEN STARTS_WITH(phone,'227')THEN 'Нигер'
    WHEN STARTS_WITH(phone,'687')THEN 'Новая Каледония'
    WHEN STARTS_WITH(phone,'234')THEN 'Нигерия'
    WHEN STARTS_WITH(phone,'599')THEN 'Нидерландские Антиллы'
    WHEN STARTS_WITH(phone,'1')THEN 'США'
    WHEN STARTS_WITH(phone,'7') AND NOT (STARTS_WITH(phone,'76') OR STARTS_WITH(phone,'77'))THEN 'Россия'
    WHEN STARTS_WITH(phone,'27')THEN 'ЮАР'
    WHEN STARTS_WITH(phone,'34')THEN 'Испания'
    WHEN STARTS_WITH(phone,'40')THEN 'Румыния'
    WHEN STARTS_WITH(phone,'41')THEN 'Швейцария'
    WHEN STARTS_WITH(phone,'44')THEN 'Великобритания'
    WHEN STARTS_WITH(phone,'46')THEN 'Швеция'
    WHEN STARTS_WITH(phone,'47')THEN 'Норвегия'
    WHEN STARTS_WITH(phone,'48')THEN 'Польша'
    WHEN STARTS_WITH(phone,'51')THEN 'Перу'
    WHEN STARTS_WITH(phone,'58')THEN 'Венесуэла'
    WHEN STARTS_WITH(phone,'63')THEN 'Филипины'
    WHEN STARTS_WITH(phone,'65')THEN 'Сингапур'
    WHEN STARTS_WITH(phone,'66')THEN 'Тайланд'
    WHEN STARTS_WITH(phone,'84')THEN 'Вьетнам'
    WHEN STARTS_WITH(phone,'90')THEN 'Турция'
    WHEN STARTS_WITH(phone,'92')THEN 'Пакистан'
    WHEN STARTS_WITH(phone,'94')THEN 'Шри Ланка'
    WHEN STARTS_WITH(phone,'221')THEN 'Сенегал'
    WHEN STARTS_WITH(phone,'228')THEN 'Тоголезе'
    WHEN STARTS_WITH(phone,'232')THEN 'Сьерра Леоне'
    WHEN STARTS_WITH(phone,'239')THEN 'Сент Том и Принцип'
    WHEN STARTS_WITH(phone,'243')THEN 'Заир'
    WHEN STARTS_WITH(phone,'248')THEN 'Сейшельские о-ва'
    WHEN STARTS_WITH(phone,'249')THEN 'Судан'
    WHEN STARTS_WITH(phone,'250')THEN 'Руанда'
    WHEN STARTS_WITH(phone,'252')THEN 'Сомали'
    WHEN STARTS_WITH(phone,'255')THEN 'Танзания'
    WHEN STARTS_WITH(phone,'256')THEN 'Уганда'
    WHEN STARTS_WITH(phone,'259')THEN 'Занзибар'
    WHEN STARTS_WITH(phone,'260')THEN 'Замбия'
    WHEN STARTS_WITH(phone,'262')THEN 'Реюнион'
    WHEN STARTS_WITH(phone,'263')THEN 'Зимбабве'
    WHEN STARTS_WITH(phone,'268')THEN 'Свазиленд'
    WHEN STARTS_WITH(phone,'351')THEN 'Португалия'
    WHEN STARTS_WITH(phone,'378')THEN 'Сан Марино'
    WHEN STARTS_WITH(phone,'380')THEN 'Украина'
    WHEN STARTS_WITH(phone,'381')THEN 'Югославия'
    WHEN STARTS_WITH(phone,'386')THEN 'Словения'
    WHEN STARTS_WITH(phone,'421')THEN 'Словакия'
    WHEN STARTS_WITH(phone,'507')THEN 'Панама'
    WHEN STARTS_WITH(phone,'508')THEN 'Сент Пьер'
    WHEN STARTS_WITH(phone,'595')THEN 'Парагвай'
    WHEN STARTS_WITH(phone,'597')THEN 'Суринам'
    WHEN STARTS_WITH(phone,'598')THEN 'Уругвай'
    WHEN STARTS_WITH(phone,'670')THEN 'Северо-Марианские о-ва'
    WHEN STARTS_WITH(phone,'675')THEN 'Папуа Новая Гвинея'
    WHEN STARTS_WITH(phone,'676')THEN 'Тонго'
    WHEN STARTS_WITH(phone,'677')THEN 'Соломоновы о-ва'
    WHEN STARTS_WITH(phone,'678')THEN 'Вануату'
    WHEN STARTS_WITH(phone,'680')THEN 'Палау'
    WHEN STARTS_WITH(phone,'681')THEN 'Эллис и Футуна острова'
    WHEN STARTS_WITH(phone,'685')THEN 'Западное Самоа'
    WHEN STARTS_WITH(phone,'688')THEN 'Тувалу'
    WHEN STARTS_WITH(phone,'690')THEN 'Токелау'
    WHEN STARTS_WITH(phone,'886')THEN 'Тайвань'
    WHEN STARTS_WITH(phone,'963')THEN 'Сирия'
    WHEN STARTS_WITH(phone,'966')THEN 'Саудовская Аравия'
    WHEN STARTS_WITH(phone,'967')THEN 'Северный Йемен'
    WHEN STARTS_WITH(phone,'968')THEN 'Оман'
    WHEN STARTS_WITH(phone,'969')THEN 'Южный Йемен'
    WHEN STARTS_WITH(phone,'971')THEN 'ОАЭ'
    WHEN STARTS_WITH(phone,'974')THEN 'Катар'
    WHEN STARTS_WITH(phone,'992')THEN 'Таджикистан'
    WHEN STARTS_WITH(phone,'993')THEN 'Туркменистан'
    WHEN STARTS_WITH(phone,'998')THEN 'Узбекистан'
    WHEN STARTS_WITH(phone,'1-340')THEN 'Вирджинские о-ва'
    WHEN STARTS_WITH(phone,'1-649')THEN 'Текс и Каикос Айландс'
    WHEN STARTS_WITH(phone,'1-758')THEN 'Санта Лючия'
    WHEN STARTS_WITH(phone,'1-784')THEN 'Сент Винцент и Гренадины'
    WHEN STARTS_WITH(phone,'1-787')THEN 'Пуэрто Рико'
    WHEN STARTS_WITH(phone,'1-868')THEN 'Тринидад и Тобаго'
    WHEN STARTS_WITH(phone,'1-869')THEN 'Сент-Китс и Невис'
    WHEN STARTS_WITH(phone,'76')THEN 'Казахстан'
    WHEN STARTS_WITH(phone,'77')THEN 'Казахстан'
    ELSE 'other country'
    END AS country_by_phone
FROM a
)
, c AS (
SELECT DISTINCT -- получить данные по покупкам
  DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS purchase_month,
  CAST(user_id AS STRING) AS purchaser,
  purchase_id,
  IF(is_first_purchase = 0, purchase_id, NULL) AS repeated_purchase,
  product_id,
  category_name,
  product_price,
  IF(promo_sum_rub>0,'1','0') as promo_used, --использование промо сверить количество с crm_com
  IF(bonus_company>0,'1','0') as bonus_used, --использование бонусов сверить количество с crm_com
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
)
, d AS ( --соединить покупки с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT b.registered_user) AS registered_user,
  c.purchase_month,
  b.country_by_phone,
  COUNT(DISTINCT c.purchaser) AS purchaser,
  COUNT(DISTINCT c.purchase_id) AS purchases,
  COUNT(DISTINCT c.repeated_purchase) AS repeated_purchase,
  -- COUNT(c.product_id) AS products,
  SUM(c.product_price) AS revenue,
  SUM(CAST(c.promo_used AS NUMERIC)) AS promo_used, 
  SUM(CAST(c.bonus_used AS NUMERIC)) AS bonus_used,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5267172'
GROUP BY month_of_registration,purchase_month,country_by_phone
ORDER BY registered_user
)
, e AS ( --соединить покупки по годам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT b.registered_user) AS registered_user,
  FORMAT_DATE('%Y',c.purchase_month) AS purchase_year,
  b.country_by_phone,
  COUNT(DISTINCT c.purchaser) AS purchaser,
  COUNT(DISTINCT c.purchase_id) AS purchases,
  COUNT(DISTINCT c.repeated_purchase) AS repeated_purchase,
  -- COUNT(c.product_id) AS products,
  SUM(c.product_price) AS revenue,
  SUM(CAST(c.promo_used AS NUMERIC)) AS promo_used, 
  SUM(CAST(c.bonus_used AS NUMERIC)) AS bonus_used,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5267172'
GROUP BY month_of_registration,purchase_year,country_by_phone
ORDER BY registered_user
)
, f AS (
SELECT
  b.month_of_registration,
  -- COUNT(DISTINCT b.registered_user) AS registered_user,
  c.purchase_month,
  b.country_by_phone,
  STRING_AGG(c.category_name) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone) AS arr_category_name,
  -- COUNT(DISTINCT c.purchase_id) AS purchases,
  -- COUNT(DISTINCT c.repeated_purchase) AS repeated_purchase,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5267172'
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
SELECT
  *
FROM f
PIVOT(STRING_AGG(arr_category_name) FOR purchase_month IN ('2024-01-01','2024-02-01')) 