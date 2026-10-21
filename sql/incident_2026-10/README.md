# תחקיר אירוע נעילה — אוקטובר 2026

## מה קרה
- הלקוח עשה KILL לתהליך של המשתמש **BIX** בשרת הלייב. הוא חסם עבודה על **טבלאות ההוצאות**, ובחלק מהמקרים נדרש אתחול שרת.
- זה קרה 4 פעמים בשבוע וחצי. טקסט השאילתה לא נשמר.
- BIX הוא משתמש הרענון של Power BI.
- **אומת:** ב-Refresh history יש רענון שנכשל בזמן ה-KILL עם השגיאה *"A severe error occurred on the current command"*, מקור `127.0.0.1;MT` (ה-gateway יושב על שרת הלייב). כלומר ה-session שנהרג היה הרענון של Power BI ולא חלון SSMS.
- מאז האירוע הרענון רץ רק בבוקר ובערב (מעקף זמני).

## מה ידוע מהצד שלנו
- טבלאות ההוצאות (`HOTSAOT_COTROT`, `HOTSAOT_SHOROT`, `HOTSAOT_SHROTIM_New`) נקראות על ידי 4 טבלאות במודל: `factPurchaseExpenses`, `factGain`, `dimPurchaseOrderNumber`, `dimSubPurchaseOrderNumber`. כולן שונו בין 30/08 ל-09/09.
- כל השאילתות במודל הן קריאה בלבד: אין בהן INSERT, UPDATE, DELETE או BEGIN TRAN.
- ב-STG ה-RCSI כבוי. כלומר קוראים וכותבים יכולים לחסום זה את זה.
- **STG ≠ לייב.** ב-STG אין כתיבה מקבילה, ולכן המספרים כאן טובים להשוואה בין גרסאות ולא מעידים על הזמנים בלייב.

## סדר עבודה

כל הסקריפטים שכותבים משהו בודקים קודם ש-`@@SERVERNAME = ADM-SQL2020\ADMNEW` (STG), ואם לא, עוצרים.

| # | קובץ | איפה | מה עושים |
|---|---|---|---|
| 1 | `01_env_check.sql` | STG, ואותו קובץ אצל שלמה בלייב | הגדרות, נפחי טבלאות, jobs מתוזמנים |
| 2 | `02_measure_all.sql` | STG | מדידה של 5 שאילתות, לפני ועכשיו |
| 3 | `03a` + `03b_reader_read_committed` | STG, 2 חלונות | כותב מחזיק נעילה, השאילתה שלנו מגיעה |
| 4 | `03b_reader_read_committed` + `03d` | STG, 2 חלונות | השאילתה שלנו רצה, כותב מגיע |
| 5 | `03b_reader_read_uncommitted` + `03d` | STG, 2 חלונות | האם התיקון מבטל את החסימה |
| 6 | Refresh history ב-Power BI Service | דפדפן | האם 4 האירועים חופפים לשעות רענון |

### הגדרות SSMS (פעם אחת לכל חלון)
- **Query → Query Options → Results → Grid → Discard results after execution** כדי ש-SSMS לא יצייר עשרות אלפי שורות.
- אם סקריפט עצר על בדיקת השרת, צריך להתחבר מחדש לחלון.

### שלב 2: מדידה
מריצים את הקובץ כולו **פעמיים** ולוקחים את המספרים מהריצה השנייה, כי הראשונה מחממת את ה-cache. בלשונית Messages כל בלוק מתחיל ב-`===== טבלה | גרסה =====`.

### שלבים 3–5: סימולציית נעילה
למשתמש BIX אין `VIEW SERVER STATE`, ולכן אי אפשר לראות מכאן מי חוסם את מי. במקום זה מודדים זמנים.
- **תרחיש 1:** מריצים את `03a` (מחזיק נעילה 60 שניות ועושה ROLLBACK לבד). תוך 60 שניות מריצים את `03b_reader_read_committed` בחלון שני. ב-Messages משווים את `start` ל-`Reader end`: אם הריצה ארוכה ב-60 שניות מהרגיל, השאילתה שלנו נחסמה.
- **תרחיש 2:** מריצים את `03b_reader_read_committed`. כמה שניות אחריו מריצים את `03d`. העמודה `writer_wait_ms` אומרת כמה זמן הכותב חיכה לשאילתה שלנו.
- **שלב 5:** חוזרים על תרחיש 2 עם `03b_reader_read_uncommitted`.

ה-UPDATE בסקריפטים מעדכן שורה אחת לערך שכבר יש בה ומבצע ROLLBACK, כך שבסוף שום נתון לא משתנה. ב-STG אין triggers על טבלאות ההוצאות (נבדק ב-12/10).

## תוצאות למילוי

### מדידה (שלב 2)
| שאילתה | גרסה | elapsed (s) | CPU (s) | reads – הטבלה הכבדה | Worktable reads |
|---|---|---|---|---|---|
| factPurchaseExpenses | before | 238 | 345 | tblOrderPriceS 2,460,941 · HOTSAOT_SHOROT 198,073 (24,897 scans) | 89,972,947 |
| factPurchaseExpenses | current | 275 | 538 | tblOrderPriceS 3,749,197 · HOTSAOT_SHOROT 396,192 (49,794 scans) | 92,403,035 |
| factGain | before | 45 | 83 | TEODOT_MSHLOCH 204,888 · PurchaseOrderLines 910 | 7,724,767 |
| factGain | current | **270** | **343** | PurchaseOrderLines **2,934,470** · TEODOT_MSHLOCH 442,513 | 14,821,585 |
| dimPurchaseOrderNumber | before | 3 | 17 | HOTSAOT_SHOROT 287,545 | 2,680,589 |
| dimPurchaseOrderNumber | current | 8 | 28 | HOTSAOT_SHOROT 288,321 | 2,738,142 |
| dimSubPurchaseOrderNumber | before | 1 | 3.5 | HOTSAOT_SHOROT 286,769 | 156,210 |
| dimSubPurchaseOrderNumber | current | 1.9 | 4 | HOTSAOT_SHOROT 286,769 | 156,210 |
| factOrders | before | 19 | 103 | tblOrderPriceS 1,260,519 | 808,971 |
| factOrders | current | 26 | 132 | tblOrderPriceS 1,256,849 | 809,559 |

ריצה אחת ב-STG, 12/10, **לפני התיקון**. מאז `current` ב-`queries/`, ב-`02` וב-`03b` הוא הגרסה המתוקנת (הם נוצרים מה-TMDL), כך שהרצה חוזרת של `02` תמדוד "לפני אוגוסט" מול "אחרי התיקון". השוואה ראשונה ל-factOrders current מ-07/10: 34.7s / 112.9s CPU.
factGain current מחזירה 105,427 שורות לעומת 78,692 ב-before.

### סימולציה (שלבים 3–5)
| תרחיש | isolation | הקורא חיכה (s) | הכותב חיכה (ms) | הערות |
|---|---|---|---|---|
| 1 – כותב קודם | READ COMMITTED | | — | |
| 2 – קורא קודם | READ COMMITTED | — | | |
| 2 – קורא קודם | READ UNCOMMITTED | — | | |

### לייב (משלמה)
| נתון | STG | לייב |
|---|---|---|
| RCSI | כבוי | |
| MAXDOP | | |
| שורות tblOrderPriceS | 23,037,143 | |
| שורות HZMNOT | 144,608 | |
| שורות HOTSAOT_SHOROT | 24,604 | |
| שורות HOTSAOT_COTROT | 12,912 | |
| jobs בשעות הרענון | | |

## תיקונים

### factGain + factPurchaseExpenses: חישוב פעם אחת לטבלאות זמניות (12/10, ב-TMDL, עוד לא פורסם)

| שאילתה | לפני (STG) | אחרי (STG) | טבלאות זמניות |
|---|---|---|---|
| factGain | 255–270s | ~100s | `#exchange_movements`, `#purchase_orders`, `#P_costs` |
| factPurchaseExpenses | 275–295s | ~54s | `#base`, `#final2`, `#OrderPrices`, `#CIF_Qty`, `#base_expenses` |

בשתיהן אף CTE לא השתנה. ההפניות לטבלה הזמנית מוחלפות רק אחרי `FROM`/`JOIN` (`CIF_Qty` הוא גם שם של עמודה).

**factGain**
- **הבעיה:** SQL Server מריץ CTE מחדש בכל פעם שמפנים אליו. `purchase_orders`, שכולל את ה-join הכבד ל-`PurchaseOrderLines`, נקרא 3 פעמים, ו-`P_costs` נקרא פעמיים בשאילתה הסופית.
- **התיקון:** השאילתה מחולקת ל-4 צעדים: `#exchange_movements` ← `#purchase_orders` ← `#P_costs` ← תוצאה סופית. שום לוגיקה לא השתנתה.
- **תוצאה ב-STG:** מ-255–270 שניות ל-**~100 שניות**.
- **QA:** אותן 105,427 שורות. ההבדלים הם רעש float, ו-411 שורות Warehouse שנובעות מבאג **קיים** ב-`inv` (ראו למטה). אותו באג גורם גם לגרסה הישנה להחזיר תוצאה אחרת בכל ריצה.

**factPurchaseExpenses**
- **הבעיה:** `CIF_Qty` (עם כל `sales`) נקרא 5 פעמים, `base_expenses` (עם `totals_raw`, טבלאות ההוצאות ו-`tblOrderPriceS`) פעמיים, ו-`base` פעמיים בתוך השרשרת.
- **QA:** אותן 27,646 שורות (בלי `RowInsertDatetime`, שהוא `GETDATE()`). 29–31 שורות שונות, כולן מבאג **קיים** ב-`final` (ראו למטה). **הוכחה:** שתי ריצות של הגרסה המתוקנת עצמה נבדלות ב-5 שורות.

**קבצים:** `fix/build_materialized.py` בונה את השאילתות, `fix/apply_to_tmdl.py factGain factPurchaseExpenses` כותב אותן ל-TMDL, ו-`fix/*__fixed.sql` הן הגרסאות הקריאות.

### באג קיים ב-`inv` (factGain): לא תוקן, דורש החלטה
`month_rn` בוחר את "היום האחרון בחודש" לפי `[Date]` בלבד. ב-88 צירופים של פריט וחודש יש כמה שורות באותו יום (ספקים או מחסנים שונים), ו-14 מהם עם **מחירים שונים**. `ROW_NUMBER` בוחר ביניהם שרירותית, ולכן `FOT_Purchase` של שורות Warehouse יכול להשתנות בין רענונים. צריך להחליט עסקית איזה מחיר גובר (למשל המקסימלי, ממוצע, או ספק מסוים).

### באג קיים ב-`final` (factPurchaseExpenses): לא תוקן, דורש החלטה
`ROW_NUMBER() OVER (PARTITION BY ItemKey, PurchaseOrderID ORDER BY Purchase_Date DESC)` בוחר את הרכישה התואמת להזמנת exchange, אבל `Purchase_Date` הוא **חודש** (`YYYY-MM`). ב-STG יש 86 הזמנות exchange עם שוויון, ו-63 מהן בין רכישות **במחיר שונה**. הבחירה שרירותית, ולכן `UnitNetPriceUSD`, `OrderQuantity`, `LineTotalNetUSD` ו-`NetTonnage` יכולים להשתנות בין רענונים. אותו דפוס ייתכן גם ב-`exchange_priced` של factGain. צריך כלל שובר שוויון (למשל `PurchaseOrderID DESC` או תאריך מלא).

## לבקש משלמה
1. להריץ את `01_env_check.sql` בלייב ולשלוח את התוצאה.
2. **בפעם הבאה, לפני KILL:** להריץ את `03c_monitor_blocking.sql` (דורש `VIEW SERVER STATE`, שיש לו ואין לנו) ולשלוח צילום מסך. העמודה `program_name` תגיד אם זה Power BI (`Mashup Engine`) או SSMS, ו-`open_transaction_count` תגיד אם הייתה טרנזקציה פתוחה.
3. האם עוד מישהו מתחבר ללייב עם BIX, חוץ מה-gateway?
4. האם `NOLOCK` / `READ UNCOMMITTED` בשאילתות הקריאה שלנו מקובל אצלם?

## קבצים
- `extract_queries.py` מחלץ את השאילתות מה-TMDL אל `queries/`. "current" זה ה-working tree, ו-"before" זה commit `5b60782` (לפני השינויים של 30/08). אחרי כל שינוי במודל מריצים שוב: `python sql/incident_2026-10/extract_queries.py`.
- `queries/*.sql`, `02_measure_all.sql` ו-`03b_reader_*.sql` נוצרים אוטומטית, ואין צורך ב-SQLCMD Mode. לא עורכים אותם ידנית.
