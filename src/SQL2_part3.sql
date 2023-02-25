-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
-- Ник пира 1, ник пира 2, количество переданных пир поинтов.
-- Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.
DROP FUNCTION IF EXISTS human_readable_transferredpoints();

CREATE OR REPLACE FUNCTION human_readable_transferredpoints()
RETURNS TABLE (Peer1 varchar, Peer2 varchar, PoinstAmount bigint) AS $$
    BEGIN
        RETURN QUERY (SELECT t1.checkingpeer, t1.checkedpeer, (t1.pointsamount -
                                                               t2.pointsamount) AS PoinstAmount
                      FROM transferredpoints AS t1
                      JOIN transferredpoints AS t2 ON t1.checkingpeer = t2.checkedpeer AND
                                                      t1.checkedpeer = t2.checkingpeer AND t1.id < t2.id);
    END;
$$ LANGUAGE plpgsql;

-- SELECT *
-- FROM human_readable_transferredpoints();


-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания,
-- кол-во полученного XP
-- В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks).
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.
DROP FUNCTION IF EXISTS get_amount_xp();

CREATE OR REPLACE FUNCTION get_amount_xp()
RETURNS TABLE (Peer varchar, Task text, XP bigint) AS $$
    BEGIN
        RETURN QUERY (SELECT checks.peer AS Peer, checks.task AS Task, xp.xpamount AS XP
                      FROM xp
                      JOIN checks ON xp."Check" = checks.id);
    END;
$$ LANGUAGE plpgsql;

-- SELECT *
-- FROM get_amount_xp();


-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022.
-- Функция возвращает только список пиров.
DROP FUNCTION IF EXISTS peers_not_come_out(day_date date);

CREATE OR REPLACE FUNCTION peers_not_come_out(IN day_date date)
RETURNS SETOF varchar AS $$
    BEGIN
        RETURN QUERY (SELECT peer
                      FROM timetracking
                      WHERE timetracking.date = day_date
                      GROUP BY peer, date
                      HAVING COUNT(state) < 3);
    END;
$$ LANGUAGE plpgsql;

-- SELECT *
-- FROM peers_not_come_out('2022-05-02');


-- 4) Найти процент успешных и неуспешных проверок за всё время
-- Формат вывода: процент успешных, процент неуспешных
-- можно сделать через курсор
DROP PROCEDURE IF EXISTS ratio_success_failure_checks(SuccessfulChecks real, UnsuccessfulChecks real);

CREATE OR REPLACE PROCEDURE ratio_success_failure_checks(OUT SuccessfulChecks real,
                                                         OUT UnsuccessfulChecks real) AS $$
    BEGIN
        CREATE VIEW full_table AS (
            SELECT p2p.state AS p2p_state, verter.state AS verter_state
                FROM checks
                JOIN p2p ON checks.id = p2p."Check"
                LEFT JOIN verter ON checks.id = verter."Check"
                WHERE p2p.state IN ('Success', 'Failure') AND
                      (verter.state IN ('Success', 'Failure') OR verter.state IS NULL));

        SELECT round((((SELECT COUNT(*) FROM full_table
                WHERE p2p_state = 'Success' AND (verter_state = 'Success' OR verter_state IS NULL)) * 100) /
                (SELECT COUNT(*) FROM full_table)::real)) INTO SuccessfulChecks;

        SELECT round((((SELECT COUNT(*) FROM full_table
                WHERE p2p_state = 'Failure' OR verter_state = 'Failure') * 100) /
                (SELECT COUNT(*) FROM full_table)::real)) INTO UnsuccessfulChecks;

        DROP VIEW full_table CASCADE;
    END;
$$ LANGUAGE plpgsql;

-- CALL ratio_success_failure_checks(NULL, NULL);


-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов
DROP PROCEDURE IF EXISTS changes_peer_points(ref refcursor);

CREATE OR REPLACE PROCEDURE changes_peer_points(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH sum_checkingpeer AS (SELECT checkingpeer, ABS(SUM(pointsamount)) AS sum_points
                FROM transferredpoints
                GROUP BY checkingpeer),
                 sum_checkedpeer AS (
                SELECT checkedpeer, ABS(SUM(pointsamount)) AS sum_points
                FROM transferredpoints
                GROUP BY checkedpeer)
            SELECT checkingpeer AS Peer, ((COALESCE(sum_checkingpeer.sum_points, 0)) -
                              (COALESCE(sum_checkedpeer.sum_points, 0))) AS PointsChange
            FROM sum_checkingpeer
            JOIN sum_checkedpeer ON sum_checkingpeer.checkingpeer = sum_checkedpeer.checkedpeer
            ORDER BY PointsChange DESC;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL changes_peer_points('ref');
-- FETCH ALL IN "ref";
-- END;


-- 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов
DROP PROCEDURE IF EXISTS changes_peer_points_2 CASCADE;

CREATE OR REPLACE PROCEDURE changes_peer_points_2(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH p1 AS (SELECT Peer1 AS peer, SUM(PoinstAmount) AS PointsChange
            FROM human_readable_transferredpoints()
            GROUP BY Peer1),
                 p2 AS (SELECT Peer2 AS peer, SUM(PoinstAmount) AS PointsChange
            FROM human_readable_transferredpoints()
            GROUP BY Peer2)
        SELECT COALESCE(p1.peer, p2.peer) AS peer, (COALESCE(p1.PointsChange, 0) - COALESCE(p2.PointsChange, 0)) AS pointschange
        FROM p1
        FULL JOIN p2 ON p1.peer = p2.peer
        ORDER BY PointsChange DESC;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL changes_peer_points_2('ref');
-- FETCH ALL IN "ref";
-- END;


-- 7) Определить самое часто проверяемое задание за каждый день
-- При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все.
-- Формат вывода: день, название задания
DROP PROCEDURE IF EXISTS frequently_checked_task CASCADE;

CREATE OR REPLACE PROCEDURE frequently_checked_task(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH t1 AS (SELECT task, date, COUNT(*) AS counts
                        FROM checks
                        GROUP BY task, date),
                 t2 AS (SELECT t1.task, t1.date, rank() OVER (PARTITION BY t1.date ORDER BY t1.counts) AS rank
                        FROM t1)
            SELECT t2.date, t2.task
            FROM t2
            WHERE rank = 1;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL frequently_checked_task('ref');
-- FETCH ALL IN "ref";
-- END;


-- 8) Определить длительность последней P2P проверки
-- Под длительностью подразумевается разница между временем, указанным в записи со статусом "начало",
-- и временем, указанным в записи со статусом "успех" или "неуспех".
-- Формат вывода: длительность проверки
DROP PROCEDURE IF EXISTS duration_last_p2p CASCADE;

CREATE OR REPLACE PROCEDURE duration_last_p2p(OUT duration time) AS $$
    BEGIN
        WITH all_p2p_time AS (
            SELECT "Check", (MAX(p2p.time) - MIN(p2p.time)) AS duration
            FROM p2p
            GROUP BY "Check"
            HAVING "Check" = MAX("Check") AND COUNT(*) = 2)
        SELECT all_p2p_time.duration INTO duration
        FROM all_p2p_time
        ORDER BY "Check" DESC
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

-- CALL duration_last_p2p(NULL);


-- 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP".
-- Результат вывести отсортированным по дате завершения.
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)
DROP PROCEDURE IF EXISTS peers_complite_task_block CASCADE;

CREATE OR REPLACE PROCEDURE peers_complite_task_block(IN block varchar, IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH tasks_block AS (SELECT *
                                 FROM tasks
                                 WHERE title SIMILAR TO concat(block, '[0-9]%')),
                 last_task AS (SELECT MAX(title) AS title FROM tasks_block),
                 date_of_successful_check AS (SELECT checks.peer, checks.task, checks.date
                                              FROM checks
                                              JOIN p2p ON checks.id = p2p."Check"
                                              WHERE p2p.state = 'Success'
                                              GROUP BY checks.id)
            SELECT date_of_successful_check.peer AS Peer, date_of_successful_check.date
            FROM date_of_successful_check
            JOIN last_task ON date_of_successful_check.task = last_task.title;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL peers_complite_task_block('C', 'ref');
-- FETCH ALL IN "ref";
-- END;


-- 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей.
-- Формат вывода: ник пира, ник найденного проверяющего
DROP PROCEDURE IF EXISTS find_most_recommend_peer CASCADE;

CREATE OR REPLACE PROCEDURE find_most_recommend_peer(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH find_friends AS (SELECT nickname,
                                 (CASE WHEN nickname = friends.peer1 THEN peer2 ELSE peer1 END) AS frineds
                                  FROM peers
                                  JOIN friends ON peers.nickname = friends.peer1 OR peers.nickname = friends.peer2),
                 find_reccommend AS (SELECT nickname, COUNT(recommendedpeer) AS count_rec, recommendedpeer
                                     FROM find_friends
                                     JOIN recommendations ON find_friends.frineds = recommendations.peer
                                     WHERE find_friends.nickname != recommendations.recommendedpeer
                                     GROUP BY nickname, recommendedpeer),
                 find_max AS (SELECT nickname, MAX(count_rec) AS max_count
                              FROM find_reccommend
                              GROUP BY nickname)
            SELECT find_reccommend.nickname AS peer, recommendedpeer
            FROM find_reccommend
            JOIN find_max ON find_reccommend.nickname = find_max.nickname AND
                             find_reccommend.count_rec = find_max.max_count;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL find_most_recommend_peer('ref');
-- FETCH ALL IN "ref";
-- END;


-- 11) Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
-- Пир считается приступившим к блоку, если он проходил хоть одну проверку любого задания
-- из этого блока (по таблице Checks)
--
-- Параметры процедуры: название блока 1, например SQL, название блока 2, например A.
-- Формат вывода: процент приступивших только к первому блоку, процент приступивших только ко второму блоку,
-- процент приступивших к обоим, процент не приступивших ни к одному
DROP PROCEDURE IF EXISTS successful_checks_blocks CASCADE;

CREATE OR REPLACE PROCEDURE successful_checks_blocks(IN block1 text, IN block2 text, OUT StartedBlock1 bigint,
        OUT StartedBlock2 bigint, OUT StartedBothBlock bigint, OUT DidntStartAnyBlock bigint) AS $$
    DECLARE count_peers bigint := (SELECT COUNT(peers.nickname) FROM peers);
    BEGIN
        CREATE TABLE temp (
            b1 text,
            b2 text,
            c_peers bigint
        );
        INSERT INTO temp VALUES (block1, block2, count_peers);

        CREATE VIEW new_view AS (
            WITH start_block1 AS (SELECT DISTINCT peer
                                  FROM checks
                                  WHERE checks.task SIMILAR TO concat((SELECT b1 FROM temp), '[0-9]%')),
                 start_block2 AS (SELECT DISTINCT peer
                                  FROM checks
                                  WHERE checks.task SIMILAR TO concat((SELECT b2 FROM temp), '[0-9]%')),
                 start_only_block1 AS (SELECT peer FROM start_block1
                                      EXCEPT
                                      SELECT peer FROM start_block2),
                 start_only_block2 AS (SELECT peer FROM start_block2
                                      EXCEPT
                                      SELECT peer FROM start_block1),
                 start_both_block AS (SELECT peer FROM start_block1
                                      INTERSECT
                                      SELECT peer FROM start_block2),
                 didnt_start AS (SELECT COUNT(nickname) AS peer_count
                                 FROM peers
                                 LEFT JOIN checks ON peers.nickname = checks.peer
                                 WHERE peer IS NULL)
            SELECT (((SELECT COUNT(*) FROM start_only_block1) * 100) / (SELECT c_peers FROM temp)) AS s1,
                   (((SELECT COUNT(*) FROM start_only_block2) * 100) / (SELECT c_peers FROM temp)) AS s2,
                   (((SELECT COUNT(*) FROM start_both_block) * 100) / (SELECT c_peers FROM temp)) AS s3,
                   (((SELECT peer_count FROM didnt_start) * 100) / (SELECT c_peers FROM temp)) AS s4);
        StartedBlock1 = (SELECT s1 FROM new_view);
        StartedBlock2 = (SELECT s2 FROM new_view);
        StartedBothBlock = (SELECT s3 FROM new_view);
        DidntStartAnyBlock = (SELECT s4 FROM new_view);
        DROP VIEW new_view CASCADE;
        DROP TABLE temp CASCADE;
    END;
$$ LANGUAGE plpgsql;

-- CALL successful_checks_blocks('C', 'DO', NULL, NULL, NULL, NULL);


-- 12) Определить N пиров с наибольшим числом друзей
-- Параметры процедуры: количество пиров N.
-- Результат вывести отсортированным по кол-ву друзей.
-- Формат вывода: ник пира, количество друзей
DROP PROCEDURE IF EXISTS pr_count_friends CASCADE;

CREATE OR REPLACE PROCEDURE pr_count_friends(IN limits int, IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH find_friends AS (SELECT nickname,
                               (CASE WHEN nickname = friends.peer1 THEN peer2 ELSE peer1 END) AS frineds
                               FROM peers
                               JOIN friends ON peers.nickname = friends.peer1 OR peers.nickname = friends.peer2)
            SELECT nickname, COUNT(frineds) AS FriendsCount
            FROM find_friends
            GROUP BY nickname
            ORDER BY FriendsCount DESC
            LIMIT limits;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL pr_count_friends(3,'ref');
-- FETCH ALL IN "ref";
-- END;


-- 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения.
-- Формат вывода: процент успехов в день рождения, процент неуспехов в день рождения
DROP PROCEDURE IF EXISTS successful_checks_birthday CASCADE;

CREATE OR REPLACE PROCEDURE successful_checks_birthday(OUT SuccessfulChecks int, OUT UnsuccessfulChecks int) AS $$
    BEGIN
        CREATE VIEW new_view AS (
        WITH get_month_date_from_peers AS (SELECT nickname, extract(month FROM birthday) AS p_month,
                                          extract(day FROM birthday) AS p_day
                                          FROM peers),
             get_month_date_from_checks AS (SELECT checks.id, peer, extract(month FROM date) AS c_month,
                                            extract(day FROM date) AS c_day,
                                            p2p.state AS p_state,
                                            verter.state AS v_state
                                            FROM checks
                                            JOIN p2p ON checks.id = p2p."Check"
                                            LEFT JOIN verter ON checks.id = verter."Check"
                                            WHERE p2p.state IN ('Success', 'Failure') AND
                                            (verter.state IN ('Success', 'Failure') OR verter.state IS NULL)),
        join_tables AS (SELECT *
                        FROM get_month_date_from_peers AS t1
                        JOIN get_month_date_from_checks AS t2 ON t1.p_day = t2.c_day AND t1.p_month = t2.c_month),
        count_success AS (SELECT COUNT(*) AS s_count
                          FROM join_tables
                          WHERE p_state = 'Success' AND (v_state = 'Success' OR v_state IS NULL)),
        count_failure AS (SELECT COUNT(*) AS f_count
                          FROM join_tables
                          WHERE p_state = 'Failure' AND (v_state = 'Failure' OR v_state IS NULL)),
        count_peers AS (SELECT COUNT(peers.nickname) AS all_count FROM peers)
        SELECT (((SELECT s_count FROM count_success) * 100) / (SELECT all_count FROM count_peers)) AS s1,
               (((SELECT f_count FROM count_failure) * 100) / (SELECT all_count FROM count_peers)) AS s2);

        SuccessfulChecks = (SELECT s1 FROM new_view);
        UnsuccessfulChecks = (SELECT s2 FROM new_view);
        DROP VIEW new_view CASCADE;
    END;
$$ LANGUAGE plpgsql;

-- CALL successful_checks_birthday(NULL, NULL);


-- 14) Определить кол-во XP, полученное в сумме каждым пиром
-- Если одна задача выполнена несколько раз, полученное за нее кол-во XP равно максимальному за эту задачу.
-- Результат вывести отсортированным по кол-ву XP.
-- Формат вывода: ник пира, количество XP
DROP PROCEDURE IF EXISTS peer_xp_sum CASCADE;

CREATE OR REPLACE PROCEDURE peer_xp_sum(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH max_xp AS (SELECT checks.peer, MAX(table_xp.xpamount) AS max_xp
                            FROM checks
                            JOIN xp AS table_xp ON checks.id = table_xp."Check"
                            GROUP BY checks.peer, task)
            SELECT max_xp.peer AS Peer, SUM(max_xp) AS XP
            FROM max_xp
            GROUP BY max_xp.peer
            ORDER BY XP;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL peer_xp_sum('ref');
-- FETCH ALL IN "ref";
-- END;


-- 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3.
-- Формат вывода: список пиров
DROP PROCEDURE IF EXISTS successful_tasks_1_2_but_not_3 CASCADE;

CREATE OR REPLACE PROCEDURE successful_tasks_1_2_but_not_3(task1 varchar, task2 varchar,
                            task3 varchar, ref refcursor) AS $$
    BEGIN
       OPEN ref FOR
            WITH success_task1 AS (SELECT peer FROM get_amount_xp() AS t
                                   WHERE task1 IN (SELECT task FROM get_amount_xp())),
                 success_task2 AS (SELECT peer FROM get_amount_xp() AS t
                                   WHERE task2 IN (SELECT task FROM get_amount_xp())),
                 failure_task3 AS (SELECT peer FROM get_amount_xp() AS t
                                   WHERE task3 NOT IN (SELECT task FROM get_amount_xp()))
            SELECT *
            FROM ((SELECT * FROM success_task1)
                   INTERSECT
                  (SELECT * FROM success_task2)
                   INTERSECT
                  (SELECT * FROM failure_task3)) AS new_table;

    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL successful_tasks_1_2_but_not_3('C2_SimpleBashUtils', 'DO2_Linux Network', 'DO1_Linux', 'ref');
-- FETCH ALL IN "ref";
-- END;


-- 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей.
-- Формат вывода: название задачи, количество предшествующих
DROP PROCEDURE IF EXISTS count_parent_tasks CASCADE;

CREATE OR REPLACE PROCEDURE count_parent_tasks(IN ref refcursor) AS $$
    BEGIN
       OPEN ref FOR
            WITH RECURSIVE r AS (SELECT (CASE WHEN tasks.parenttask IS NULL THEN 0 ELSE 1 END) AS counter,
                                        tasks.title, tasks.parenttask AS current_task, tasks.parenttask
                                 FROM tasks
                                 UNION ALL
                SELECT (CASE WHEN child.parenttask IS NOT NULL THEN counter + 1 ELSE counter END) AS counter,
                        child.title AS title, child.parenttask AS current_task, parrent.title AS parrenttask
                        FROM tasks AS child
                        CROSS JOIN r AS parrent
                        WHERE parrent.title LIKE child.parenttask)
            SELECT title AS Task, MAX(counter) AS PrevCount
            FROM r
            GROUP BY title
            ORDER BY task;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL count_parent_tasks('ref');
-- FETCH ALL IN "ref";
-- END;


-- 17) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы
-- N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N.
-- Временем проверки считать время начала P2P этапа.
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных.
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального.
-- Формат вывода: список дней
DROP PROCEDURE IF EXISTS lucky_day CASCADE;

CREATE OR REPLACE PROCEDURE lucky_day(IN N int, IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            WITH t AS (SELECT *
                       FROM checks
                       JOIN p2p ON checks.id = p2p."Check"
                       LEFT JOIN verter ON checks.id = verter."Check"
                       JOIN tasks ON checks.task = tasks.title
                       JOIN xp ON checks.id = xp."Check"
                       WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL))
        SELECT date
        FROM t
        WHERE t.xpamount >= t.maxxp * 0.8
        GROUP BY date
        HAVING COUNT(date) >= N;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL lucky_day(2,'ref');
-- FETCH ALL IN "ref";
-- END;


-- 18) Определить пира с наибольшим числом выполненных заданий
-- Формат вывода: ник пира, число выполненных заданий
DROP PROCEDURE IF EXISTS max_done_task CASCADE;

CREATE OR REPLACE PROCEDURE max_done_task(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            SELECT peer, COUNT(*) AS XP
            FROM xp
            JOIN checks ON xp."Check" = checks.id
            GROUP BY peer
            ORDER BY XP DESC
            LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL max_done_task('ref');
-- FETCH ALL IN "ref";
-- END;


-- 19) Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP
DROP PROCEDURE IF EXISTS max_peer_xp CASCADE;

CREATE OR REPLACE PROCEDURE max_peer_xp(IN ref refcursor) AS $$
    BEGIN
       OPEN ref FOR
            SELECT peer, SUM(xpamount) AS XP
            FROM xp
            JOIN checks ON xp."Check" = checks.id
            GROUP BY peer
            ORDER BY XP DESC
            LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL max_peer_xp('ref');
-- FETCH ALL IN "ref";
-- END;


-- 20) Определить пира, который провел сегодня в кампусе больше всего времени
-- Формат вывода: ник пира
DROP PROCEDURE IF EXISTS the_longest_interval CASCADE;

CREATE OR REPLACE PROCEDURE the_longest_interval(OUT nick varchar) AS $$
    BEGIN
        WITH time_in AS (SELECT peer, SUM(time) AS time_in_campus
                         FROM timetracking
                         WHERE date = current_date AND state = 1
                         GROUP BY peer),
             time_out AS (SELECT peer, SUM(time) AS time_out_campus
                         FROM timetracking
                         WHERE date = current_date AND state = 2
                         GROUP BY peer),
             diff_time AS (SELECT time_in.peer, (time_out_campus - time_in_campus) AS full_time
                           FROM time_in
                           JOIN time_out ON time_in.peer = time_out.peer)
        SELECT peer INTO nick
        FROM diff_time
        ORDER BY full_time DESC
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

-- CALL the_longest_interval(NULL);


-- 21) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N.
-- Формат вывода: список пиров
DROP PROCEDURE IF EXISTS peer_came_early CASCADE;

CREATE OR REPLACE PROCEDURE peer_came_early(IN checktime time, IN N int, IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            SELECT peer
            FROM (SELECT peer, MIN(time) AS min_time, date
                  FROM timetracking
                  WHERE state = 1
                  GROUP BY date, peer) AS t
            WHERE min_time < checktime
            GROUP BY peer
            HAVING COUNT(peer) >= N;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL peer_came_early('19:00:00', 1, 'ref');
-- FETCH ALL IN "ref";
-- END;


-- 22) Определить пиров, выходивших за последние N дней из кампуса больше M раз
-- Параметры процедуры: количество дней N, количество раз M.
-- Формат вывода: список пиров
DROP PROCEDURE IF EXISTS count_out_of_campus CASCADE;

CREATE OR REPLACE PROCEDURE count_out_of_campus(IN N int, IN M int, IN ref refcursor) AS $$
    BEGIN
       OPEN ref FOR
            SELECT peer
            FROM (SELECT peer, date, (COUNT(*) - 1) AS counts
                  FROM timetracking
                  WHERE state = 2 AND date > (current_date - N)
                  GROUP BY peer, date) AS t
            GROUP BY peer
            HAVING SUM(counts) > M;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL count_out_of_campus(1000, 0, 'ref');
-- FETCH ALL IN "ref";
-- END;


-- 23) Определить пира, который пришел сегодня последним
-- Формат вывода: ник пира
DROP PROCEDURE IF EXISTS peer_came_last CASCADE;

CREATE OR REPLACE PROCEDURE peer_came_last(OUT nick varchar) AS $$
    BEGIN
        WITH t AS (SELECT peer, MIN(time) AS first_input
                   FROM timetracking
                   WHERE state = 1 AND date = current_date
                   GROUP BY peer)
        SELECT peer INTO nick
        FROM t
        ORDER BY first_input DESC
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

-- CALL peer_came_last(NULL);


-- 24) Определить пиров, которые выходили вчера из кампуса больше чем на N минут
-- Параметры процедуры: количество минут N.
-- Формат вывода: список пиров
DROP PROCEDURE IF EXISTS interval_left_yesterday CASCADE;

CREATE OR REPLACE PROCEDURE interval_left_yesterday(IN N int, IN ref refcursor) AS $$
    DECLARE yesterday date = (SELECT current_date - 1);
    BEGIN
        CREATE TABLE new_table (
            yest date
        );
        INSERT INTO new_table VALUES (yesterday);
        OPEN ref FOR
            WITH find_first_input AS (SELECT peer, date, MIN(time) AS min_time
                                      FROM timetracking
                                      WHERE state = 1 AND date = (SELECT yest FROM new_table)
                                      GROUP BY peer, date),
                find_last_output AS (SELECT peer, date, MAX(time) AS max_time
                                      FROM timetracking
                                      WHERE state = 2 AND date = (SELECT yest FROM new_table)
                                      GROUP BY peer, date),
                all_inputs AS (SELECT t.peer AS peer, t.time AS time
                               FROM timetracking AS t
                               JOIN find_first_input AS ff ON t.peer = ff.peer AND t.time != ff.min_time
                                                                AND t.state = 1
                               WHERE t.date = (SELECT yest FROM new_table)),
                all_outputs AS (SELECT t.peer AS peer, t.time AS time
                               FROM timetracking AS t
                               JOIN find_last_output AS fl ON t.peer = fl.peer AND t.time != fl.max_time
                                                                AND t.state = 2
                               WHERE t.date = (SELECT yest FROM new_table))
            SELECT all_inputs.peer AS peer
            FROM all_inputs
            JOIN all_outputs ON all_inputs.peer = all_outputs.peer
            WHERE  (all_outputs.time + (SELECT make_interval(mins => N))) > all_inputs.time;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL interval_left_yesterday(5, 'ref');
-- FETCH ALL IN "ref";
-- END;
-- DROP TABLE new_table CASCADE;


-- 25) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время
-- (будем называть это общим числом входов).
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00
-- за всё время (будем называть это числом ранних входов).
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов.
-- Формат вывода: месяц, процент ранних входов
DROP PROCEDURE IF EXISTS early_entry CASCADE;

CREATE OR REPLACE PROCEDURE early_entry(IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
        WITH birthday_month AS (SELECT nickname, date_part('month', birthday) AS b_month
                                FROM peers),
             all_entries AS (SELECT COUNT(*) AS sum_entries, b_month
                             FROM (SELECT peer, date, b_month
                                   FROM timetracking
                                   JOIN birthday_month ON timetracking.peer = birthday_month.nickname
                                   WHERE state = 1 AND date_part('month', date) = b_month
                                   GROUP BY peer, date, b_month) AS t
                             GROUP BY b_month),
        all_entries_early_12 AS (SELECT COUNT(*) AS sum_early_entries, b_month
                                 FROM (SELECT peer, date, b_month
                                       FROM timetracking
                                       JOIN birthday_month ON timetracking.peer = birthday_month.nickname
                                       WHERE state = 1 AND date_part('month', date) = b_month AND time < '12:00:00'
                                       GROUP BY peer, date, b_month) AS t
                                 GROUP BY b_month)
        SELECT (CASE WHEN a1.b_month = 1 THEN 'January'
                WHEN a1.b_month = 2 THEN 'February'
                WHEN a1.b_month = 3 THEN 'March'
                WHEN a1.b_month = 4 THEN 'April'
                WHEN a1.b_month = 5 THEN 'May'
                WHEN a1.b_month = 6 THEN 'June'
                WHEN a1.b_month = 7 THEN 'July'
                WHEN a1.b_month = 8 THEN 'August'
                WHEN a1.b_month = 9 THEN 'September'
                WHEN a1.b_month = 10 THEN 'October'
                WHEN a1.b_month = 11 THEN 'November' ELSE 'December' END) AS month,
               (a2.sum_early_entries * 100) / a1.sum_entries AS EarlyEntries
        FROM all_entries AS a1
        JOIN all_entries_early_12 AS a2 ON a1.b_month = a2.b_month;
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL early_entry('ref');
-- FETCH ALL IN "ref";
-- END;