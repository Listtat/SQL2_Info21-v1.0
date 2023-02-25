-- Процедура добавления p2p проверки
DROP PROCEDURE IF EXISTS add_p2p_review CASCADE;

CREATE OR REPLACE PROCEDURE add_p2p_review(IN nick_checkedpeer varchar, IN nick_checkingpeer varchar,
                        IN task_name text, IN p2p_status check_status, IN check_time time) AS $$
    BEGIN
        IF (p2p_status = 'Start') THEN
            IF ((SELECT COUNT(*) FROM p2p
                JOIN checks ON p2p."Check" = checks.id
                WHERE p2p.checkingpeer = nick_checkingpeer
                    AND checks.peer = nick_checkedpeer AND checks.task = task_name) = 1) THEN
                RAISE EXCEPTION 'Добавление записи невозможно. У данной пары пиров имеется незавершенная проверка';
            ELSE
                INSERT INTO checks
                VALUES ((SELECT MAX(id) + 1 FROM checks), nick_checkedpeer, task_name, NOW());

                INSERT INTO p2p
                VALUES ((SELECT MAX(id) FROM p2p) + 1, (SELECT MAX(id) FROM checks),
                    nick_checkingpeer, p2p_status, check_time);
            END IF;
        ELSE
            INSERT INTO p2p
            VALUES ((SELECT MAX(id) FROM p2p) + 1,
                    (SELECT "Check" FROM p2p
                     JOIN checks ON p2p."Check" = checks.id
                     WHERE p2p.checkingpeer = nick_checkingpeer AND checks.peer = nick_checkedpeer
                       AND checks.task = task_name),
                    nick_checkingpeer, p2p_status, check_time);
        END IF;
    END;
$$ LANGUAGE plpgsql;

-- Добавление проверки проекта "s21_decimal" для пира "rossetel" пиром "changeli"
-- CALL add_p2p_review('rossetel', 'changeli', 'C5_s21_decimal', 'Start'::check_status, '12:00:00');
-- DELETE FROM p2p WHERE id = 34;
-- Попытка добавления записи, при имеющейся незавершенной проверки проекта "s21_decimal" у пары пиров
-- CALL add_p2p_review('mikaelag', 'alesande', 'C5_s21_decimal', 'Start'::check_status, '12:00:00');
-- Добавление записей для случая, когда у проверяющего имеется незакрытая проверка
-- CALL add_p2p_review('changeli', 'alesande', 'C5_s21_decimal', 'Start'::check_status, '12:00:00');
-- DELETE FROM p2p WHERE id = 34;
-- Добавление записи в таблицу p2p со статусом "Success" или "Failure"
-- CALL add_p2p_review('mikaelag', 'alesande', 'C5_s21_decimal', 'Failure'::check_status, '12:00:00');
-- DELETE FROM p2p WHERE id = 34;
-- Попытка добавления неверной записи
-- CALL add_p2p_review('changeli', 'alesande', 'C5_s21_decimal', 'Failure'::check_status, '12:00:00');


-- Процедура добавления verter проверки
DROP PROCEDURE IF EXISTS add_verter_review CASCADE;

CREATE OR REPLACE PROCEDURE add_verter_review(IN nick_checkedpeer varchar, IN task_name text,
                            IN verter_status check_status, IN check_time time) AS $$
    BEGIN
        IF (verter_status = 'Start') THEN
                IF ((SELECT MAX(p2p.time) FROM p2p
                    JOIN checks ON p2p."Check" = checks.id
                    WHERE checks.peer = nick_checkedpeer AND checks.task = task_name
                        AND p2p.state = 'Success') IS NOT NULL ) THEN

                    INSERT INTO verter
                    VALUES ((SELECT MAX(id) FROM verter) + 1,
                            (SELECT DISTINCT checks.id FROM p2p
                             JOIN checks ON p2p."Check" = checks.id
                             WHERE checks.peer = nick_checkedpeer AND p2p.state = 'Success'
                                AND checks.task = task_name),
                            verter_status, check_time);
            ELSE
                RAISE EXCEPTION 'Добавление записи невозможно.'
                    'P2P-проверка для задания не завершена или имеет статус Failure';
            END IF;
        ELSE
            INSERT INTO verter
            VALUES ((SELECT MAX(id) FROM verter) + 1,
                    (SELECT "Check" FROM verter
                     GROUP BY "Check" HAVING COUNT(*) % 2 = 1), verter_status, check_time);
        END IF;
    END;
$$ LANGUAGE plpgsql;

-- Добавление проверки проекта "s21_string+" со статусом "Start". P2P прошла успешно
-- CALL add_verter_review('changeli', 'C3_s21_string+', 'Start', '15:02:00');
-- Добавление проверки проекта "s21_string+" со статусом "Success" или "Failure". P2P прошла успешно
-- CALL add_verter_review('changeli', 'C3_s21_string+', 'Failure', '15:03:00');
-- DELETE FROM verter WHERE id = 15;
-- DELETE FROM verter WHERE id = 16;
-- Попытка добавления записи при условии, что p2p проверка еще не завершена
-- CALL add_verter_review('mikaelag', 'C5_s21_decimal', 'Start', '15:03:00');
-- Попытка добавления записи при условии, что нет успешных p2p проверок
-- CALL add_verter_review('milagros', 'C4_s21_math', 'Start', '15:03:00');


-- Триггерная функция для обновления таблицы transferredpoints
DROP FUNCTION IF EXISTS fnc_trg_update_transferredpoints() CASCADE;

CREATE OR REPLACE FUNCTION fnc_trg_update_transferredpoints() RETURNS TRIGGER AS $trg_update_transferredpoints$
    BEGIN
       IF (NEW.state = 'Start') THEN
           WITH new_table AS (
               SELECT checks.peer AS peer FROM p2p
               JOIN checks ON p2p."Check" = checks.id
               WHERE state = 'Start' AND NEW."Check" = checks.id
           )
           UPDATE transferredpoints
           SET pointsamount = pointsamount + 1
           FROM new_table
           WHERE new_table.peer = transferredpoints.checkedpeer AND
                 NEW.checkingpeer = transferredpoints.checkingpeer;
       END IF;
       RETURN NULL;
    END;
$trg_update_transferredpoints$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_transferredpoints
AFTER INSERT ON P2P
    FOR EACH ROW EXECUTE FUNCTION fnc_trg_update_transferredpoints();

-- Изменение количества поинтов в паре пиров mikaelag - changeli через INSERT
-- INSERT INTO p2p
-- values (34, 8, 'mikaelag', 'Start', '23:00:00');
-- DELETE FROM p2p WHERE id = 34;
-- Изменение количества поинтов в паре пиров mikaelag - changeli через add_p2p_review
-- CALL add_p2p_review('changeli', 'mikaelag', 'C6_s21_matrix', 'Start', '12:00:00');
-- DELETE FROM p2p WHERE id = 34;

-- Триггерная функция проверки значений перед добавление XP
DROP FUNCTION IF EXISTS fnc_check_correct_before_insert_xp() CASCADE;

CREATE OR REPLACE FUNCTION fnc_check_correct_before_insert_xp() RETURNS TRIGGER AS $trg_check_correct_before_insert_xp$
    BEGIN
        IF ((SELECT maxxp FROM checks
            JOIN tasks ON checks.task = tasks.title
            WHERE NEW."Check" = checks.id) < NEW.xpamount OR
            (SELECT state FROM p2p
             WHERE NEW."Check" = p2p."Check" AND p2p.state IN ('Success', 'Failure')) = 'Failure' OR
            (SELECT state FROM verter
             WHERE NEW."Check" = verter."Check" AND verter.state = 'Failure') = 'Failure') THEN
                RAISE EXCEPTION 'Количество ХР превышает максимум или результат проверки неуспешный';
        END IF;
    RETURN (NEW.id, NEW."Check", NEW.xpamount);
    END;
$trg_check_correct_before_insert_xp$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_correct_before_insert_xp
BEFORE INSERT ON XP
    FOR EACH ROW EXECUTE FUNCTION fnc_check_correct_before_insert_xp();

-- Добавление записи в таблицу ХР при условии, что проверки p2p и verter успешны (меняем кол-во экспы)
-- INSERT INTO xp (id, "Check", xpamount)
-- VALUES (10, 13, 500);
-- DELETE FROM xp WHERE id = 10;
-- Добавление записи в таблицу ХР при условии, что проверка р2р успешна, а проверки verter нет
-- INSERT INTO xp (id, "Check", xpamount)
-- VALUES (10, 14, 300);
-- DELETE FROM xp WHERE id = 10;
-- Добавление записи в таблицу ХР при условии, что проверка р2р успешна, а проверка verter не успешна
-- INSERT INTO xp (id, "Check", xpamount)
-- VALUES (10, 15, 100);
-- Добавление записи в таблицу ХР при условии, что проверка р2р не успешна
-- INSERT INTO xp (id, "Check", xpamount)
-- VALUES (10, 16, 150)