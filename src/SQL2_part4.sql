-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей
-- базы данных, имена которых начинаются с фразы 'TableName'.
CREATE TABLE table_for_delete (
    column1 varchar,
    column2 varchar,
    column3 varchar
);

CREATE TABLE table_for_delete2 (
    column1 varchar,
    column2 varchar,
    column3 varchar
);

DROP PROCEDURE IF EXISTS remove_table CASCADE;

CREATE OR REPLACE PROCEDURE remove_table(IN tablename text) AS $$
    BEGIN
        FOR tablename IN (SELECT table_name
                       FROM information_schema.tables
                       WHERE table_name LIKE concat(tablename,'%') AND table_schema = 'public')
            LOOP
                EXECUTE  concat('DROP TABLE ', tablename);
            END LOOP;
    END;
$$ LANGUAGE plpgsql;

-- CALL remove_table('table_for');


-- 2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров всех
-- скалярных SQL функций пользователя в текущей базе данных. Имена функций без параметров не выводить.
-- Имена и список параметров должны выводиться в одну строку. Выходной параметр возвращает количество
-- найденных функций.
DROP PROCEDURE IF EXISTS count_table CASCADE;

CREATE OR REPLACE PROCEDURE count_table(OUT count_tables int) AS $$
    BEGIN
        WITH get_params AS (SELECT r.routine_name AS function,
                        concat('(', p.parameter_mode, ' ', p.parameter_name, ' ', p.data_type, ')') AS params
                   FROM information_schema.routines AS r
                   JOIN information_schema.parameters AS p ON r.specific_name = p.specific_name
                   WHERE r.routine_type = 'FUNCTION' AND r.specific_schema = 'public' AND
                         p.specific_schema = 'public' AND parameter_name IS NOT NULL),
             f_concat AS (SELECT concat(function, ' ', string_agg(params, ','))
                          FROM get_params
                          GROUP BY function)
        SELECT COUNT(*) INTO count_tables
        FROM f_concat;
    END;
$$ LANGUAGE plpgsql;

-- CALL count_table(NULL);


-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей
-- базе данных. Выходной параметр возвращает количество уничтоженных триггеров.
DROP PROCEDURE IF EXISTS delete_dml_triggers CASCADE;

CREATE OR REPLACE PROCEDURE delete_dml_triggers(OUT count_drops int) AS $$
    DECLARE trg_name text;
            table_name text;
    BEGIN
        SELECT COUNT(DISTINCT trigger_name) INTO count_drops
        FROM information_schema.triggers
        WHERE trigger_schema = 'public';

        FOR trg_name, table_name IN (SELECT DISTINCT trigger_name, event_object_table
                         FROM information_schema.triggers
                         WHERE trigger_schema = 'public')
            LOOP
                EXECUTE concat('DROP TRIGGER ', trg_name, ' ON ', table_name);
            END LOOP;
    END;
$$ LANGUAGE plpgsql;

-- CALL delete_dml_triggers(NULL);


-- 4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов
-- (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка,
-- задаваемая параметром процедуры.
DROP PROCEDURE IF EXISTS show_info CASCADE;

CREATE OR REPLACE PROCEDURE show_info(IN name text, IN ref refcursor) AS $$
    BEGIN
        OPEN ref FOR
            SELECT routine_name AS name, routine_type AS type
            FROM information_schema.routines
            WHERE specific_schema = 'public' AND routine_definition LIKE concat('%', name, '%');
    END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL show_info('p2p', 'ref');
-- FETCH ALL IN "ref";
-- END;