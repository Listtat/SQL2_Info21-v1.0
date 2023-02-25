-- Создание таблицы Peers
DROP TABLE IF EXISTS Peers CASCADE;

CREATE TABLE Peers (
    Nickname varchar NOT NULL PRIMARY KEY,
    Birthday date NOT NULL
);


-- Создание таблицы TransferredPoints
DROP TABLE IF EXISTS TransferredPoints CASCADE;

CREATE TABLE TransferredPoints (
    ID bigint PRIMARY KEY NOT NULL,
    CheckingPeer varchar NOT NULL,
    CheckedPeer varchar NOT NULL,
    PointsAmount bigint NOT NULL,
    FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    FOREIGN KEY (CheckedPeer) REFERENCES Peers(Nickname)
);


-- Триггерная функция приведения ников к нижнему регистру
DROP FUNCTION IF EXISTS insert_lower_nickname() CASCADE;

CREATE OR REPLACE FUNCTION insert_lower_nickname() RETURNS TRIGGER AS $trg_insert_lower_nickname$
    BEGIN
        new.nickname := lower(new.nickname);
        RETURN new;
    END;
$trg_insert_lower_nickname$ LANGUAGE plpgsql;

-- Триггер, вызывающий функцию приведения ников к нижнему регистру
DROP TRIGGER IF EXISTS trg_insert_lower_nickname ON peers CASCADE;

CREATE TRIGGER trg_insert_lower_nickname
BEFORE UPDATE OR INSERT ON Peers
    FOR EACH ROW EXECUTE PROCEDURE insert_lower_nickname();


-- Триггерная функция для составления пар в таблице transferred points
DROP FUNCTION IF EXISTS insert_in_transferredpoints() CASCADE;

CREATE OR REPLACE FUNCTION insert_in_transferredpoints() RETURNS TRIGGER AS $trg_in_transferredpoints$
    DECLARE peer_nick varchar;
    BEGIN
        IF ((SELECT COUNT(*) FROM peers) > 1) THEN
            FOR peer_nick IN (SELECT nickname FROM peers) LOOP
                peer_nick := replace(peer_nick, '(', '');
                peer_nick := replace(peer_nick, ')', '');
                IF (peer_nick != NEW.nickname AND (SELECT COUNT(*) FROM transferredpoints
                                                   WHERE peer_nick = checkingpeer AND
                                                         NEW.nickname = transferredpoints.checkedpeer) = 0) THEN
                    INSERT INTO transferredpoints
                    VALUES (COALESCE((SELECT MAX(id) FROM transferredpoints), 0) + 1, peer_nick, NEW.nickname, 0);

                    INSERT INTO transferredpoints
                    VALUES ((SELECT MAX(id) FROM transferredpoints) + 1, NEW.nickname, peer_nick, 0);
                END IF;
            END LOOP;
        END IF;
        RETURN NULL;
    END;
$trg_in_transferredpoints$ LANGUAGE plpgsql;

-- Триггер, вызывающий функцию составления пар в таблице transferred points
DROP TRIGGER IF EXISTS trg_in_transferredpoints ON Peers;

CREATE TRIGGER trg_in_transferredpoints
AFTER INSERT ON Peers
    FOR EACH ROW EXECUTE FUNCTION insert_in_transferredpoints();


-- Добавление пользователей в таблицу Peers
INSERT INTO peers (nickname, birthday)
VALUES ('changeli', '1996-01-01'),
       ('tamelabe', '1996-01-02'),
       ('yonnarge', '1996-01-03'),
       ('alesande', '1996-01-04'),
       ('violette', '1996-01-05'),
       ('curranca', '1996-01-06'),
       ('milagros', '1996-01-07'),
       ('keyesdar', '1996-01-08'),
       ('mikaelag', '1996-01-09'),
       ('rossetel', '1996-01-10');


-- Создание таблицы Tasks
DROP TABLE IF EXISTS tasks CASCADE;

CREATE TABLE Tasks (
    Title text NOT NULL PRIMARY KEY,
    ParentTask text,
    MaxXP BIGINT NOT NULL,
    FOREIGN KEY (ParentTask) REFERENCES Tasks (Title)
);

-- Добавление значений в таблицу Tasks
INSERT INTO Tasks
VALUES ('C2_SimpleBashUtils', NULL, 250),
       ('C3_s21_string+', 'C2_SimpleBashUtils', 500),
       ('C4_s21_math', 'C2_SimpleBashUtils', 300),
       ('C5_s21_decimal', 'C4_s21_math', 350),
       ('C6_s21_matrix', 'C5_s21_decimal', 200),
       ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
       ('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
       ('DO1_Linux', 'C3_s21_string+', 300),
       ('DO2_Linux Network', 'DO1_Linux', 250),
       ('DO3_LinuxMonitoring v1.0', 'DO2_Linux Network', 350),
       ('DO4_LinuxMonitoring v2.0', 'DO3_LinuxMonitoring v1.0', 350),
       ('DO5_SimpleDocker', 'DO3_LinuxMonitoring v1.0', 300),
       ('DO6_CICD', 'DO5_SimpleDocker', 300),
       ('CPP1_s21_matrix+', 'C8_3DViewer_v1.0', 300),
       ('CPP2_s21_containers', 'CPP1_s21_matrix+', 350),
       ('CPP3_SmartCalc_v2.0', 'CPP2_s21_containers', 600),
       ('CPP4_3DViewer_v2.0', 'CPP3_SmartCalc_v2.0', 750),
       ('CPP5_3DViewer_v2.1', 'CPP4_3DViewer_v2.0', 600),
       ('CPP6_3DViewer_v2.2', 'CPP4_3DViewer_v2.0', 800),
       ('CPP7_MLP', 'CPP4_3DViewer_v2.0', 700),
       ('CPP8_PhotoLab_v1.0', 'CPP4_3DViewer_v2.0', 450),
       ('CPP9_MonitoringSystem', 'CPP4_3DViewer_v2.0', 1000),
       ('A1_Maze', 'CPP4_3DViewer_v2.0', 300),
       ('A2_SimpleNavigator v1.0', 'A1_Maze', 400),
       ('A3_Parallels', 'A2_SimpleNavigator v1.0', 300),
       ('A4_Crypto', 'A2_SimpleNavigator v1.0', 350),
       ('A5_s21_memory', 'A2_SimpleNavigator v1.0', 400),
       ('A6_Transactions', 'A2_SimpleNavigator v1.0', 700),
       ('A7_DNA Analyzer', 'A2_SimpleNavigator v1.0', 800),
       ('A8_Algorithmic trading', 'A2_SimpleNavigator v1.0', 800),
       ('SQL1_Bootcamp', 'C8_3DViewer_v1.0', 1500),
       ('SQL2_Info21 v1.0', 'SQL1_Bootcamp', 500),
       ('SQL3_RetailAnalitycs v1.0', 'SQL2_Info21 v1.0', 600);

-- Создание перечисления "Статус проверки"
DROP TYPE IF EXISTS check_status CASCADE;
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');


-- Создание таблицы Checks
DROP TABLE IF EXISTS Checks CASCADE;

CREATE TABLE Checks (
    ID BIGINT PRIMARY KEY NOT NULL,
    Peer varchar NOT NULL,
    Task text NOT NULL,
    Date date NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    FOREIGN KEY (Task) REFERENCES Tasks (Title)
);


-- Добавление значений в таблицу Checks Часть 2
INSERT INTO Checks (id, peer, task, date)
VALUES (1, 'changeli', 'C2_SimpleBashUtils', '2022-06-01'),
       (2, 'changeli', 'C2_SimpleBashUtils', '2022-06-06'),
       (3, 'tamelabe', 'C4_s21_math', '2022-05-06'),
       (4, 'yonnarge', 'C6_s21_matrix', '2022-07-16'),
       (5, 'yonnarge', 'C6_s21_matrix', '2022-07-20'),
       (6, 'keyesdar', 'DO1_Linux', '2022-06-16'),
       (7, 'rossetel', 'DO2_Linux Network', '2022-07-16'),
       (8, 'changeli', 'DO2_Linux Network', '2022-07-16'),
       (9, 'changeli', 'DO3_LinuxMonitoring v1.0', '2022-08-21'),
       (10, 'mikaelag', 'C5_s21_decimal', '2022-05-21'),
       (11, 'changeli', 'C3_s21_string+', '2022-06-06'),
       (12, 'milagros', 'C4_s21_math', '2022-07-08'),
       (13, 'tamelabe', 'C3_s21_string+', '2022-08-08'),
       (14, 'violette', 'DO1_Linux', '2022-06-01'),
       (15, 'alesande', 'C6_s21_matrix', '2022-10-10'),
       (16, 'curranca', 'DO1_Linux', '2022-07-07'),
       (17, 'changeli', 'C2_SimpleBashUtils', '2022-06-07');


-- Создание таблицы P2P
DROP TABLE IF EXISTS P2P CASCADE;

CREATE TABLE P2P (
    ID BIGINT PRIMARY KEY NOT NULL,
    "Check" bigint NOT NULL,
    CheckingPeer varchar NOT NULL,
    State check_status NOT NULL,
    Time time NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks (ID),
    FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname)
);


-- Добавление значений в таблицу P2P Часть 2
INSERT INTO P2P (id, "Check", CheckingPeer, State, Time)
VALUES (1, 1, 'tamelabe', 'Start', '09:00:00'),
       (2, 1, 'tamelabe', 'Failure', '10:00:00'),  -- Пир завалил

       (3, 2, 'yonnarge', 'Start', '13:00:00'),
       (4, 2, 'yonnarge', 'Success', '14:00:00'),

       (5, 3, 'changeli', 'Start', '22:00:00'),
       (6, 3, 'changeli', 'Success', '23:00:00'),

       (7, 4, 'curranca', 'Start', '15:00:00'),
       (8, 4, 'curranca', 'Success', '16:00:00'),  -- Verter завалил

       (9, 5, 'rossetel', 'Start', '14:00:00'),
       (10, 5, 'rossetel', 'Success', '15:00:00'),

       (11, 6, 'violette', 'Start', '01:00:00'),
       (12, 6, 'violette', 'Success', '02:00:00'),

       (13, 7, 'keyesdar', 'Start', '10:00:00'),
       (14, 7, 'keyesdar', 'Success', '12:00:00'),

       (15, 8, 'mikaelag', 'Start', '12:00:00'),
       (16, 8, 'mikaelag', 'Success', '13:00:00'),

       (17, 9, 'tamelabe', 'Start', '12:00:00'),
       (18, 9, 'tamelabe', 'Success', '13:00:00'),

       (19, 10, 'alesande', 'Start', '19:00:00'),

       (20, 11, 'keyesdar', 'Start', '15:00:00'),
       (21, 11, 'keyesdar', 'Success', '15:01:00'),

       (22, 12, 'curranca', 'Start', '22:00:00'),
       (23, 12, 'curranca', 'Failure', '23:00:00'),

       (24, 13, 'rossetel', 'Start', '22:00:00'),
       (25, 13, 'rossetel', 'Success', '23:00:00'),

       (26, 14, 'changeli', 'Start', '22:00:00'),
       (27, 14, 'changeli', 'Success', '23:00:00'),

       (28, 15, 'curranca', 'Start', '04:00:00'),
       (29, 15, 'curranca', 'Success', '05:00:00'),

       (30, 16, 'milagros', 'Start', '05:00:00'),
       (31, 16, 'milagros', 'Failure', '06:00:00'),

       (32, 17, 'milagros', 'Start', '05:00:00'),
       (33, 17, 'milagros', 'Success', '06:00:00');


-- Создание таблицы Verter
DROP TABLE IF EXISTS Verter CASCADE;

CREATE TABLE Verter (
    ID bigint PRIMARY KEY NOT NULL,
    "Check" bigint NOT NULL,
    State check_status NOT NULL,
    Time time NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks(ID)
);

-- Добавление значений в таблицу Verter
INSERT INTO Verter (id, "Check", State, Time)
VALUES (1, 2, 'Start', '13:01:00'),
       (2, 2, 'Success', '13:02:00'),

       (3, 3, 'Start', '23:01:00'),
       (4, 3, 'Success', '23:02:00'),

       (5, 4, 'Start', '16:01:00'),
       (6, 4, 'Failure', '16:02:00'),

       (7, 5, 'Start', '15:01:00'),
       (8, 5, 'Success', '15:02:00'),

       (9, 13, 'Start', '23:01:00'),
       (10, 13, 'Success', '23:02:00'),

       (11, 15, 'Start', '05:01:00'),
       (12, 15, 'Failure', '05:02:00'),

       (13, 17, 'Start', '06:01:00'),
       (14, 17, 'Success', '06:02:00');


-- Создание таблицы Friends
DROP TABLE IF EXISTS Friends CASCADE;

CREATE TABLE Friends (
    ID bigint PRIMARY KEY NOT NULL,
    Peer1 varchar NOT NULL,
    Peer2 varchar NOT NULL,
    FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    FOREIGN KEY (Peer2) REFERENCES Peers(Nickname)
);

-- Добавление значений в таблицу Friends
-- Добавить ограничения, чтобы строки не дублировались (changeli - tamelabe, tamelabe - changeli)
INSERT INTO Friends (id, Peer1, Peer2)
VALUES (1, 'changeli', 'tamelabe'),
       (2, 'changeli', 'mikaelag'),
       (3, 'tamelabe', 'mikaelag'),
       (4, 'tamelabe', 'rossetel'),
       (5, 'violette', 'milagros'),
       (6, 'curranca', 'changeli'),
       (7, 'yonnarge', 'tamelabe'),
       (8, 'alesande', 'yonnarge'),
       (9, 'milagros', 'keyesdar'),
       (10, 'yonnarge', 'alesande');


-- Создание таблицы Recommendations
DROP TABLE IF EXISTS Recommendations CASCADE;

CREATE TABLE Recommendations (
    ID bigint PRIMARY KEY NOT NULL,
    Peer varchar NOT NULL,
    RecommendedPeer varchar NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname)
);

-- Добавление значений в таблицу Recommendations
INSERT INTO Recommendations (id, Peer, RecommendedPeer)
VALUES (1, 'changeli', 'tamelabe'),
       (2, 'changeli', 'violette'),
       (3, 'tamelabe', 'yonnarge'),
       (4, 'curranca', 'changeli'),
       (5, 'alesande', 'curranca'),
       (6, 'alesande', 'milagros'),
       (7, 'keyesdar', 'alesande'),
       (8, 'milagros', 'tamelabe'),
       (9, 'mikaelag', 'keyesdar'),
       (10, 'mikaelag', 'rossetel');


-- Создание таблицы XP
DROP TABLE IF EXISTS XP CASCADE;

CREATE TABLE XP (
    ID bigint PRIMARY KEY NOT NULL,
    "Check" bigint NOT NULL,
    XPAmount bigint NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks(ID)
);

-- Добавление значений в таблицу XP
INSERT INTO XP (id, "Check", XPAmount)
VALUES (1, 2, 240),
       (2, 3, 300),
       (3, 5, 200),
       (4, 6, 250),
       (5, 7, 250),
       (6, 8, 250),
       (7, 9, 350),
       (8, 10, 299),
       (9, 17, 250);


-- Создание таблицы TimeTracking
DROP TABLE IF EXISTS TimeTracking CASCADE;

CREATE TABLE TimeTracking (
    ID bigint PRIMARY KEY NOT NULL,
    Peer varchar NOT NULL,
    Date date NOT NULL,
    Time time NOT NULL,
    State bigint NOT NULL CHECK (State IN (1, 2)),
    FOREIGN KEY (Peer) REFERENCES Peers(Nickname)
);

-- Добавление значений в таблицу TimeTracking
INSERT INTO TimeTracking (id, Peer, Date, Time, State)
VALUES (1, 'changeli', '2022-05-02', '08:00:00', 1),
       (2, 'changeli', '2022-05-02', '18:00:00', 2),
       (3, 'tamelabe', '2022-05-02', '18:30:00', 1),
       (4, 'tamelabe', '2022-05-02', '23:30:00', 2),
       (5, 'changeli', '2022-05-02', '18:10:00', 1),
       (6, 'changeli', '2022-05-02', '21:00:00', 2),
       (7, 'curranca', '2022-06-22', '10:00:00', 1),
       (8, 'tamelabe', '2022-06-22', '11:00:00', 1),
       (9, 'tamelabe', '2022-06-22', '21:00:00', 2),
       (10, 'curranca', '2022-06-22', '23:00:00', 2);

-- Создание процедуры для экпорта данных в файлы
DROP PROCEDURE IF EXISTS export() CASCADE;

CREATE OR REPLACE PROCEDURE export(IN tablename varchar, IN path text, IN separator char) AS $$
    BEGIN
        EXECUTE format('COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;',
            tablename, path, separator);
    END;
$$ LANGUAGE plpgsql;

-- Создание процедуры для импорта данных из файлов
DROP PROCEDURE IF EXISTS import() CASCADE;

CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN path text, IN separator char) AS $$
    BEGIN
        EXECUTE format('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;',
            tablename, path, separator);
    END;
$$ LANGUAGE plpgsql;


-- ************CHECK PROCEDURES************
-- *****************EXPORT*****************
-- CALL export('Peers', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/peers.csv', ',');
-- CALL export('Tasks', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/Tasks.csv', ',');
-- CALL export('Checks', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/checks.csv', ',');
-- CALL export('P2P', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/p2p.csv', ',');
-- CALL export('verter', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/verter.csv', ',');
-- CALL export('transferredpoints', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/transferredpoints.csv', ',');
-- CALL export('friends', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/friends.csv', ',');
-- CALL export('recommendations', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/recommendations.csv', ',');
-- CALL export('xp', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/xp.csv', ',');
-- CALL export('timetracking', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/timetracking.csv', ',');

-- TRUNCATE TABLE Peers CASCADE;
-- TRUNCATE TABLE Tasks CASCADE;
-- TRUNCATE TABLE Checks CASCADE;
-- TRUNCATE TABLE P2P CASCADE;
-- TRUNCATE TABLE Verter CASCADE;
-- TRUNCATE TABLE Transferredpoints CASCADE;
-- TRUNCATE TABLE Friends CASCADE;
-- TRUNCATE TABLE Recommendations CASCADE;
-- TRUNCATE TABLE XP CASCADE;
-- TRUNCATE TABLE TimeTracking CASCADE;

-- *****************IMPORT*****************
-- CALL import('Peers', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/peers.csv', ',');
-- CALL import('Tasks', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/Tasks.csv', ',');
-- CALL import('Checks', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/checks.csv', ',');
-- CALL import('P2P', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/p2p.csv', ',');
-- CALL import('verter', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/verter.csv', ',');
-- CALL import('transferredpoints', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/transferredpoints.csv', ',');
-- CALL import('friends', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/friends.csv', ',');
-- CALL import('recommendations', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/recommendations.csv', ',');
-- CALL import('xp', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/xp.csv', ',');
-- CALL import('timetracking', '/Users/changeli/goinfre/SQL_1/SQL2_Info21_v1.0-0-master/src/timetracking.csv', ',');


-- DROP TABLE checks CASCADE ;
-- DROP TABLE friends CASCADE ;
-- DROP TABLE p2p CASCADE ;
-- DROP TABLE peers CASCADE ;
-- DROP TABLE recommendations CASCADE ;
-- DROP TABLE tasks CASCADE ;
-- DROP TABLE timetracking CASCADE ;
-- DROP TABLE transferredpoints CASCADE ;
-- DROP TABLE verter CASCADE ;
-- DROP TABLE xp CASCADE ;
-- DROP TYPE check_status;
-- DROP PROCEDURE export CASCADE ;
-- DROP PROCEDURE import CASCADE ;