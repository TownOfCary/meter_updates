SELECT
    METERNUMBER,
    CUSTOMERID,
    LOCATIONID,
    SERVICETYPE,
    NAVILINE_SERVICE_ID,
    METER_STATUS,
    METER_SERVICE,
    SEQNUMB,
    METER_SIZE,
    MULTIPLIER,
    METER_MAKE,
    METER_STYLE,
    WAREHOUSE_CODE,
    INSTALLDATE,
    MANUFACTURE_DATE,
    PURCHASE_DATE,
    RADIO,
    REGISTER,
    LAST_INSTALL_EVENT_DATE,
    LAST_INSTALL_EVENT_TYPE,
    CASE
        WHEN WAREHOUSE_CODE = 'AM' THEN 'AUDIT_METER'
        WHEN LOCATIONID = 0 AND WAREHOUSE_CODE = '' THEN 'NOT_INSTALLED/NO_WAREHOUSE'
        WHEN Meter_Status = 'PURGED' AND LOCATIONID != 0 THEN 'PURGED/INSTALLED'
        WHEN Meter_Status = 'PURGED' AND WAREHOUSE_CODE IN ('OP', 'UW') THEN 'PURGED/ACTIVE_WAREHOUSE'
        WHEN Meter_Status = 'PURGED' AND WAREHOUSE_CODE IN ('SW', 'WW') THEN 'CLEAN-PURGED/SCRAPPED_INVENTORY'
        WHEN Meter_Status = 'ACTIVE' AND LOCATIONID != 0 AND WAREHOUSE_CODE != '' THEN 'ACTIVE/INSTALLED/WAREHOUSE'
        WHEN Meter_Status = 'ACTIVE' AND LOCATIONID != 0 THEN 'CLEAN-ACTIVE/INSTALLED'
        WHEN Meter_Status = 'ACTIVE' AND WAREHOUSE_CODE IN ('SW', 'WW') THEN 'ACTIVE/INACTIVE_WAREHOUSE' 
        WHEN Meter_Status = 'ACTIVE' AND WAREHOUSE_CODE = 'UW' THEN 'CLEAN-ACTIVE/USED_INVENTORY'
        WHEN Meter_Status = 'ACTIVE' AND WAREHOUSE_CODE = 'OP' THEN 'CLEAN-ACTIVE/NEW_INVENTORY'
        ELSE 'UNKNOWN'
    END as DATA_ISSUE_TYPE 
FROM
    (
        SELECT
            DISTINCT STRIP(UT550AP.UTMTR) as METERNUMBER,
            Services.CID as CUSTOMERID,
            UT550AP.UTLCID as LOCATIONID,
            trim(UT550AP.UTSVC) as SERVICETYPE,
            CASE WHEN UT550AP.UTLCID != 0 THEN RIGHT(REPEAT('0', 9) || TRIM(CHAR(UT550AP.UTLCID)), 9) || '-' || TRIM(UT550AP.UTSVC) || '-' || RIGHT(REPEAT('0', 5) || TRIM(CHAR(UT550AP.UTSSEQ)), 5) END AS NAVILINE_SERVICE_ID,
            CASE
                WHEN UT550AP.UTMSTS = 'P' THEN 'PURGED'
                ELSE 'ACTIVE'
            END as METER_STATUS,
            UT550AP.UTTMS as METER_SERVICE,
            UT550AP.UTSSEQ as SEQNUMB,
            UT550AP.UTMSZC as METER_SIZE,
            STRIP(multipliers.UTADID) as MULTIPLIER,
            STRIP(makes.LMCDDS) as METER_MAKE,
            STRIP(styles.LMCDDS) as METER_STYLE,
            STRIP(UT550AP.UTWHSE) as WAREHOUSE_CODE,
            CASE
                WHEN UT550AP.UTINMM > 0
                AND UT550AP.UTINMM <= 12
                AND UT550AP.UTINDD > 0 THEN TRIM(
                    CHAR(1900 + (100 * UT550AP.UTINCC) + UT550AP.UTINYY)
                ) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTINMM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTINDD)), 2)
            END as INSTALLDATE,
            CASE
                WHEN UT550AP.UTMANM > 0
                AND UT550AP.UTMANM <= 12
                AND UT550AP.UTMAND > 0 THEN TRIM(
                    CHAR(1900 + (100 * UT550AP.UTMACC) + UT550AP.UTMANY)
                ) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTMANM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTMAND)), 2)
            END as MANUFACTURE_DATE,
            CASE
                WHEN UT550BP.UTPDTE != 0 THEN '20' || RIGHT(
                    REPEAT('0', 2) || TRIM(CHAR(MOD(INTEGER(UT550BP.UTPDTE) / 10000, 100))),
                    2
                ) || '-' || RIGHT(
                    REPEAT('0', 2) || TRIM(CHAR(MOD(INTEGER(UT550BP.UTPDTE) / 100, 100))),
                    2
                ) || '-' || RIGHT(
                    REPEAT('0', 2) || TRIM(CHAR(MOD(UT550BP.UTPDTE, 100))),
                    2
                )
            END as PURCHASE_DATE,
            STRIP(radios.UTADID) as RADIO,
            STRIP(registers.UTADID) as REGISTER,
            LEFT(last_events.DATETIME, 19) as LAST_INSTALL_EVENT_DATE,
            CASE WHEN last_events.event_type ='S' THEN 'INSTALL' WHEN last_events.event_type = 'O' THEN 'REMOVE' END as LAST_INSTALL_EVENT_TYPE
        FROM
            UT550AP
            LEFT JOIN UT550BP ON (UT550AP.UTMTR = UT550BP.UTMTR)
            LEFT JOIN (
                SELECT
                    UTMTR,
                    MAX(UTADID) as UTADID
                FROM
                    UT553AP
                WHERE
                    UTADTP = 'TC'
                GROUP BY
                    UTMTR
            ) multipliers ON (multipliers.UTMTR = UT550AP.UTMTR)
            LEFT JOIN (
                SELECT
                    UTMTR,
                    MAX(UTADID) as UTADID
                FROM
                    UT553AP
                WHERE
                    UTADTP = 'FX'
                GROUP BY
                    UTMTR
            ) radios ON (radios.UTMTR = UT550AP.UTMTR)
            LEFT JOIN (
                SELECT
                    UTMTR,
                    MAX(UTADID) as UTADID
                FROM
                    UT553AP
                WHERE
                    UTADTP = 'ER'
                GROUP BY
                    UTMTR
            ) registers ON (registers.UTMTR = UT550AP.UTMTR)
            LEFT JOIN LM800AP makes ON (
                makes.LMCDAP = 'UT'
                AND makes.LMCDTP = 'MM'
                AND makes.LMCODE = UT550AP.UTMMKE
            )
            LEFT JOIN LM800AP styles ON (
                styles.LMCDAP = 'UT'
                AND styles.LMCDTP = 'ST'
                AND styles.LMCODE = UT550AP.UTSTYL
            )
            LEFT JOIN (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            UTCSID as CID,
                            UTLCID as LID,
                            UTSVC as SVC,
                            UTSSTS as STATUS,
                            CASE
                                WHEN UTSSTM > 0
                                AND UTSSTM <= 12
                                AND UTSSTD > 0 THEN TO_DATE(
                                    TRIM(CHAR(1900 + (100 * UTSSTC) + UTSSTY)) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UTSSTM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UTSSTD)), 2),
                                    'YYYY-MM-DD'
                                )
                            END as ServiceStartDate,
                            CASE
                                WHEN UTSOFM > 0
                                AND UTSOFM <= 12
                                AND UTSOFD > 0 THEN TO_DATE(
                                    TRIM(CHAR(1900 + (100 * UTSOFC) + UTSOFY)) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UTSOFM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UTSOFD)), 2),
                                    'YYYY-MM-DD'
                                )
                            END as ServiceEndDate
                        FROM
                            UT220AP as service_inner
                    )
                WHERE
                    STATUS = 'A'
                    AND (
                        (
                            ServiceStartDate IS NOT NULL
                            AND ServiceEndDate IS NOT NULL
                            AND CURRENT DATE >= ServiceStartDate
                            AND CURRENT DATE < ServiceEndDate
                        )
                        OR (
                            ServiceStartDate IS NULL
                            AND ServiceEndDate IS NOT NULL
                            AND CURRENT DATE < ServiceEndDate
                        )
                        OR (
                            ServiceStartDate IS NOT NULL
                            AND ServiceEndDate IS NULL
                            AND CURRENT DATE >= ServiceStartDate
                        )
                        OR (
                            ServiceStartDate IS NULL
                            AND ServiceEndDate IS NULL
                        )
                    )
            ) services ON UT550AP.UTLCID = services.LID
            AND UT550AP.UTSVC = services.SVC
            LEFT JOIN (
                SELECT
                    METER,
                    DATETIME,
                    EVENT_TYPE
                FROM
                    (
                        SELECT
                            METER,
                            DATETIME,
                            EVENT_TYPE,
                            MAX(DATETIME) OVER (PARTITION BY METER) as LAST_DATETIME
                        FROM
                            (
                                SELECT
                                    events.UTMTR as METER,
                                    CASE
                                        WHEN events.UTRDMM > 0
                                        AND events.UTRDMM <= 12
                                        AND events.UTRDDD > 0 THEN TRIM(
                                            CHAR(1900 + (100 * events.UTRDCC) + events.UTRDYY)
                                        ) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(events.UTRDMM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(events.UTRDDD)), 2)
                                    END || ' ' || CASE
                                        WHEN (
                                            events.UTRDTM > 59
                                            AND events.UTRDTM < 100
                                        ) THEN '00:01:' || RIGHT(
                                            REPEAT('0', 2) || TRIM(CHAR(MOD(events.UTRDTM, 60))),
                                            2
                                        )
                                        ELSE RIGHT(
                                            REPEAT('0', 2) || TRIM(CHAR(INTEGER(events.UTRDTM) / 10000)),
                                            2
                                        ) || ':' || RIGHT(
                                            REPEAT('0', 2) || TRIM(CHAR(MOD(INTEGER(events.UTRDTM) / 100, 100))),
                                            2
                                        ) || ':' || RIGHT(
                                            REPEAT('0', 2) || TRIM(CHAR(MOD(events.UTRDTM, 100))),
                                            2
                                        ) || ' ' || events.UTRDTP
                                    END as DATETIME,
                                    events.UTRDTP as EVENT_TYPE
                                FROM
                                    UT580AP events
                                WHERE
                                    events.UTRDTP IN ('S', 'O')
                            )
                    )
                WHERE
                    DATETIME = LAST_DATETIME
            ) last_events ON UT550AP.UTMTR = last_events.METER
    )
WHERE
    LOCATIONID != 0
    OR WAREHOUSE_CODE IN ('OP', 'UW')
    OR LAST_INSTALL_EVENT_DATE > '2022-06-01 00:00:00'