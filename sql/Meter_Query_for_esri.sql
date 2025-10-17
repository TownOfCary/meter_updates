SELECT
    *
FROM
    (
        SELECT DISTINCT
            RIGHT(REPEAT('0', 9) || TRIM(CHAR(UT550AP.UTLCID)), 9) || '-' || TRIM(UT550AP.UTSVC) || '-' || RIGHT(REPEAT('0', 5) || TRIM(CHAR(UT550AP.UTSSEQ)), 5) AS NAVILINE_SERVICE_ID,
            TRIM(UT550AP.UTMTR) as MeterNumber,
            UT550AP.UTLCID as LocationID,
	    TRIM(locations.LMCDDS) as Location_On_Property,
            TRIM(UT550AP.UTSVC) as ServiceType,
            TRIM(UT550AP.UTMSZC) as Meter_Size,
            UT550AP.UTSSEQ as SEQNUMB,
            STRIP(
                (
                    CASE
                        WHEN STRIP(LMABREP.ABABTX) != '' THEN STRIP(LMABREP.ABABTX) || ' '
                        ELSE ''
                    END
                ) || (
                    CASE
                        WHEN STRIP(LMABREP.ABCHCD) != '' THEN STRIP(LMABREP.ABCHCD) || ' '
                        ELSE ''
                    END
                ) || (
                    CASE
                        WHEN STRIP(LMABREP.ABACCD) != '' THEN STRIP(LMABREP.ABACCD) || ' '
                        ELSE ''
                    END
                ) || (
                    CASE
                        WHEN STRIP(LMABREP.ABAECD) != '' THEN STRIP(LMABREP.ABAECD) || ' '
                        ELSE ''
                    END
                ) || (
                    CASE
                        WHEN STRIP(LMABREP.ABFXCD) != '' THEN STRIP(LMABREP.ABFXCD) || ' '
                        ELSE ''
                    END
                ) || (
                    CASE
                        WHEN STRIP(LMABREP.ABDFCD) != '' THEN STRIP(LMABREP.ABDFCD)
                        ELSE ''
                    END
                )
            ) AS ADDRESS,
            TRIM(UT100AP.UTCYCN) AS CycleNumb,

	    CASE
                WHEN UT550AP.UTINMM > 0
                AND UT550AP.UTINMM <= 12
                AND UT550AP.UTINDD > 0 THEN TRIM(
                    CHAR(1900 + (100 * UT550AP.UTINCC) + UT550AP.UTINYY)
                ) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTINMM)), 2) || '-' || RIGHT(REPEAT('0', 2) || TRIM(CHAR(UT550AP.UTINDD)), 2) || ' 00:00:00'
            END as InstallDate,
            TRIM(UT100AP.UTCYCN) || '-' || TRIM(UT100AP.UTRTEN) AS CycleRoute,
            TRIM(makes.LMCDDS) as Meter_Make,
            TRIM(radios.UTADID) as Radio,
            TRIM(registers.UTADID) as Register, 
            TRIM(UT100AP.UTJUR) AS Jurisdiction,
            TRIM(UT100AP.UTCLAS) AS Rate_Class,
            STRIP(UT200AP.UTCSNM) as CustName,
            'XXXXX' || RIGHT(TRIM(UT550AP.UTMTR),5) as MaskedMeterNumb
        FROM
            UT550AP
            LEFT JOIN LMABREP ON (
                UT550AP.UTLCID = LMABREP.ABAUCD
            )
            LEFT JOIN UT152AP ON (
                UT152AP.UTLCID = UT550AP.UTLCID
                AND UT152AP.UTSVC = UT550AP.UTSVC
                AND UT152AP.UTSSEQ = UT550AP.UTSSEQ
            )
            LEFT JOIN UT100AP ON UT100AP.UTLCID = UT550AP.UTLCID
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
            LEFT JOIN LM800AP locations ON (
                locations.LMCDAP = 'UT'
                AND locations.LMCDTP = 'ML'
                AND locations.LMCODE = UT152AP.UTMLOC
            )
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
                    STATUS IN ('A','T')
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
            LEFT JOIN UT200AP ON (Services.CID = UT200AP.UTCSID)
        WHERE UT550AP.UTMSTS != 'P'
    )
WHERE
    LocationID != 0

ORDER BY NAVILINE_SERVICE_ID





