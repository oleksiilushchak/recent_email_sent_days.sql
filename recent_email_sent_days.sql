SELECT
    account_id,
    sent_date,
    sent_day_rank
FROM (
    SELECT
        account_id,
        sent_date,
        DENSE_RANK() OVER (
            PARTITION BY account_id
            ORDER BY sent_date DESC
        ) AS sent_day_rank
    FROM (
        -- Build distinct sent dates per account
        SELECT DISTINCT
            es.id_account AS account_id,
            DATE_ADD(sess.date, INTERVAL es.sent_date DAY) AS sent_date
        FROM `DA.email_sent` es
        JOIN `DA.account_session` acs
            ON es.id_account = acs.account_id
        JOIN `DA.session` sess
            ON acs.ga_session_id = sess.ga_session_id
    ) AS email_sent_table
) AS ranked_days
WHERE sent_day_rank <= 10
ORDER BY account_id, sent_day_rank;
