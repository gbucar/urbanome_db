create table agg.noisecapture_filtered_record as
select
    *
from
    agg.noiscapture_nocar_record nncr
where
    (
        nncr.record_uuid in (
            select
                nnsr.record_uuid
            from
                agg.noisecapture_nostatic_record nnsr
        )
        and nncr.duration_seconds > 20
    );

create table agg.noisecapture_nostatic_record as
SELECT
    *
FROM
    noisecapture_raw_record nrr
WHERE
    nrr.record_uuid IN (
        SELECT DISTINCT
            record_uuid
        FROM
            noisecapture_raw_point
        WHERE
            speed > 2.6
    );
