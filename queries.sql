create table agg.noisecapture_nocar_record_view as
SELECT
    *
FROM
    public.noisecapture_raw_record nrr
where
    not exists (
        select
            1
        from
            public.noisecapture_raw_point nrp
        where
            (nrr.record_uuid = nrp.record_uuid)
            and (nrp.speed > 11)
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

create table agg.noisecapture_points_with_next_point as
SELECT
    record_uuid,
    location_utc,
    geom,
    LEAD (geom) OVER (
        PARTITION BY
            record_uuid
        ORDER BY
            location_utc
    ) AS next_geom
FROM
    noisecapture_raw_point nrp
ORDER BY
    location_utc;

create table agg.noisecapture_nojump_record as
select
    *
from
    noisecapture_raw_record nrr
where
    nrr.record_uuid not in (
        select distinct
            record_uuid
        from
            agg.noisecapture_points_with_next_point nnp
        where
            ST_Distance (
                ST_Transform (geom, 3857),
                ST_Transform (next_geom, 3857)
            ) > 70
    );

create table agg.noisecapture_filtered_record as
select
    *
from
    agg.noisecapture_nocar_record nncr
where
    (
        nncr.record_uuid in (
            select
                nnsr.record_uuid
            from
                agg.noisecapture_nostatic_record nnsr
        )
        and nncr.record_uuid in (
            select
                nnsr.record_uuid
            from
                agg.noisecapture_nostatic_record nnsr
        )
        and nncr.duration_seconds > 20
        and nncr.start_utc > '2024-01-01'
    );
