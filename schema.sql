create table dockerAgentEvents
(
    timestamp  DateTime,
    machine_id String,
    component  String,
    event      String,
    count      Nullable(int),
    message    Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toStartOfDay(timestamp)
ORDER BY (timestamp, machine_id);
