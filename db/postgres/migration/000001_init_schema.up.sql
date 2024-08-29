create table "messages" (
    "id" integer PRIMARY KEY,
    "subject" varchar not null,
    "body" varchar not null,
    expiration integer,
    "create_at" timestamptz not null default (now())
);