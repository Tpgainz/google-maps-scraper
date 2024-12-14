BEGIN;
    CREATE TABLE gmaps_jobs(
        id UUID PRIMARY KEY,
        priority SMALLINT NOT NULL,
        payload_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        status TEXT NOT NULL
    );

    CREATE TABLE results(
        id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        title TEXT NOT NULL,
        category TEXT NOT NULL,
        address TEXT NOT NULL,
        openhours TEXT NOT NULL,
        website TEXT NOT NULL,
        phone TEXT NOT NULL,
        pluscode TEXT NOT NULL,
        review_count INT NOT NULL,
        rating NUMERIC NOT NULL
    );

    CREATE TABLE business_infos (
        id UUID PRIMARY KEY,
        result_id INT NOT NULL REFERENCES results(id),
        user_id TEXT NOT NULL,
        status TEXT NOT NULL,
        mail_infos JSONB NOT NULL,
        comment TEXT NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );

    CREATE TABLE phone_calls (
        id UUID PRIMARY KEY,
        business_infos_id UUID NOT NULL REFERENCES business_infos(id),
        duration SMALLINT NOT NULL,
        result_category TEXT NOT NULL,
        comment TEXT NOT NULL,
        raw_mp3 TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
COMMIT;