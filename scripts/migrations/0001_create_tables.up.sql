BEGIN;

-- Table for storing mailing campaigns
CREATE TABLE mailing_campaigns (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    strategy TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Table for storing Google Maps jobs
CREATE TABLE gmaps_jobs (
    id UUID PRIMARY KEY,
    priority SMALLINT NOT NULL,
    payload_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL
);

-- Table for storing results
CREATE TABLE results (
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

-- Table for storing business information
CREATE TABLE business_infos (
    id UUID PRIMARY KEY,
    result_id INT NOT NULL REFERENCES results(id) ON DELETE CASCADE,
    mailing_campaign_id UUID REFERENCES mailing_campaigns(id) ON DELETE SET NULL,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    comment TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Table for storing phone call records
CREATE TABLE phone_calls (
    id UUID PRIMARY KEY,
    business_infos_id UUID NOT NULL REFERENCES business_infos(id) ON DELETE CASCADE,
    duration SMALLINT NOT NULL,
    result_category TEXT NOT NULL,
    comment TEXT NOT NULL,
    raw_mp3 TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Table for linking mailing campaigns and business information
CREATE TABLE mailing_campaign_business_infos (
    id UUID PRIMARY KEY,
    mailing_campaign_id UUID NOT NULL REFERENCES mailing_campaigns(id) ON DELETE CASCADE,
    business_infos_id UUID NOT NULL REFERENCES business_infos(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMIT;