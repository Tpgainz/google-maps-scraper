BEGIN;
-- Table for storing Google Maps jobs
CREATE TABLE gmaps_jobs (
    id UUID PRIMARY KEY,
    parentId UUID REFERENCES gmaps_jobs(id),
    priority SMALLINT NOT NULL,
    payload_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL,
    child_jobs_count INTEGER NOT NULL DEFAULT 0,
    child_jobs_completed INTEGER NOT NULL DEFAULT 0
);

-- Table for storing results
CREATE TABLE results (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parentId UUID NOT NULL REFERENCES gmaps_jobs(id),
    user_id TEXT NOT NULL,
    organization_id TEXT,
    link TEXT NOT NULL,
    payload_type TEXT NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    address TEXT NOT NULL,
    emails TEXT[],
    website TEXT NOT NULL,
    phone TEXT NOT NULL,
    societe_dirigeant TEXT,
    societe_dirigeant_link TEXT,
    societe_forme TEXT,
    societe_effectif TEXT,
    societe_creation TEXT,
    societe_cloture TEXT,
    societe_link TEXT,
    status client_status NOT NULL DEFAULT 'new',
    call_count INTEGER NOT NULL DEFAULT 0,
    comment_count INTEGER NOT NULL DEFAULT 0,
    email_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE

);

-- Table for storing mailing campaigns
CREATE TABLE mailing_campaigns (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    organization_id TEXT,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    strategy TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE comments (
    id UUID PRIMARY KEY,
    result_id INT NOT NULL REFERENCES results(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE phone_calls (
    id UUID PRIMARY KEY,
    result_id INT NOT NULL REFERENCES results(id) ON DELETE CASCADE,
    duration SMALLINT NOT NULL,
    result_category TEXT NOT NULL,
    comment TEXT NOT NULL,
    raw_mp3 TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE mailing_campaign_mappings (
    id UUID PRIMARY KEY,
    mailing_campaign_id UUID NOT NULL REFERENCES mailing_campaigns(id) ON DELETE CASCADE,
    result_id INT NOT NULL REFERENCES results(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);


-- Table for tracking usage counts
CREATE TABLE usage_counts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL UNIQUE,
    organization_id TEXT,
    searches_count INTEGER NOT NULL DEFAULT 0,
    calls_minutes_used INTEGER NOT NULL DEFAULT 0,
    campaigns_created INTEGER NOT NULL DEFAULT 0,
    campaigns_this_month INTEGER NOT NULL DEFAULT 0,
    emails_sent INTEGER NOT NULL DEFAULT 0,
    reset_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (NOW() + INTERVAL '1 month'),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create subscriptions table
CREATE TABLE subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan plans NOT NULL,
    user_id TEXT NOT NULL UNIQUE,
    stripe_subscription_id TEXT NOT NULL,
    current_period_end TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMIT; 