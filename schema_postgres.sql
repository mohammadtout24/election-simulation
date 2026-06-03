CREATE TABLE IF NOT EXISTS election_members (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    district_code TEXT NOT NULL,
    candidate_id TEXT,
    member TEXT NOT NULL,
    group_name TEXT NOT NULL DEFAULT 'Independent',
    religion TEXT NOT NULL DEFAULT 'Unknown',
    district TEXT NOT NULL DEFAULT 'General'
);

CREATE TABLE IF NOT EXISTS election_votes (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    district_code TEXT NOT NULL,
    candidate_id TEXT,
    member TEXT,
    votes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS election_seats (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    district_code TEXT NOT NULL,
    district TEXT NOT NULL,
    religion TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_members_year_district ON election_members(year, district_code);
CREATE INDEX IF NOT EXISTS idx_votes_year_district ON election_votes(year, district_code);
CREATE INDEX IF NOT EXISTS idx_seats_year_district ON election_seats(year, district_code);
CREATE INDEX IF NOT EXISTS idx_members_name ON election_members(member);
CREATE INDEX IF NOT EXISTS idx_votes_name ON election_votes(member);
