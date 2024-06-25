CREATE TABLE IF NOT EXISTS ndb.leadmodelbasis (
    pbbasisid SERIAL PRIMARY KEY,
    pbbasis text);
INSERT INTO ndb.leadmodelbasis (pbbasis) VALUES ('asymptote of alpha'),('gamma point-subtraction'),('gamma average');

CREATE TABLE IF NOT EXISTS ndb.leadmodels
(pbbasisid INT REFERENCES ndb.leadmodelbasis(pbbasisid),
 analysisunitid INT REFERENCES ndb.analysisunits(analysisunitid),
 cumulativeinventory NUMERIC CHECK (cumulativeinventory > 0));

----- Queries taken from Uncertainty.pdf

 CREATE TABLE IF NOT EXISTS ndb.uncertaintybases (
    uncertaintybasisid SERIAL PRIMARY KEY,
    uncertaintybasis text,
    CONSTRAINT uniquebasis  UNIQUE(uncertaintybasis));
INSERT INTO ndb.uncertaintybases (uncertaintybasis) 
VALUES ('1 Standard Deviation'),
       ('2 Standard Deviation'),
       ('3 Standard Deviation'), 
       ('1 Standard Error');

CREATE TABLE IF NOT EXISTS ndb.datauncertainties (
 dataid integer REFERENCES ndb.data(dataid),
 uncertaintyvalue float,
 uncertaintyunitid integer REFERENCES ndb.variableunits(variableunitsid),
 uncertaintybasisid integer REFERENCES ndb.uncertaintybases(uncertaintybasisid),
 notes text,
CONSTRAINT uniqueentryvalue UNIQUE (dataid, uncertaintyunitid, uncertaintybasisid)
);