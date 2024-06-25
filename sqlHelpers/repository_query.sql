CREATE OR REPLACE FUNCTION insertrepositoryspecimen(_datasetid integer, 
                                       _repositoryid integer, 
                                       _notes character varying DEFAULT NULL::character varying,
                                       _recdatecreated date DEFAULT NULL::date, 
                                       _recdatemodified date DEFAULT NULL::date)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.repositoryspecimens(datasetid, repositoryid, notes, recdatecreated, recdatemodified)
    VALUES (_datasetid, _repositoryid, _notes, _recdatecreated, _recdatemodified)
    ON CONFLICT (datasetid, repositoryid) DO NOTHING
$function$