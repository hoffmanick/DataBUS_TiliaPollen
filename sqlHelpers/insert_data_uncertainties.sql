CREATE OR REPLACE FUNCTION insert_data_uncertainties(_dataid integer, 
                                                   _uncertaintyvalue float, 
                                                   _uncertaintyunitid integer,
                                                   _uncertaintybasisid integer,
                                                   _notes character varying DEFAULT NULL::character varying)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.datauncertainties(dataid, uncertaintyvalue, uncertaintyunitid, uncertaintybasisid, notes)
    VALUES (_dataid, _uncertaintyvalue, _uncertaintyunitid, _uncertaintybasisid, _notes)
    ON CONFLICT (dataid, uncertaintyunitid, uncertaintybasisid) DO NOTHING
$function$