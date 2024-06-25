CREATE OR REPLACE FUNCTION upsert_site(_siteid integer, 
                                       _sitename character varying, 
                                       _altitude integer DEFAULT NULL::integer,
                                       _area numeric DEFAULT NULL::numeric, 
                                       _descript character varying DEFAULT NULL::character varying, 
                                       _notes character varying DEFAULT NULL::character varying,
                                       _east numeric DEFAULT NULL::integer, 
                                       _north numeric DEFAULT NULL::integer,  
                                       _west numeric DEFAULT NULL::integer, 
                                       _south numeric DEFAULT NULL::integer)
RETURNS integer
LANGUAGE sql
AS $function$
    INSERT INTO ndb.sites(siteid, sitename, altitude, area, sitedescription, notes, geog)
    VALUES (_siteid, _sitename, _altitude, _area, _descript, _notes,
            (SELECT ST_Envelope(('LINESTRING(' ||
                    _west::text || ' ' || _south::text || ',' ||
                    _east::text || ' ' || _north::text || ')')::geometry)::geography))
    ON CONFLICT (siteid) DO UPDATE
    SET sitename = COALESCE(EXCLUDED.sitename, sites.sitename), 
        altitude = COALESCE(EXCLUDED.altitude, sites.altitude),
        area = COALESCE(EXCLUDED.area, sites.area),
        sitedescription = COALESCE(EXCLUDED.sitedescription, sites.sitedescription),
        notes = COALESCE(EXCLUDED.notes, sites.notes),
        geog = COALESCE(EXCLUDED.geog, sites.geog)
    RETURNING siteid
$function$