CREATE OR REPLACE FUNCTION upsert_site(_siteid integer, 
                                       _sitename character varying, 
                                       _east numeric, 
                                       _north numeric, 
                                       _west numeric, 
                                       _south numeric, 
                                       _altitude integer DEFAULT NULL::integer, 
                                       _area numeric DEFAULT NULL::numeric, 
                                       _descript character varying DEFAULT NULL::character varying, 
                                       _notes character varying DEFAULT NULL::character varying)
RETURNS integer
    INSERT INTO ndb.sites(siteid, sitename, altitude, area, sitedescription, notes, geog)
    VALUES (_siteid, _sitename, _altitude, _area, _descript, _notes,)
    ON CONFLICT (_siteid) DO UPDATE
    SET sitename = EXCLUDED.sitename, 
        altitude = EXCLUDED.altitude;
        area = EXCLUDED.area,
        descript = EXCLUDED.descript,
        notes = EXCLUDED.notes
RETURNING siteid

INSERT INTO ndb.sites(siteid, sitename)
    VALUES (172, 'AgnesL')
    ON CONFLICT (siteid) DO UPDATE
    SET sitename = EXCLUDED.sitename