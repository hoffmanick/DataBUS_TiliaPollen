
SELECT ts.insertsite(_sitename := %(sitename)s, 
                        _altitude := %(altitude)s,
                        _area := %(area)s,
                        _descript := %(description)s,
                        _notes := %(notes)s,
                        _east := %(ew)s,
                        _north := %(ns)s,
                        _west := %(ew)s,
                        _south := %(ns)s)